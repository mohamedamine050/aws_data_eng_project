"""Le format sur disque : ce qu'un etage ecrit doit se relire par le suivant.

Les chemins et les colonnes etaient verifies. Le *format* ne l'etait pas — et
c'est le mode de panne le plus vicieux du pipeline :

    Spark lit bronze en mode PERMISSIVE. Un objet qui ne correspond pas au
    schema v3 donne ZERO LIGNE, pas une erreur. Le job rapporte SUCCEEDED en
    trente secondes, silver reste vide, et rien dans les logs ne le dit.

``bronze/events/`` a deux ecrivains qui ne partagent aucun code : la Lambda
serialise en Python avec ``json.dumps``, le job Glue passe par Spark. Les deux
doivent produire quelque chose que ``read_raw`` relit.

Les tests de lecture visent un *fichier*, pas un repertoire : lister un
repertoire local passe par la couche native de Hadoop, absente sous Windows.
Ceux qui doivent ecrire demandent la fixture ``local_fs`` et se sautent la.
"""

import json
import pathlib
from datetime import datetime, timezone

import pytest

import jobs.glue_bronze_to_silver as silver
import jobs.glue_landing_ingest as landing
import jobs.glue_silver_to_gold as gold
from common.ecommerce_schema import normalize_record
from common.event_simulator import simulate

SAMPLE_CSV = pathlib.Path(__file__).resolve().parent.parent / "data/landing/partner_events.csv"

PRODUCTS = [
    {"product_id": f"sku-{i}", "sku": f"SKU-{i}", "name": f"Produit {i}",
     "category": "electronics", "brand": "Acme", "price": 25.0 * i}
    for i in range(1, 5)
]


@pytest.fixture(scope="module")
def records():
    """Ce que la Lambda producer met dans SQS."""
    return simulate(
        PRODUCTS,
        {"SEED": 5, "SESSIONS": 12, "CUSTOMER_POOL": 6, "WINDOW_MINUTES": 90},
        now=datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
    )


def _as_the_lambda_writes_it(events) -> str:
    """La ligne exacte de ``_flush`` dans stream_processor."""
    return "\n".join(
        json.dumps(e, separators=(",", ":"), default=str) for e in events
    ) + "\n"


def _bronze_file(tmp_path, events, name="20260624T120000-abcd1234.json") -> str:
    path = tmp_path / name
    path.write_text(_as_the_lambda_writes_it(events), encoding="utf-8")
    return str(path).replace("\\", "/")


# ─────────────────────────────────────────────
# SOURCE 1 — LAMBDA -> BRONZE -> JOB 2
# ─────────────────────────────────────────────

def test_what_the_lambda_writes_to_bronze_is_read_back_by_job_2(spark, records, tmp_path):
    """Le contrat entre les deux unites deployables, qui ne partagent aucun code."""
    raw = silver.read_raw(spark, _bronze_file(tmp_path, records))

    assert raw.count() == len(records), "le schema v3 n'a pas reconnu ce que la Lambda a ecrit"


def test_no_field_is_silently_dropped_on_the_way_in(spark, records, tmp_path):
    """Compter les lignes ne suffit pas : PERMISSIVE peut tout mettre a NULL."""
    row = silver.read_raw(spark, _bronze_file(tmp_path, records)).collect()[0]

    assert row["event_type"], "event_type perdu"
    assert row["occurred_at"], "occurred_at perdu"
    assert row["idempotency_key"], "idempotency_key perdue — la deduplication ne marcherait plus"
    assert row["product"]["product_id"], "le bloc product ne s'est pas relu"
    assert row["session"]["session_id"], "le bloc session ne s'est pas relu"


def test_the_lambda_events_survive_all_the_way_to_silver(spark, records, tmp_path):
    """Bout en bout : SQS -> bronze -> silver, sans perte."""
    processed = silver.to_processed(silver.read_raw(spark, _bronze_file(tmp_path, records)))

    assert processed.count() > 0, "tout a ete perdu entre bronze et silver"
    assert processed.filter("idempotency_key IS NULL").count() == 0


# ─────────────────────────────────────────────
# SOURCE 2 — JOB 1 -> BRONZE -> JOB 2
# ─────────────────────────────────────────────

@pytest.fixture(scope="module")
def lifted(spark):
    """Ce que le job 1 produit, depuis l'export partenaire livre."""
    raw = spark.read.option("header", "true").csv(str(SAMPLE_CSV).replace("\\", "/"))
    return landing.add_identity(landing.lift_flat(raw), source_object="test").cache()


def test_what_job_1_writes_to_bronze_is_read_back_by_job_2(spark, lifted, local_fs):
    """L'autre ecrivain de bronze, par un chemin de code entierement different."""
    bronze = str(local_fs / "bronze_glue").replace("\\", "/")
    written = lifted.count()

    landing._write_ndjson(landing.with_partitions(lifted), bronze, ["dt", "hour"], 1)

    assert silver.read_raw(spark, bronze).count() == written


def test_both_writers_produce_the_same_shape(spark, records, lifted, local_fs):
    """Le job 2 lit un seul prefixe : les deux sources doivent y etre indiscernables."""
    bronze = str(local_fs / "bronze_mixed").replace("\\", "/")
    landing._write_ndjson(landing.with_partitions(lifted), bronze, ["dt", "hour"], 1)

    extra = pathlib.Path(bronze) / "dt=2026-06-24" / "hour=12"
    extra.mkdir(parents=True, exist_ok=True)
    (extra / "from-the-lambda.json").write_text(
        _as_the_lambda_writes_it(records), encoding="utf-8"
    )

    mixed = silver.read_raw(spark, bronze)

    assert mixed.count() == lifted.count() + len(records)
    assert mixed.filter("idempotency_key IS NULL").count() == 0


# ─────────────────────────────────────────────
# SILVER -> GOLD : PARQUET, PAS NDJSON
# ─────────────────────────────────────────────

def test_silver_is_written_as_parquet_and_read_back_as_parquet(spark, records, local_fs):
    """Le job 3 lit du Parquet. Du NDJSON a cet endroit donnerait zero ligne."""
    processed = silver.to_processed(silver.read_raw(spark, _bronze_file(local_fs, records)))
    silver_path = str(local_fs / "silver" / "events").replace("\\", "/")
    processed.coalesce(1).write.mode("overwrite") \
        .partitionBy("partition_date", "partition_hour").parquet(silver_path)

    reread = spark.read.parquet(silver_path)

    assert reread.count() == processed.count()
    assert {"partition_date", "partition_hour"} <= set(reread.columns)


def test_gold_builds_on_what_silver_actually_wrote(spark, records, local_fs):
    """Les tables gold se construisent sur le Parquet relu, pas sur le DataFrame en memoire.

    Un aller-retour Parquet peut changer un type — une colonne de partition
    revient en chaine — et un agregat qui marchait en memoire echoue apres
    relecture.
    """
    processed = silver.to_processed(silver.read_raw(spark, _bronze_file(local_fs, records)))
    silver_path = str(local_fs / "silver" / "events").replace("\\", "/")
    processed.coalesce(1).write.mode("overwrite") \
        .partitionBy("partition_date", "partition_hour").parquet(silver_path)

    reread = spark.read.parquet(silver_path)

    assert gold.build_sessions(reread).count() > 0
    assert gold.build_funnel_daily(reread).count() > 0
    assert gold.build_customer_rfm(reread).count() > 0


# ─────────────────────────────────────────────
# CE QUI NE DOIT PAS ETRE DEPOSE DANS BRONZE
# ─────────────────────────────────────────────

def _events(n=3):
    return [
        normalize_record(
            PRODUCTS[0],
            {"event_type": "product_viewed", "occurred_at": "2026-06-24T12:00:00+00:00",
             "session_id": f"s{i}", "sequence": i},
            "web",
        )
        for i in range(n)
    ]


def _read(spark, tmp_path, name, content):
    """(lignes lues, lignes exploitables) pour un contenu donne."""
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    raw = silver.read_raw(spark, str(path).replace("\\", "/"))
    return raw.count(), raw.filter("event_type IS NOT NULL").count()


def test_ndjson_is_the_contract(spark, tmp_path):
    """Une ligne = un evenement complet. C'est ce que les deux ecrivains produisent."""
    assert _read(spark, tmp_path, "ndjson.json",
                 "\n".join(json.dumps(e) for e in _events())) == (3, 3)


def test_a_single_line_json_array_is_tolerated(spark, tmp_path):
    """Mesure, pas supposition : Spark lit un tableau qui tient sur une ligne.

    Je croyais l'inverse. Le test dit ce qui est, pour que personne n'aille
    corriger un probleme qui n'existe pas.
    """
    assert _read(spark, tmp_path, "array.json", json.dumps(_events())) == (3, 3)


@pytest.mark.parametrize(
    ("name", "content"),
    [
        ("pretty.json", json.dumps(_events(), indent=2)),
        ("one-object.json", json.dumps(_events(1)[0], indent=2)),
        ("export.csv", "event_type,occurred_at\nproduct_viewed,2026-06-24T12:00:00+00:00\n"),
        ("notes.txt", "bonjour"),
    ],
    ids=["json-indente", "un-objet-indente", "csv", "texte"],
)
def test_these_shapes_give_rows_that_are_all_null(spark, tmp_path, name, content):
    """Le mode de panne, reproduit — et pire que "zero ligne".

    Un JSON indente ne donne pas zero ligne : il en donne UNE PAR LIGNE DE
    FORMATAGE, toutes nulles. Le compte est donc non nul, ce qui trompait le
    diagnostic — le job accusait la deduplication d'un probleme de format.
    Seul un ``event_type`` non nul prouve qu'un objet a ete compris.
    """
    _rows, usable = _read(spark, tmp_path, name, content)

    assert usable == 0, f"{name} ne devrait donner aucun evenement exploitable"


def test_the_job_calls_an_all_null_read_a_format_problem(monkeypatch, caplog):
    """Ce que le job doit dire quand le compte est non nul mais rien n'est lisible."""
    class _AllNull:
        def count(self):
            return 161

        def filter(self, *_args):
            return _Zero()

    class _Zero:
        def count(self):
            return 0

    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _AllNull())

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(
            {"OUTPUT_BUCKET": "demo-lake", "METRICS_ENABLED": "false"}, spark="unused"
        )

    assert result["records"] == 0
    assert "161 row(s), none of which carry an event_type" in caplog.text
    assert "NDJSON" in caplog.text
    assert "deduplication" not in caplog.text, "ne doit pas accuser la deduplication"
