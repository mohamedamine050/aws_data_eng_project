"""Chaque composant declare son entree et sa sortie — et la declaration doit etre vraie.

Une declaration qui derive du code est pire que pas de declaration : elle fait
chercher au mauvais endroit. Ces tests l'epinglent sur trois axes.

1. **Exactitude** — le chemin declare est celui que le code utilise vraiment.
2. **Raccordement** — ce qu'un etage ecrit est ce que le suivant lit, chemin ET
   format. C'est precisement ce qui a casse en production : un fichier depose a
   ``bronze/123.json`` alors que le job lit ``bronze/events/``.
3. **Lisibilite** — le contrat part dans les logs au demarrage, avant tout
   travail, parce qu'un job qui reussit sans rien ecrire pose toujours la meme
   question : quel prefixe as-tu lu ?
"""

import pytest

import jobs.glue_bronze_to_silver as job2
import jobs.glue_landing_ingest as job1
import jobs.glue_rds_load as job4
import jobs.glue_silver_to_gold as job3
import lambdas.ecommerce_producer.handler as producer
import lambdas.stream_processor.handler as consumer

CONFIG = {
    "OUTPUT_BUCKET": "mon-lac",
    "ENVIRONMENT": "dev",
    "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/1234/pipeline-queue",
    "ECOMMERCE_API_URL": "https://api.example.com/products",
    "RDS_HOST": "warehouse.rds.amazonaws.com",
    "RDS_DATABASE": "ecommerce",
    "METRICS_ENABLED": "false",
}

COMPONENTS = {
    "job1 landing_ingest": job1,
    "job2 bronze_to_silver": job2,
    "job3 silver_to_gold": job3,
    "job4 rds_load": job4,
    "lambda producer": producer,
    "lambda consumer": consumer,
}


def _where(contract, side, what):
    for item in contract[side]:
        if item["what"] == what:
            return item["where"]
    raise AssertionError(f"{what!r} absent de {side}: {[i['what'] for i in contract[side]]}")


# ─────────────────────────────────────────────
# 1 · TOUT LE MONDE DECLARE
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(COMPONENTS))
def test_every_component_declares_its_io(name):
    contract = COMPONENTS[name].describe_io(CONFIG)

    assert contract["job"], "le composant doit se nommer"
    assert contract["reads"], "un composant qui ne lit rien n'a pas de raison d'etre"
    assert contract["writes"], "ni un qui n'ecrit rien"


@pytest.mark.parametrize("name", sorted(COMPONENTS))
def test_every_entry_names_a_place_and_a_format(name):
    contract = COMPONENTS[name].describe_io(CONFIG)

    for side in ("reads", "writes"):
        for item in contract[side]:
            assert item["what"], f"{name}: une entree sans libelle"
            assert item["format"], f"{name}: {item['what']} sans format declare"
            assert item["where"], f"{name}: {item['what']} sans emplacement"


# ─────────────────────────────────────────────
# 2 · LA DECLARATION DIT VRAI
# ─────────────────────────────────────────────

def test_job1_declares_the_prefix_it_really_reads():
    contract = job1.describe_io(CONFIG)

    assert _where(contract, "reads", "partner file drops") == (
        f"s3://mon-lac/{job1.ingest_prefix(CONFIG)}"
    )
    assert _where(contract, "writes", "bronze events") == job1.build_paths(CONFIG)["bronze_events"]


def test_job2_declares_the_prefixes_it_really_uses():
    contract = job2.describe_io(CONFIG)
    paths = job2.build_paths(CONFIG)

    assert _where(contract, "reads", "bronze events") == paths["bronze_events"]
    assert _where(contract, "writes", "silver events") == paths["silver_events"]


def test_job3_declares_one_output_per_table_it_builds():
    contract = job3.describe_io(CONFIG)

    declared = {item["what"] for item in contract["writes"]}
    assert declared == {f"gold {name}" for name in job3.plan(CONFIG)}
    assert _where(contract, "reads", "silver events") == job3.build_paths(CONFIG)["silver_events"]


def test_job4_declares_every_target_it_loads():
    contract = job4.describe_io(CONFIG)
    targets = job4.resolve_targets(CONFIG)

    assert len(contract["reads"]) == len(targets)
    assert {item["what"] for item in contract["writes"]} == {t["table"] for t in targets}


def test_the_consumer_declares_the_queue_and_the_zones_it_writes():
    contract = consumer.describe_io(CONFIG)

    assert _where(contract, "reads", "queued events") == CONFIG["QUEUE_URL"]
    assert _where(contract, "writes", "bronze events") == "s3://mon-lac/bronze/events/"
    assert _where(contract, "writes", "rejected records") == "s3://mon-lac/quarantine/events/"


def test_the_producer_writes_to_the_queue_and_nowhere_else():
    """Le seul composant qui n'ecrit rien dans S3."""
    contract = producer.describe_io(CONFIG)

    assert len(contract["writes"]) == 1
    assert _where(contract, "writes", "simulated events") == CONFIG["QUEUE_URL"]
    assert not any(item["where"].startswith("s3://") for item in contract["writes"])


def test_the_producer_only_lists_the_sources_that_are_configured():
    """Declarer un catalogue S3 vide enverrait chercher un fichier inexistant."""
    contract = producer.describe_io({**CONFIG, "PRODUCTS_S3_CSV": "", "CUSTOMERS_S3_CSV": ""})

    assert [item["what"] for item in contract["reads"]] == ["catalog API"]


# ─────────────────────────────────────────────
# 3 · LES ETAGES SE RACCORDENT
# ─────────────────────────────────────────────

def test_both_bronze_writers_declare_the_same_place_and_format():
    """Le Glue job 1 et la Lambda consumer partagent bronze/events/."""
    from_job = next(i for i in job1.describe_io(CONFIG)["writes"] if i["what"] == "bronze events")
    from_lambda = next(
        i for i in consumer.describe_io(CONFIG)["writes"] if i["what"] == "bronze events"
    )

    assert from_job["where"] == from_lambda["where"]
    assert from_job["format"] == from_lambda["format"]


@pytest.mark.parametrize(
    ("upstream", "downstream", "what"),
    [
        (job1, job2, "bronze events"),
        (job2, job3, "silver events"),
    ],
    ids=["bronze: job1 -> job2", "silver: job2 -> job3"],
)
def test_what_one_stage_writes_is_what_the_next_one_reads(upstream, downstream, what):
    """Le raccordement, chemin ET format.

    Le format compte autant : bronze est du NDJSON, silver du Parquet. Se
    tromper de format donne zero ligne, pas une erreur.
    """
    written = next(i for i in upstream.describe_io(CONFIG)["writes"] if i["what"] == what)
    read = next(i for i in downstream.describe_io(CONFIG)["reads"] if i["what"] == what)

    assert written["where"] == read["where"]
    assert written["format"].split(",")[0] == read["format"].split(",")[0]


def test_the_warehouse_load_reads_exactly_what_gold_wrote():
    gold_outputs = {i["where"] for i in job3.describe_io(CONFIG)["writes"]}
    read_by_job4 = {i["where"] for i in job4.describe_io(CONFIG)["reads"]}

    assert gold_outputs <= read_by_job4, "une table gold construite et jamais chargee"


def test_no_stage_declares_a_bare_zone_where_a_dataset_is_meant():
    """L'erreur exacte de production : bronze/ au lieu de bronze/events/.

    Une zone nue comme emplacement de lecture ou d'ecriture signifie que le
    dataset a ete oublie, et c'est indetectable a l'execution — le job lit un
    prefixe vide et rapporte un succes.
    """
    bare = {f"s3://mon-lac/{zone}/" for zone in ("bronze", "silver", "gold")}

    for name, module in COMPONENTS.items():
        contract = module.describe_io(CONFIG)
        for side in ("reads", "writes"):
            for item in contract[side]:
                assert item["where"] not in bare, (
                    f"{name} declare la zone nue {item['where']} pour {item['what']!r}"
                )


# ─────────────────────────────────────────────
# 4 · LE CONTRAT PART DANS LES LOGS
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(COMPONENTS))
def test_the_contract_reaches_the_log(name, caplog):
    with caplog.at_level("INFO"):
        COMPONENTS[name].log_io(CONFIG)

    assert "input/output contract" in caplog.text
    assert "READS" in caplog.text and "WRITES" in caplog.text


def test_the_log_carries_the_prefix_that_matters(caplog):
    """La ligne qui aurait repondu tout de suite a "pourquoi silver est vide"."""
    with caplog.at_level("INFO"):
        job2.log_io(CONFIG)

    assert "s3://mon-lac/bronze/events/" in caplog.text
    assert "s3://mon-lac/silver/events/" in caplog.text
    assert "NDJSON" in caplog.text and "Parquet" in caplog.text


# ─────────────────────────────────────────────
# 5 · LE DIAGNOSTIC NE DOIT JAMAIS TUER LE JOB
#
# `log_io` tourne avant tout travail. Une exception ici arreterait le job pour
# une ligne de journal — exactement le defaut du `LOGGER` indefini qui a
# ouvert cette serie de pannes.
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(COMPONENTS))
def test_log_io_survives_a_broken_contract(name, monkeypatch, caplog):
    module = COMPONENTS[name]
    monkeypatch.setattr(
        module, "describe_io",
        lambda config: (_ for _ in ()).throw(RuntimeError("contrat casse")),
    )

    with caplog.at_level("WARNING"):
        module.log_io(CONFIG)  # ne doit pas lever

    assert "Could not describe the input/output contract" in caplog.text


@pytest.mark.parametrize("name", sorted(COMPONENTS))
def test_log_io_survives_an_unusable_config(name, caplog):
    """Une config vide n'a meme pas OUTPUT_BUCKET : on journalise, on continue."""
    with caplog.at_level("WARNING"):
        COMPONENTS[name].log_io({})
