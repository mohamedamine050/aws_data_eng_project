"""Bronze n'existe pas encore : le job doit le dire, pas planter en AnalysisException.

Incident reel : le job 1 n'ayant rien ecrit, le job 2 est mort sur

    AnalysisException: [PATH_NOT_FOUND] Path does not exist:
    s3://data-lake-.../bronze/events

Message vrai, mais qui designe le symptome et pas la cause — la faute est en
amont, dans ce qui aurait du remplir bronze.
"""

import pytest

import jobs.glue_bronze_to_silver as silver

CONFIG = {"OUTPUT_BUCKET": "data-lake-demo", "METRICS_ENABLED": False}


class _Empty:
    """Un prefixe sans aucun objet."""

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):  # noqa: N803 - boto3's names
        return {"KeyCount": 0}


class _Filled:
    def list_objects_v2(self, Bucket, Prefix, MaxKeys):  # noqa: N803
        return {"KeyCount": 1, "Contents": [{"Key": f"{Prefix}dt=2026-06-24/part-0.json"}]}


class _Broken:
    def list_objects_v2(self, Bucket, Prefix, MaxKeys):  # noqa: N803
        raise RuntimeError("access denied")


# ─────────────────────────────────────────────
# LA VERIFICATION EN AMONT
# ─────────────────────────────────────────────

def test_an_empty_prefix_is_detected_before_spark_reads():
    assert silver._has_objects("s3://lake/bronze/events/", client=_Empty()) is False


def test_a_filled_prefix_passes():
    assert silver._has_objects("s3://lake/bronze/events/", client=_Filled()) is True


def test_a_listing_failure_is_not_an_empty_lake():
    """Pas le droit de conclure "vide" parce que la question a echoue."""
    assert silver._has_objects("s3://lake/bronze/events/", client=_Broken()) is True


def test_a_local_path_is_left_to_spark():
    assert silver._has_objects("/tmp/bronze/events/", client=_Broken()) is True


# ─────────────────────────────────────────────
# CE QUE FAIT LE JOB
# ─────────────────────────────────────────────

def test_an_empty_bronze_names_the_upstream_cause(monkeypatch, caplog):
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(CONFIG, spark="unused")

    assert result["records"] == 0
    assert result["reason"] == "bronze is empty"
    assert "s3://data-lake-demo/bronze/events/" in caplog.text
    assert "glue_landing_ingest" in caplog.text     # qui remplit bronze
    assert "stream_processor" in caplog.text        # l'autre producteur


def test_an_empty_bronze_never_starts_spark(monkeypatch):
    """Aucune session, aucune lecture : on sort avant de payer le cluster."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: pytest.fail("Spark ne doit pas lire"))

    assert silver.run_spark_job(CONFIG, spark="unused")["records"] == 0


def test_an_empty_bronze_can_be_made_fatal(monkeypatch):
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)

    config = {**CONFIG, "FAIL_ON_EMPTY_BRONZE": True}

    with pytest.raises(ValueError, match="Nothing to process"):
        silver.run_spark_job(config, spark="unused")


def test_sparks_own_path_not_found_is_absorbed_too(monkeypatch, caplog):
    """La verification en amont peut etre devancee par une course : filet de securite."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)

    def _raise(*args, **kwargs):
        raise RuntimeError(
            "[PATH_NOT_FOUND] Path does not exist: s3://data-lake-demo/bronze/events"
        )

    monkeypatch.setattr(silver, "read_raw", _raise)

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(CONFIG, spark="unused")

    assert result["reason"] == "bronze is empty"
    assert "PATH_NOT_FOUND" in caplog.text          # l'erreur d'origine reste lisible


def test_any_other_spark_error_still_fails_the_job(monkeypatch):
    """Absorber PATH_NOT_FOUND ne doit pas absorber tout le reste."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)

    def _raise(*args, **kwargs):
        raise RuntimeError("[UNSUPPORTED_FEATURE] something genuinely broken")

    monkeypatch.setattr(silver, "read_raw", _raise)

    with pytest.raises(RuntimeError, match="UNSUPPORTED_FEATURE"):
        silver.run_spark_job(CONFIG, spark="unused")


# ─────────────────────────────────────────────
# DES OBJETS, MAIS ZERO LIGNE
#
# Le cas le plus difficile a diagnostiquer : le job lit un prefixe qui contient
# des objets, n'en tire aucune ligne, ecrit un dataset vide, et rapporte
# SUCCEEDED en 34 secondes. Rien dans les logs ne disait combien de lignes
# etaient entrees ni sorties.
# ─────────────────────────────────────────────

class _Rows:
    """Un DataFrame reduit a ce que le job lui demande avant d'ecrire.

    ``usable`` est le nombre de lignes portant un ``event_type`` : le job
    distingue "aucune ligne" de "des lignes, toutes nulles", et le double
    n'aurait aucun sens s'il confondait les deux.
    """

    def __init__(self, count, usable=None):
        self._count = count
        self._usable = count if usable is None else usable

    def count(self):
        return self._count

    def cache(self):
        return self

    def filter(self, *_args):
        # le seul filter() appele avant l'ecriture est celui d'event_type
        return _Rows(self._usable)


def test_objects_that_parse_to_zero_rows_are_reported(monkeypatch, caplog):
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _Rows(0))

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(CONFIG, spark="unused")

    assert result["reason"] == "bronze parsed to zero rows"
    assert result["row_counts"] == {"raw": 0, "processed": 0}
    assert "NDJSON" in caplog.text                      # ce qu'il attendait
    assert "s3://data-lake-demo/silver/events/" in caplog.text


def test_rows_read_but_none_kept_names_the_filter(monkeypatch, caplog, spark):
    """``spark`` sert uniquement a activer un SparkContext : le job filtre
    desormais sur ``F.col("event_type")``, qui en exige un."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _Rows(120))
    monkeypatch.setattr(silver, "to_processed", lambda raw: _Rows(0))

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(CONFIG, spark="unused")

    assert result["row_counts"] == {"raw": 120, "processed": 0}
    assert "Read 120 row(s)" in caplog.text
    assert "cleaning or deduplication" in caplog.text


def test_a_process_date_that_matches_nothing_says_so(monkeypatch, caplog, spark):
    """La cause la plus banale : rejouer une date sans donnees.

    ``spark`` n'est demande que pour activer un SparkContext : le filtre passe
    par ``F.col``, qui exige un contexte actif meme sans donnees.
    """
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _Rows(120))
    monkeypatch.setattr(silver, "to_processed", lambda raw: _Rows(0))

    config = {**CONFIG, "PROCESS_DATE": "2020-01-01"}

    with caplog.at_level("WARNING"):
        silver.run_spark_job(config, spark="unused")

    assert "PROCESS_DATE=2020-01-01 matched none of them" in caplog.text


def test_the_row_counts_are_logged_on_a_normal_run(monkeypatch, caplog):
    """Le compteur doit apparaitre aussi quand tout va bien."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _Rows(120))

    with caplog.at_level("INFO"):
        with pytest.raises(Exception):
            # l'ecriture Parquet echouera sur le faux DataFrame — peu importe,
            # les deux lignes de comptage sont deja passees
            silver.run_spark_job(CONFIG, spark="unused")

    assert "Read 120 row(s)" in caplog.text


def test_zero_rows_can_be_made_fatal(monkeypatch):
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: True)
    monkeypatch.setattr(silver, "read_raw", lambda *a, **kw: _Rows(0))

    with pytest.raises(ValueError, match="No usable event"):
        silver.run_spark_job({**CONFIG, "FAIL_ON_EMPTY_BRONZE": "true"}, spark="unused")


# ─────────────────────────────────────────────
# LES OBJETS SONT LA, UN PREFIXE TROP HAUT
#
# Le cas reel : le fichier depose a `bronze/123.json` au lieu de
# `bronze/events/`. Le job lit un prefixe vide et rapporte SUCCEEDED. Le log
# doit nommer les objets qu'il a vus a cote.
# ─────────────────────────────────────────────

class _Listing:
    def __init__(self, keys):
        self.keys = keys

    def get_paginator(self, _name):
        keys = self.keys

        class _P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {"Contents": [{"Key": k} for k in keys if k.startswith(Prefix)]}

        return _P()


def test_objects_one_prefix_too_high_are_named_in_the_log(monkeypatch, caplog):
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)
    monkeypatch.setattr(silver, "s3", _Listing(["bronze/123.json", "bronze/456.json"]))

    with caplog.at_level("WARNING"):
        silver.run_spark_job(CONFIG, spark="unused")

    assert "bronze/123.json" in caplog.text
    assert "outside the prefix this job reads" in caplog.text


def test_objects_in_the_right_place_are_not_reported_as_strays(monkeypatch, caplog):
    """Ce qui est deja sous bronze/events/ n'est pas un egare."""
    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)
    monkeypatch.setattr(
        silver, "s3", _Listing(["bronze/events/dt=2026-06-24/hour=12/part-0.json"])
    )

    with caplog.at_level("WARNING"):
        silver.run_spark_job(CONFIG, spark="unused")

    assert "outside the prefix this job reads" not in caplog.text


def test_a_listing_failure_never_hides_the_real_message(monkeypatch, caplog):
    class _Broken:
        def get_paginator(self, _name):
            raise RuntimeError("access denied")

    monkeypatch.setattr(silver, "_has_objects", lambda uri, client=None: False)
    monkeypatch.setattr(silver, "s3", _Broken())

    with caplog.at_level("WARNING"):
        result = silver.run_spark_job(CONFIG, spark="unused")

    assert result["records"] == 0
    assert "Nothing to process" in caplog.text
