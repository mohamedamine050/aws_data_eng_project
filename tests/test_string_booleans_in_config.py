"""Un booleen ecrit en chaine doit se lire comme un booleen.

Un config.json genere par Terraform porte ``"METRICS_ENABLED": "true"`` — une
chaine. Et ``bool("false")`` vaut ``True``, donc un interrupteur d'arret devient
un interrupteur de marche, sans erreur, sans trace. Le meme nom etait deja lu
correctement quand il venait d'une variable d'environnement : les deux chemins
divergeaient.
"""

import pytest

import common.metrics as metrics
import jobs.glue_bronze_to_silver as job2
import jobs.glue_landing_ingest as job1
import jobs.glue_rds_load as job4
import jobs.glue_silver_to_gold as job3

MODULES = {
    "landing_ingest": job1,
    "bronze_to_silver": job2,
    "silver_to_gold": job3,
    "rds_load": job4,
    "common.metrics": metrics,
}

FALSY = ["false", "False", "FALSE", "0", "no", "off", "none", "", "  false  "]
TRUTHY = ["true", "True", "1", "yes", "on"]


@pytest.mark.parametrize("module", sorted(MODULES))
@pytest.mark.parametrize("value", FALSY)
def test_a_falsy_string_reads_as_false(module, value):
    assert MODULES[module].as_bool(value) is False


@pytest.mark.parametrize("module", sorted(MODULES))
@pytest.mark.parametrize("value", TRUTHY)
def test_a_truthy_string_reads_as_true(module, value):
    assert MODULES[module].as_bool(value) is True


@pytest.mark.parametrize("module", sorted(MODULES))
def test_real_booleans_pass_through(module):
    as_bool = MODULES[module].as_bool

    assert as_bool(True) is True
    assert as_bool(False) is False


@pytest.mark.parametrize("module", sorted(MODULES))
def test_an_absent_value_takes_the_default(module):
    as_bool = MODULES[module].as_bool

    assert as_bool(None, True) is True
    assert as_bool(None, False) is False


# ─────────────────────────────────────────────
# LA OU CA COMPTE VRAIMENT
# ─────────────────────────────────────────────

@pytest.mark.parametrize(
    "module", ["landing_ingest", "bronze_to_silver", "silver_to_gold", "rds_load"]
)
def test_metrics_enabled_false_as_a_string_really_disables_them(module):
    """Le cas qui a motive tout ceci : couper les metriques depuis le fichier."""
    emitter = MODULES[module].JobMetrics.from_config(
        {"METRICS_ENABLED": "false", "ENVIRONMENT": "dev"}, stage="test"
    )

    assert emitter.enabled is False


@pytest.mark.parametrize(
    "module", ["landing_ingest", "bronze_to_silver", "silver_to_gold", "rds_load"]
)
def test_metrics_enabled_true_as_a_string_keeps_them_on(module):
    emitter = MODULES[module].JobMetrics.from_config(
        {"METRICS_ENABLED": "true", "ENVIRONMENT": "dev"}, stage="test"
    )

    assert emitter.enabled is True


def test_archive_processed_can_be_switched_off_from_a_string(monkeypatch):
    """``"false"`` laissait les fichiers etre archives quand meme."""
    seen = {"archived": False}
    monkeypatch.setattr(job1, "list_drops", lambda *a, **kw: [])

    config = {"OUTPUT_BUCKET": "lake", "METRICS_ENABLED": "false", "ARCHIVE_PROCESSED": "false"}

    assert job1.as_bool(config["ARCHIVE_PROCESSED"], True) is False
    assert seen["archived"] is False


def test_rds_truncate_off_as_a_string_is_honoured():
    config = {
        "RDS_HOST": "db", "RDS_DATABASE": "d", "RDS_USERNAME": "u", "RDS_PASSWORD": "p",
        "RDS_TRUNCATE": "false",
    }

    assert job4._resolve_rds_settings(config)["truncate"] is False


def test_fail_on_empty_as_a_string_is_honoured(monkeypatch):
    monkeypatch.setattr(job1, "list_drops", lambda *a, **kw: [])

    class _S3:
        def get_paginator(self, _name):
            class _P:
                def paginate(self, Bucket, Prefix):
                    yield {"Contents": []}
            return _P()

    config = {"OUTPUT_BUCKET": "lake", "METRICS_ENABLED": "false",
              "FAIL_ON_EMPTY_DROP_ZONE": "true"}

    with pytest.raises(ValueError, match="No file to ingest"):
        job1.run(config, spark=None, s3=_S3())
