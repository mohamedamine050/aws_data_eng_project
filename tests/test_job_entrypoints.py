"""What Glue actually calls: the four ``main()`` entrypoints.

Every job on the cluster starts the same way — resolve the arguments, read the
config, hand it to ``run``. Nothing here exercised that path, which is how a
``load_config`` referencing an undefined ``LOGGER`` and an argument check that
never matched a real Glue command line both reached production. A wiring bug in
these fourteen lines kills the job before Spark starts, so they are worth the
stubs.

The work itself is stubbed out: this is about the plumbing between
``getResolvedOptions`` and ``run``.
"""

import json

import pytest

import jobs.glue_bronze_to_silver as bronze_silver
import jobs.glue_landing_ingest as landing
import jobs.glue_rds_load as rds
import jobs.glue_silver_to_gold as gold


CONFIG = {
    "ENVIRONMENT": "dev",
    "OUTPUT_BUCKET": "demo-lake",
}


@pytest.fixture()
def config_file(tmp_path):
    path = tmp_path / "job.json"
    path.write_text(json.dumps(CONFIG), encoding="utf-8")
    return str(path)


def _resolver(config_path, job_name="ecommerce-job"):
    """Stand-in for ``awsglue.utils.getResolvedOptions``."""
    def resolve(argv, keys):
        return {"JOB_NAME": job_name, "CONFIG_PATH": config_path}
    return resolve


# ─────────────────────────────────────────────
# THE THREE JOBS THAT SHARE THE SHAPE
# ─────────────────────────────────────────────

@pytest.mark.parametrize(
    ("module", "runner"),
    [
        (landing, "run"),
        (gold, "run"),
        (bronze_silver, "run_spark_job"),
    ],
    ids=["landing_ingest", "silver_to_gold", "bronze_to_silver"],
)
def test_main_reads_the_config_and_hands_it_to_the_job(module, runner, config_file, monkeypatch):
    captured = {}

    def fake_run(config, *args, **kwargs):
        captured.update(config)
        return {"status": "success"}

    monkeypatch.setattr(module, "getResolvedOptions", _resolver(config_file))
    monkeypatch.setattr(module, runner, fake_run)

    assert module.main(["prog", "--JOB_NAME", "x", "--CONFIG_PATH", config_file]) == {"status": "success"}
    assert captured["OUTPUT_BUCKET"] == "demo-lake"


@pytest.mark.parametrize(
    ("module", "runner"),
    [(landing, "run"), (gold, "run")],
    ids=["landing_ingest", "silver_to_gold"],
)
def test_main_names_the_job_from_the_glue_argument(module, runner, config_file, monkeypatch):
    """``JOB_NAME`` is what the metrics are tagged with, so it must survive."""
    captured = {}
    monkeypatch.setattr(module, "getResolvedOptions", _resolver(config_file, job_name="nightly-run"))
    monkeypatch.setattr(module, runner, lambda config, *a, **k: captured.update(config) or {})

    module.main(["prog"])

    assert captured["JOB_NAME"] == "nightly-run"


def test_bronze_to_silver_main_honours_the_python_engine(tmp_path, monkeypatch):
    """``ENGINE: python`` is the no-Spark path — it must not build a session."""
    path = tmp_path / "job.json"
    path.write_text(json.dumps({**CONFIG, "ENGINE": "python"}), encoding="utf-8")

    seen = {}
    monkeypatch.setattr(bronze_silver, "getResolvedOptions", _resolver(str(path)))
    monkeypatch.setattr(bronze_silver, "run_job", lambda **kwargs: seen.update(kwargs) or {"status": "success"})
    monkeypatch.setattr(bronze_silver, "run_spark_job", lambda config: pytest.fail("Spark must not start"))

    assert bronze_silver.main(["prog"])["status"] == "success"
    assert seen["bucket"] == "demo-lake"
    assert seen["local_fs"] is False


# ─────────────────────────────────────────────
# THE WAREHOUSE LOAD — ITS OWN SHAPE
# ─────────────────────────────────────────────

def test_rds_main_runs_end_to_end_over_stubs(config_file, monkeypatch, capsys):
    """``main`` prints the run summary; everything below it is stubbed."""
    class DummyBuilder:
        def appName(self, name):
            return self

        def getOrCreate(self):
            return "spark-session"

    monkeypatch.setattr(rds, "getResolvedOptions", _resolver(config_file))
    monkeypatch.setattr(rds.sys, "argv", ["prog", "--JOB_NAME", "x", "--CONFIG_PATH", config_file])
    monkeypatch.setattr(rds, "SparkSession", type("S", (), {"builder": DummyBuilder()}))
    monkeypatch.setattr(rds, "_resolve_rds_settings", lambda config: {"url": "jdbc:postgresql://db/x"})
    monkeypatch.setattr(rds, "resolve_targets", lambda config: [{"table": "fact_events"}])
    monkeypatch.setattr(
        rds,
        "load_targets",
        lambda spark, targets, settings: [{"table": "fact_events", "status": "loaded", "rows_loaded": 42}],
    )

    rds.main()

    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "success"
    assert summary["mode"] == "glue"
    assert summary["rows_loaded"] == 42
