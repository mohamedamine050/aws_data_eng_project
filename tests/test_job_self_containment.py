"""The Glue jobs are standalone — this file is what keeps that safe.

Each job carries its own copy of the lake layout, so nothing has to be shipped
alongside its script. The cost of that choice is four copies that could drift,
and a drift is not a loud failure: the landing Lambda would keep writing to
``bronze/events/`` while a job quietly read somewhere else, and a day of data
would simply appear to be missing.

So the copies are pinned here. If one of them stops agreeing with
``common/lakehouse.py`` — the definition the Lambdas use — the build fails
instead of the pipeline.
"""

import ast
from pathlib import Path

import pytest

from common import lakehouse

import jobs.glue_landing_ingest as job_landing
import jobs.glue_ecommerce_processing as job_silver
import jobs.glue_quality_audit as job_audit
import jobs.glue_silver_to_gold as job_gold
import jobs.glue_rds_load as job_load

JOBS = {
    "glue_landing_ingest": job_landing,
    "glue_ecommerce_processing": job_silver,
    "glue_quality_audit": job_audit,
    "glue_silver_to_gold": job_gold,
    "glue_rds_load": job_load,
}

#: Configs worth checking: the medallion defaults, a relocated bucket layout,
#: and a pre-medallion deployment still using the old keys.
CONFIGS = [
    {"OUTPUT_BUCKET": "lake"},
    {"OUTPUT_BUCKET": "lake", "BRONZE_PREFIX": "incoming/", "GOLD_PREFIX": "marts/"},
    {"OUTPUT_BUCKET": "lake", "RAW_PREFIX": "raw/", "PROCESSED_PREFIX": "processed/",
     "CURATED_PREFIX": "curated/", "REJECTED_PREFIX": "rejected/"},
    {"OUTPUT_BUCKET": "lake", "PROCESSED_S3_PATH": "s3://elsewhere/fact/"},
]

DATASETS = ["bronze/events", "silver/events", "gold/orders", "quarantine/events"]


# ─────────────────────────────────────────────
# SELF-CONTAINMENT
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(JOBS))
def test_a_job_script_imports_nothing_from_the_lambda_package(name):
    """`common/` belongs to the Lambdas. A job that imports it needs shipping with it."""
    source = Path(JOBS[name].__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])

    assert "common" not in imported, f"{name} imports common/"
    assert "jobs" not in imported, f"{name} imports another job"


# ─────────────────────────────────────────────
# THE COPIES MUST AGREE
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(JOBS))
@pytest.mark.parametrize("config", CONFIGS)
def test_dataset_paths_match_the_lambda_definition(name, config):
    """The one that actually matters: where the landing Lambda writes is where
    job 1 must read."""
    job = JOBS[name]
    for dataset in DATASETS:
        assert job.dataset_path(config, dataset) == lakehouse.dataset_path(config, dataset), dataset


@pytest.mark.parametrize("name", sorted(JOBS))
def test_the_gold_dataset_registry_matches(name):
    assert JOBS[name].GOLD_DATASETS == lakehouse.GOLD_DATASETS


@pytest.mark.parametrize("name", sorted(JOBS))
@pytest.mark.parametrize("config", CONFIGS)
def test_build_paths_matches(name, config):
    assert JOBS[name].build_paths(config) == lakehouse.build_paths(config)


# ─────────────────────────────────────────────
# THE METRICS EMITTER STAYS HARMLESS
# ─────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(JOBS))
def test_metrics_are_silent_when_disabled(name):
    metrics = JOBS[name].JobMetrics.from_config({"METRICS_ENABLED": False}, stage="t")
    metrics.count("Rows", 10)

    assert metrics.flush() == 0


@pytest.mark.parametrize("name", sorted(JOBS))
def test_metrics_carry_the_stage_and_environment(name):
    class _Client:
        def __init__(self):
            self.calls = []

        def put_metric_data(self, Namespace, MetricData):  # noqa: N803 - boto3's names
            self.calls.append((Namespace, MetricData))

    client = _Client()
    metrics = JOBS[name].JobMetrics.from_config(
        {"METRICS_ENABLED": True, "ENVIRONMENT": "prod", "METRICS_NAMESPACE": "NS"},
        stage="a_stage", client=client,
    )
    metrics.count("Rows", 3)
    metrics.gauge("Pct", 99.5, unit="Percent")

    assert metrics.flush() == 2
    namespace, data = client.calls[0]
    assert namespace == "NS"
    dimensions = {d["Name"]: d["Value"] for d in data[0]["Dimensions"]}
    assert dimensions == {"Stage": "a_stage", "Environment": "prod"}


@pytest.mark.parametrize("name", sorted(JOBS))
def test_a_broken_cloudwatch_never_fails_the_run(name):
    """An observability failure must not fail a data run."""
    class _Broken:
        def put_metric_data(self, **kwargs):
            raise RuntimeError("throttled")

    metrics = JOBS[name].JobMetrics.from_config(
        {"METRICS_ENABLED": True}, stage="t", client=_Broken(),
    )
    metrics.count("Rows", 1)

    assert metrics.flush() == 0
