"""Unit tests for Glue job 3 — silver into the gold layer.

``plan`` is pure and runs everywhere, so a scheduling mistake shows up here
rather than in a 40-minute job.
"""

from io import BytesIO

import pytest

import jobs.glue_silver_to_gold as gold


def test_the_default_plan_builds_every_table():
    assert gold.plan({}) == [
        "sessions", "funnel_daily", "orders", "customer_rfm", "product_daily", "anomalies",
    ]


def test_the_plan_honours_an_explicit_subset():
    """Rebuilding one table after a fix must not recompute the other five."""
    assert gold.plan({"GOLD_DATASETS": ["orders", "anomalies"]}) == ["orders", "anomalies"]


def test_an_empty_list_builds_nothing():
    """How the silver job hands the gold layer over to this one."""
    assert gold.plan({"GOLD_DATASETS": []}) == []


def test_the_pre_medallion_key_is_still_accepted():
    assert gold.plan({"CURATED_DATASETS": ["sessions"]}) == ["sessions"]


def test_an_unknown_dataset_fails_before_the_cluster_starts():
    with pytest.raises(ValueError, match="Unknown gold dataset"):
        gold.plan({"GOLD_DATASETS": ["dim_supplier"]})


# ─────────────────────────────────────────────
# CONFIG LOADING
#
# The job's first line of real work, and the one that failed on the cluster:
# ``load_config`` logged through a ``LOGGER`` name this module never defined,
# so every run died with a NameError before Spark started. Nothing here was
# tested, which is exactly why nobody saw it. Both branches now run.
# ─────────────────────────────────────────────

def test_load_config_reads_a_local_file(tmp_path):
    config = tmp_path / "config.json"
    config.write_text('{"OUTPUT_BUCKET": "demo-bucket"}', encoding="utf-8")

    assert gold.load_config(str(config))["OUTPUT_BUCKET"] == "demo-bucket"


def test_load_config_reads_from_s3(monkeypatch):
    class DummyS3:
        def get_object(self, Bucket, Key):
            assert (Bucket, Key) == ("demo", "jobs/gold.json")
            return {"Body": BytesIO(b'{"GOLD_DATASETS": ["orders"]}')}

    monkeypatch.setattr(gold.boto3, "client", lambda service: DummyS3())

    assert gold.load_config("s3://demo/jobs/gold.json")["GOLD_DATASETS"] == ["orders"]
