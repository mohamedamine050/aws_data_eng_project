"""Unit tests for Glue job 3 — silver into the gold layer.

``plan`` is pure and runs everywhere, so a scheduling mistake shows up here
rather than in a 40-minute job.
"""

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
