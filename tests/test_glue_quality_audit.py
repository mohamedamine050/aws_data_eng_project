"""Unit tests for Glue job 4 — the batch quality gate.

Split in two: the decision logic (check resolution, scoring, the verdict) runs
without Spark, and the Spark expressions are exercised against a local session
that skips when PySpark is absent.
"""

import pytest

import jobs.glue_quality_audit as audit


# ─────────────────────────────────────────────
# CHECK RESOLUTION
# ─────────────────────────────────────────────

def test_the_baseline_checks_are_used_by_default():
    names = [check["name"] for check in audit.resolve_checks({})]

    assert names == [check["name"] for check in audit.DEFAULT_CHECKS]


def test_every_baseline_check_declares_a_severity_and_a_reason():
    for check in audit.DEFAULT_CHECKS:
        assert check["severity"] in ("error", "warn")
        assert check["description"], f"{check['name']} has no description"


def test_a_config_check_is_added():
    checks = audit.resolve_checks({"QUALITY_CHECKS": [
        {"name": "eur_only", "expr": "currency = 'EUR'", "severity": "warn"}
    ]})

    assert checks[-1]["name"] == "eur_only"
    assert len(checks) == len(audit.DEFAULT_CHECKS) + 1


def test_a_config_check_overrides_a_baseline_check_of_the_same_name():
    """Relaxing one rule must not mean copying the other eleven."""
    checks = audit.resolve_checks({"QUALITY_CHECKS": [
        {"name": "quantity_sane", "expr": "quantity <= 1000", "severity": "warn"}
    ]})
    relaxed = next(c for c in checks if c["name"] == "quantity_sane")

    assert relaxed["expr"] == "quantity <= 1000"
    assert len(checks) == len(audit.DEFAULT_CHECKS)


def test_a_check_can_be_disabled():
    checks = audit.resolve_checks({"QUALITY_CHECKS_DISABLED": ["customer_id_present"]})

    assert "customer_id_present" not in [c["name"] for c in checks]


def test_a_check_without_an_expression_is_rejected():
    with pytest.raises(ValueError, match="needs a 'name' and an 'expr'"):
        audit.resolve_checks({"QUALITY_CHECKS": [{"name": "broken"}]})


def test_a_config_check_defaults_to_error_severity():
    checks = audit.resolve_checks({"QUALITY_CHECKS": [{"name": "x", "expr": "1 = 1"}]})

    assert checks[-1]["severity"] == "error"


# ─────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────

def _results(*pairs):
    return [
        {"name": name, "severity": severity, "failed": failed}
        for name, severity, failed in pairs
    ]


def test_a_clean_batch_scores_100():
    summary = audit.score(_results(("a", "error", 0), ("b", "warn", 0)), total=100)

    assert summary["pass_pct"] == 100.0
    assert summary["checks_failed"] == 0


def test_pass_pct_uses_the_worst_error_check_not_their_sum():
    """One bad row usually breaks several checks; summing would over-count it."""
    summary = audit.score(_results(("a", "error", 10), ("b", "error", 8)), total=100)

    assert summary["pass_pct"] == 90.0
    assert summary["error_failures"] == 18


def test_warnings_do_not_move_the_score():
    summary = audit.score(_results(("a", "warn", 50)), total=100)

    assert summary["pass_pct"] == 100.0
    assert summary["warn_failures"] == 50


def test_an_empty_batch_does_not_divide_by_zero():
    summary = audit.score(_results(("a", "error", 0)), total=0)

    assert summary["pass_pct"] == 100.0
    assert summary["records"] == 0


# ─────────────────────────────────────────────
# THE VERDICT
# ─────────────────────────────────────────────

def _summary(**overrides):
    return {"records": 1000, "pass_pct": 100.0, "duplicate_pct": 0.0, **overrides}


def test_a_healthy_batch_passes():
    assert audit.assess(_summary(), {})["verdict"] == "pass"


def test_a_slightly_degraded_batch_only_warns():
    verdict = audit.assess(_summary(pass_pct=99.5), {})

    assert verdict["verdict"] == "warn"
    assert verdict["breaches"] == []


def test_breaching_the_floor_fails_with_the_reason():
    verdict = audit.assess(_summary(pass_pct=90.0), {})

    assert verdict["verdict"] == "fail"
    assert verdict["breaches"] == ["min_pass_pct"]


def test_too_many_duplicates_fail():
    assert audit.assess(_summary(duplicate_pct=7.0), {})["breaches"] == ["max_duplicate_pct"]


def test_an_empty_partition_is_a_breach():
    """Silence is the failure mode nobody notices; min_records makes it loud."""
    assert "min_records" in audit.assess(_summary(records=0), {})["breaches"]


def test_a_null_rate_is_only_gated_when_a_limit_is_set():
    summary = _summary(null_pct={"customer_id": 40.0, "campaign": 90.0})

    verdict = audit.assess(summary, {"max_null_pct": {"customer_id": 30.0}})

    assert verdict["breaches"] == ["max_null_pct:customer_id"]


def test_thresholds_from_the_config_override_the_defaults():
    relaxed = {"min_pass_pct": 80.0, "warn_pass_pct": 85.0}

    assert audit.assess(_summary(pass_pct=90.0), relaxed)["verdict"] == "pass"
    # ...and the default gates would have called the same batch a failure.
    assert audit.assess(_summary(pass_pct=90.0), {})["verdict"] == "fail"


# ─────────────────────────────────────────────
# SPARK EXPRESSIONS
# ─────────────────────────────────────────────

def _rows():
    return [
        # a good row
        {"idempotency_key": "k1", "event_type": "order_placed", "product_id": "sku-1",
         "customer_id": "c1", "product_price": 10.0, "order_id": "o1", "quantity": 2,
         "channel": "web", "currency": "EUR", "gross_amount": 20.0,
         "discount_amount": 0.0, "net_amount": 20.0},
        # no idempotency key, no order id on a revenue event
        {"idempotency_key": None, "event_type": "order_placed", "product_id": "sku-2",
         "customer_id": "c2", "product_price": 5.0, "order_id": None, "quantity": 1,
         "channel": "web", "currency": "EUR", "gross_amount": 5.0,
         "discount_amount": 0.0, "net_amount": 5.0},
        # amounts that do not add up, and an unknown channel (a warning)
        {"idempotency_key": "k3", "event_type": "product_viewed", "product_id": "sku-3",
         "customer_id": None, "product_price": 7.0, "order_id": None, "quantity": 1,
         "channel": "carrier_pigeon", "currency": "EUR", "gross_amount": 100.0,
         "discount_amount": 0.0, "net_amount": 42.0},
    ]


@pytest.fixture
def audited(spark):
    from datetime import datetime, timezone

    from pyspark.sql import Row

    rows = [Row(**{**row, "occurred_ts": datetime(2026, 6, 24, 12, tzinfo=timezone.utc)})
            for row in _rows()]
    return spark.createDataFrame(rows)


def test_run_checks_counts_failures_per_rule(audited):
    results = {r["name"]: r["failed"] for r in audit.run_checks(audited, audit.resolve_checks({}))}

    assert results["idempotency_key_present"] == 1
    assert results["order_has_id"] == 1
    assert results["amount_adds_up"] == 1
    assert results["channel_known"] == 1
    assert results["product_id_present"] == 0


def test_a_null_never_passes_a_check_by_accident(audited):
    """`NULL = 'x'` is NULL, which would silently pass a naive predicate."""
    results = {r["name"]: r["failed"] for r in audit.run_checks(audited, audit.resolve_checks({}))}

    assert results["customer_id_present"] == 1


def test_failing_rows_carry_the_names_of_what_they_broke(audited):
    bad = audit.failing_rows(audited, audit.resolve_checks({}), severity="error")
    collected = {row["product_id"]: set(row["failed_checks"]) for row in bad.collect()}

    assert "sku-1" not in collected
    assert collected["sku-2"] == {"idempotency_key_present", "order_has_id"}
    assert collected["sku-3"] == {"amount_adds_up"}


def test_no_error_checks_means_nothing_to_quarantine(audited):
    assert audit.failing_rows(audited, [], severity="error") is None


def test_profile_reports_null_rates(audited):
    nulls = audit.profile(audited, ["customer_id", "order_id", "product_id"], total=3)

    assert nulls["customer_id"] == pytest.approx(33.33, abs=0.01)
    assert nulls["order_id"] == pytest.approx(66.67, abs=0.01)
    assert nulls["product_id"] == 0.0


def test_profile_ignores_columns_the_dataset_does_not_have(audited):
    assert audit.profile(audited, ["not_a_column"], total=3) == {}
