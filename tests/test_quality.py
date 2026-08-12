"""Unit tests for common.quality — the declarative rule engine."""

from datetime import datetime, timedelta, timezone

from common import quality
from common.ecommerce_schema import normalize_record


def _record(**event_overrides):
    product = {"product_id": "sku-1", "sku": "SKU-1", "name": "Mouse", "category": "electronics", "price": 19.99}
    event = {
        "event_type": "order_placed",
        "occurred_at": "2026-06-24T12:00:00+00:00",
        "session_id": "sess-1",
        "sequence": 1,
        "customer_id": "cust-1",
        "segment": "new",
        "currency": "EUR",
        "quantity": 2,
        "discount_pct": 10.0,
        "order_id": "ord-1",
        **event_overrides,
    }
    return normalize_record(product, event, "web")


# ── PER-RECORD ───────────────────────────────────────────────

def test_a_well_formed_record_passes_every_rule():
    outcome = quality.check_record(_record())
    assert outcome == {"errors": [], "warnings": []}


def test_missing_product_id_is_an_error():
    record = _record()
    record["product"]["product_id"] = None

    outcome = quality.check_record(record)
    assert "product_id_present" in outcome["errors"]
    assert "schema_valid" in outcome["errors"]


def test_unparseable_timestamp_is_an_error():
    record = _record()
    record["occurred_at"] = "not-a-timestamp"

    assert "timestamp_parseable" in quality.check_record(record)["errors"]


def test_out_of_range_quantity_is_an_error():
    record = _record()
    record["order"]["quantity"] = 5000

    assert "quantity_in_range" in quality.check_record(record)["errors"]


def test_negative_amount_is_an_error():
    record = _record()
    record["order"]["net_amount"] = -10.0

    assert "amount_non_negative" in quality.check_record(record)["errors"]


def test_unknown_currency_is_only_a_warning():
    record = _record(currency="XYZ")
    outcome = quality.check_record(record)

    assert outcome["errors"] == []
    assert "known_currency" in outcome["warnings"]


def test_missing_customer_id_is_only_a_warning():
    record = _record()
    record["customer"]["customer_id"] = None

    outcome = quality.check_record(record)
    assert outcome["errors"] == []
    assert "customer_id_present" in outcome["warnings"]


def test_future_timestamp_is_flagged():
    ahead = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
    record = _record(occurred_at=ahead)

    assert "timestamp_not_in_future" in quality.check_record(record)["warnings"]


def test_inconsistent_amounts_are_flagged():
    record = _record()
    record["order"]["net_amount"] = 999.0

    assert "amount_consistent" in quality.check_record(record)["warnings"]


def test_order_event_without_order_id_is_flagged():
    record = _record()
    record["order"]["order_id"] = None

    assert "order_event_has_order_id" in quality.check_record(record)["warnings"]


def test_a_crashing_rule_counts_as_a_failure():
    exploding = quality.Rule("boom", quality.ERROR, "always raises",
                             lambda record: (_ for _ in ()).throw(RuntimeError("boom")))

    assert quality.check_record(_record(), rules=[exploding])["errors"] == ["boom"]


# ── BATCH ────────────────────────────────────────────────────

def test_evaluate_reports_totals_and_a_clean_verdict():
    report = quality.evaluate([_record(), _record(customer_id="cust-2")])

    assert report["verdict"] == "pass"
    assert report["totals"]["records"] == 2
    assert report["totals"]["failed"] == 0
    assert report["totals"]["pass_pct"] == 100.0
    assert report["breaches"] == []


def test_evaluate_fails_below_the_pass_threshold():
    bad = _record()
    bad["product"]["product_id"] = None

    report = quality.evaluate([_record(), bad])

    assert report["verdict"] == "fail"
    assert "min_pass_pct" in report["breaches"]
    assert report["totals"]["pass_pct"] == 50.0
    assert report["rules"]["product_id_present"]["failed"] == 1


def test_evaluate_warns_on_warning_only_failures():
    report = quality.evaluate([_record(currency="XYZ")])

    assert report["verdict"] == "warn"
    assert report["breaches"] == []
    assert report["totals"]["failed"] == 0


def test_evaluate_applies_the_duplicate_threshold():
    records = [_record() for _ in range(10)]
    report = quality.evaluate(records, duplicates=3)

    assert "max_duplicate_pct" in report["breaches"]
    assert report["totals"]["duplicate_pct"] == 30.0


def test_evaluate_honours_threshold_overrides():
    bad = _record()
    bad["product"]["product_id"] = None

    report = quality.evaluate([_record(), bad], thresholds={"min_pass_pct": 10.0, "warn_pass_pct": 20.0})

    assert report["verdict"] == "pass"


def test_evaluate_of_an_empty_batch_is_a_pass():
    report = quality.evaluate([])
    assert report["verdict"] == "pass"
    assert report["totals"]["pass_pct"] == 100.0


# ── PARTITION ────────────────────────────────────────────────

def test_partition_splits_and_annotates():
    bad = _record()
    bad["product"]["product_id"] = None

    accepted, rejected = quality.partition([_record(), bad])

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert "product_id_present" in rejected[0]["reasons"]
    assert rejected[0]["record"] is bad
    assert rejected[0]["rejected_at"]


def test_partition_keeps_warning_only_records():
    accepted, rejected = quality.partition([_record(currency="XYZ")])

    assert len(accepted) == 1
    assert rejected == []
