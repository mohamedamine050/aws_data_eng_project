"""Unit tests for common.ecommerce_schema."""

import sys
from pathlib import Path

import pytest

# Make `common` importable (src/ on the path).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from common.ecommerce_schema import (  # noqa: E402
    SCHEMA_VERSION,
    idempotency_key,
    is_valid,
    normalize_record,
    validate_record,
)


def _sample_product():
    return {
        "product_id": "sku-1001",
        "sku": "SKU-1001",
        "name": "Wireless Mouse",
        "category": "electronics",
        "price": 49.99,
    }


def _sample_event():
    return {
        "event_type": "product_viewed",
        "occurred_at": "2026-06-24T12:00",
        "customer_id": "cust-42",
        "segment": "new",
        "currency": "EUR",
        "amount": 49.99,
    }


def test_normalize_record_shape():
    rec = normalize_record(_sample_product(), _sample_event(), "web")

    assert rec["schema_version"] == SCHEMA_VERSION
    assert rec["event_type"] == "product_viewed"
    assert rec["channel"] == "web"
    assert rec["event_id"].startswith("product_viewed-sku-1001")
    assert rec["occurred_at"].startswith("2026-06-24T12:00:00")

    assert rec["product"]["product_id"] == "sku-1001"
    assert rec["product"]["name"] == "Wireless Mouse"
    assert rec["customer"]["customer_id"] == "cust-42"
    assert rec["customer"]["segment"] == "new"
    assert rec["order"]["amount"] == 49.99
    assert rec["order"]["currency"] == "EUR"


def test_normalize_record_falls_back_to_now_without_time():
    event = _sample_event()
    del event["occurred_at"]
    rec = normalize_record(_sample_product(), event, "mobile")
    assert "T" in rec["occurred_at"]


def test_normalize_record_supports_new_api_product_shape():
    product = {
        "id": 7,
        "title": "Handmade Frozen Table",
        "price": 15.5,
        "category": {"name": "Home"},
    }
    event = _sample_event()

    rec = normalize_record(product, event, "web")

    assert rec["product"]["product_id"] == "7"
    assert rec["product"]["name"] == "Handmade Frozen Table"
    assert rec["product"]["category"] == "Home"
    assert rec["product"]["price"] == 15.5


# ─────────────────────────────────────────────
# v3 — SESSION / DEVICE / GEO / MARKETING
# ─────────────────────────────────────────────

def test_normalize_record_carries_the_v3_context():
    event = {
        **_sample_event(),
        "session_id": "sess-1",
        "sequence": 3,
        "device_type": "mobile",
        "device_os": "ios",
        "user_agent": "Mozilla/5.0",
        "country": "FR",
        "city": "Lyon",
        "campaign": "spring_sale",
        "utm_source": "google",
        "utm_medium": "cpc",
        "payment_method": "card",
    }
    rec = normalize_record(_sample_product(), event, "mobile_app")

    assert rec["schema_version"] == "3.0"
    assert rec["session"] == {"session_id": "sess-1", "sequence": 3}
    assert rec["device"] == {"type": "mobile", "os": "ios", "user_agent": "Mozilla/5.0"}
    assert rec["geo"] == {"country": "FR", "city": "Lyon"}
    assert rec["marketing"] == {"campaign": "spring_sale", "source": "google", "medium": "cpc"}
    assert rec["order"]["payment_method"] == "card"


def test_normalize_record_defaults_when_context_is_absent():
    rec = normalize_record(_sample_product(), _sample_event(), "web")

    assert rec["session"] == {"session_id": None, "sequence": None}
    assert rec["device"]["type"] == "unknown"
    assert rec["order"]["payment_method"] == "unknown"
    assert rec["order"]["currency"] == "EUR"
    assert rec["order"]["quantity"] == 1


# ─────────────────────────────────────────────
# v3 — BASKET ECONOMICS
# ─────────────────────────────────────────────

def test_quantity_and_discount_drive_gross_and_net():
    event = {**_sample_event(), "quantity": 3, "discount_pct": 10.0}
    del event["amount"]

    order = normalize_record(_sample_product(), event, "web")["order"]

    assert order["quantity"] == 3
    assert order["unit_price"] == 49.99
    assert order["gross_amount"] == 149.97
    assert order["discount_amount"] == 15.0
    assert order["net_amount"] == 134.97
    assert order["amount"] == order["net_amount"]   # v2 alias


def test_an_explicit_amount_overrides_the_arithmetic():
    """Upstream's authoritative figure must win over our derivation."""
    event = {**_sample_event(), "quantity": 3, "amount": 100.0}

    order = normalize_record(_sample_product(), event, "web")["order"]
    assert order["net_amount"] == 100.0


def test_missing_price_leaves_amounts_null():
    product = {**_sample_product()}
    del product["price"]
    event = {**_sample_event()}
    del event["amount"]

    order = normalize_record(product, event, "web")["order"]
    assert order["gross_amount"] is None
    assert order["net_amount"] is None


# ─────────────────────────────────────────────
# IDEMPOTENCY
# ─────────────────────────────────────────────

def test_idempotency_key_is_stable_across_reingestion():
    first = normalize_record(_sample_product(), _sample_event(), "web")
    second = normalize_record(_sample_product(), _sample_event(), "web")

    # Ingestion metadata may differ between the two; the business identity does not.
    assert first["idempotency_key"] == second["idempotency_key"]


def test_idempotency_key_changes_with_the_business_identity():
    base = normalize_record(_sample_product(), _sample_event(), "web")
    other = normalize_record(_sample_product(), {**_sample_event(), "customer_id": "cust-99"}, "web")

    assert base["idempotency_key"] != other["idempotency_key"]


def test_idempotency_key_tolerates_a_bare_record():
    assert idempotency_key({}) == idempotency_key({})


# ─────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────

def test_validate_record_accepts_a_normalized_record():
    assert validate_record(normalize_record(_sample_product(), _sample_event(), "web")) == []
    assert is_valid(normalize_record(_sample_product(), _sample_event(), "web"))


def test_validate_record_rejects_a_non_dict():
    assert validate_record("nope") == ["not_a_dict"]


def test_validate_record_reports_every_error_at_once():
    errors = validate_record({"event_type": "product_viewed"})

    assert "missing:occurred_at" in errors
    assert "missing:product" in errors
    assert "missing:customer" in errors


def test_validate_record_flags_a_bad_timestamp():
    rec = normalize_record(_sample_product(), _sample_event(), "web")
    rec["occurred_at"] = "24/06/2026"

    assert "invalid_format:occurred_at" in validate_record(rec)


@pytest.mark.parametrize("field,value,expected", [
    ("quantity", 0, "out_of_range:order.quantity"),
    ("quantity", 5000, "out_of_range:order.quantity"),
    ("discount_pct", -5, "out_of_range:order.discount_pct"),
    ("discount_pct", 150, "out_of_range:order.discount_pct"),
    ("net_amount", -1.0, "out_of_range:order.net_amount"),
])
def test_validate_record_enforces_order_bounds(field, value, expected):
    rec = normalize_record(_sample_product(), _sample_event(), "web")
    rec["order"][field] = value

    assert expected in validate_record(rec)


def test_validate_record_rejects_a_missing_product_id():
    rec = normalize_record(_sample_product(), _sample_event(), "web")
    rec["product"]["product_id"] = None

    assert "missing:product.product_id" in validate_record(rec)


def test_strict_mode_enforces_the_controlled_vocabularies():
    rec = normalize_record(_sample_product(), {**_sample_event(), "event_type": "teleported"}, "carrier_pigeon")

    assert validate_record(rec) == []                       # tolerant by default
    strict = validate_record(rec, strict=True)
    assert "unknown_value:event_type" in strict
    assert "unknown_value:channel" in strict


def test_strict_mode_flags_an_unsupported_currency():
    rec = normalize_record(_sample_product(), {**_sample_event(), "currency": "XYZ"}, "web")

    assert "unknown_value:order.currency" in validate_record(rec, strict=True)
