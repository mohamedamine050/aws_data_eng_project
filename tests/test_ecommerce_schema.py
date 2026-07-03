"""Unit tests for common.ecommerce_schema."""

import sys
from pathlib import Path

# Make `common` importable (src/ on the path).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from common.ecommerce_schema import (  # noqa: E402
    SCHEMA_VERSION,
    normalize_record,
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
