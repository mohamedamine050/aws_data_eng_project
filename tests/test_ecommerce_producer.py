"""Unit tests for the ecommerce_producer Lambda handler."""

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Let boto3.client(...) be created at import time without real credentials,
# and make `common` importable when the handler is loaded.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))


@pytest.fixture
def producer():
    """Load the ecommerce_producer handler under a unique module name."""
    spec = importlib.util.spec_from_file_location(
        "producer_handler", _SRC / "lambdas/ecommerce_producer/handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── ARGS & CONFIG ────────────────────────────────────────────

def test_get_args_from_event(producer):
    assert producer.get_args({"CONFIG_PATH": "s3://b/k.json"}) == {"CONFIG_PATH": "s3://b/k.json"}


def test_get_args_env_fallback(producer, monkeypatch):
    monkeypatch.setenv("CONFIG_PATH", "config/local.json")
    assert producer.get_args({})["CONFIG_PATH"] == "config/local.json"


def test_get_args_missing_raises(producer, monkeypatch):
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    with pytest.raises(RuntimeError):
        producer.get_args({})


def test_load_config_local_file(producer, tmp_path):
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"QUEUE_URL": "https://sqs/q"}), encoding="utf-8")
    assert producer.load_config(str(cfg))["QUEUE_URL"] == "https://sqs/q"


# ── PRODUCTS ────────────────────────────────────────────────

def test_slugify(producer):
    assert producer._slugify("Wireless Mouse", "US") == "wireless-mouse-us"
    assert producer._slugify("Keyboard", None) == "keyboard"


def test_resolve_products_without_sources_returns_empty(producer):
    products = producer.resolve_products(None)
    assert products == []


def test_resolve_products_custom(producer):
    products = producer.resolve_products([
        {"product_id": "sku-200", "name": "Keyboard", "category": "electronics", "price": 19.99},
    ])
    assert products[0]["product_id"] == "sku-200"
    assert products[0]["name"] == "Keyboard"


def test_resolve_products_skips_malformed(producer):
    products = producer.resolve_products([
        {"product_id": "sku-1", "name": "Good", "price": 10.0},
        {"name": "Missing id", "price": 5.0},
    ])
    assert [product["product_id"] for product in products] == ["sku-1"]


def test_resolve_products_from_api_url(producer, monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps([
                {"id": 1, "title": "Widget", "category": "electronics", "price": 12.34},
            ]).encode("utf-8")

    def fake_urlopen(url, timeout=10):
        assert url == "https://example.test/products"
        return FakeResponse()

    monkeypatch.setattr(producer.urllib.request, "urlopen", fake_urlopen)
    products = producer.resolve_products(None, api_url="https://example.test/products")
    assert products[0]["name"] == "Widget"
    assert products[0]["price"] == 12.34


def test_lambda_handler_uses_config_file_for_api_url_only(producer, monkeypatch, tmp_path):
    cfg = tmp_path / "prod.json"
    cfg.write_text(json.dumps({"QUEUE_URL": "https://sqs/demo"}), encoding="utf-8")

    monkeypatch.setenv("ECOMMERCE_API_URL", "https://env.example/products")
    calls = []

    def fail_urlopen(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("environment fallback should not be used")

    monkeypatch.setattr(producer.urllib.request, "urlopen", fail_urlopen)
    result = producer.lambda_handler({"CONFIG_PATH": str(cfg)}, None)

    assert result["generated"] == 0
    assert calls == []


# ── EXTRACT ──────────────────────────────────────────────────

def test_fetch_event_ok(producer):
    product = {"product_id": "sku-1", "name": "Keyboard", "category": "electronics", "price": 19.99}
    rec = producer.fetch_event(product, "web", 10)
    assert rec["event_type"] == "product_viewed"
    assert rec["product"]["product_id"] == "sku-1"
    assert rec["product"]["name"] == "Keyboard"

def test_fetch_event_uses_current_timestamp(producer):
    product = {"product_id": "sku-1", "name": "Keyboard", "category": "electronics", "price": 19.99}
    rec = producer.fetch_event(product, "web", 10)
    occurred_at = datetime.fromisoformat(rec["occurred_at"])
    now = datetime.now(timezone.utc)
    assert abs((occurred_at - now).total_seconds()) < 15

# ── LOAD (SQS) ───────────────────────────────────────────────

def _record(product_id="sku-1"):
    return {"product": {"product_id": product_id}, "event_type": "product_viewed"}


def test_send_messages_success(producer, monkeypatch):
    captured = {}

    def fake_send(QueueUrl, Entries):
        captured["url"] = QueueUrl
        captured["n"] = len(Entries)
        return {"Successful": [{} for _ in Entries], "Failed": []}

    monkeypatch.setattr(producer._SQS, "send_message_batch", fake_send)
    failed = producer.send_messages("https://sqs/queue", [_record(), _record("sku-2")])
    assert failed == 0
    assert captured == {"url": "https://sqs/queue", "n": 2}


def test_send_messages_batches_over_10(producer, monkeypatch):
    calls = []
    monkeypatch.setattr(producer._SQS, "send_message_batch",
                        lambda QueueUrl, Entries: calls.append(len(Entries)) or {"Failed": []})
    producer.send_messages("q", [_record() for _ in range(23)])
    assert calls == [10, 10, 3]


def test_send_messages_empty(producer):
    assert producer.send_messages("q", []) == 0


def test_send_messages_total_failure(producer, monkeypatch):
    def boom(QueueUrl, Entries):
        raise producer.BotoCoreError()

    monkeypatch.setattr(producer._SQS, "send_message_batch", boom)
    assert producer.send_messages("q", [_record()]) == 1


def test_send_messages_partial_failure(producer, monkeypatch):
    monkeypatch.setattr(producer._SQS, "send_message_batch",
                        lambda QueueUrl, Entries: {"Failed": [{"Id": "0", "Code": "X"}]})
    assert producer.send_messages("q", [_record(), _record("x")]) == 1


# ── HANDLER ──────────────────────────────────────────────────

def test_lambda_handler_end_to_end(producer, monkeypatch, tmp_path):
    cfg = tmp_path / "prod.json"
    cfg.write_text(json.dumps({
        "QUEUE_URL": "https://sqs/demo",
        "PRODUCTS": [{"product_id": "sku-1", "name": "Keyboard", "category": "electronics", "price": 19.99}],
        "EVENT_TYPE": "custom_event",
        "CUSTOMER_ID": "cust-42",
        "CUSTOMER_SEGMENT": "vip",
        "CURRENCY": "USD",
    }), encoding="utf-8")

    sent = []
    monkeypatch.setattr(producer._SQS, "send_message_batch",
                        lambda QueueUrl, Entries: (sent.extend(Entries), {"Failed": []})[1])

    result = producer.lambda_handler({"CONFIG_PATH": str(cfg)}, None)
    assert result == {"products_requested": 1, "generated": 1, "sent": 1, "failed": 0}
    assert len(sent) == 1
