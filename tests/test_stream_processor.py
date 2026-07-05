"""Unit tests for the stream_processor Lambda handler."""

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
def processor():
    """Load the stream_processor handler under a unique module name."""
    spec = importlib.util.spec_from_file_location(
        "processor_handler", _SRC / "lambdas/stream_processor/handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sqs_record(event_obj, message_id="1"):
    return {
        "messageId": message_id,
        "receiptHandle": "rh",
        "body": json.dumps(event_obj),
        "eventSourceARN": "arn:aws:sqs:::ecommerce-queue",
    }


def _valid_event():
    return {
        "occurred_at": "2026-06-24T12:00:00+00:00",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1", "name": "Keyboard"},
        "customer": {"customer_id": "cust-7"},
    }


# ── ARGS & CONFIG ────────────────────────────────────────────

def test_get_args_event(processor):
    assert processor.get_args({"CONFIG_PATH": "s3://b/k"})["CONFIG_PATH"] == "s3://b/k"


def test_get_args_env_fallback(processor, monkeypatch):
    monkeypatch.setenv("CONFIG_PATH", "config/sp.json")
    assert processor.get_args({"Records": []})["CONFIG_PATH"] == "config/sp.json"


def test_get_args_missing_raises(processor, monkeypatch):
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    with pytest.raises(RuntimeError):
        processor.get_args({"Records": []})


def test_load_config_local(processor, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "b"}), encoding="utf-8")
    assert processor.load_config(str(cfg))["OUTPUT_BUCKET"] == "b"


# ── DECODE & VALIDATE ────────────────────────────────────────

def test_decode_record(processor):
    rec = _sqs_record({"hello": "world"})
    assert processor._decode_record(rec) == {"hello": "world"}


def test_decode_empty_raises(processor):
    rec = {"messageId": "1", "body": "   "}
    with pytest.raises(processor.InvalidRecordError):
        processor._decode_record(rec)


def test_validate_ok(processor):
    processor._validate(_valid_event())  # should not raise


def test_validate_missing_key(processor):
    bad = _valid_event()
    del bad["product"]
    with pytest.raises(processor.InvalidRecordError):
        processor._validate(bad)


def test_validate_missing_product_id(processor):
    bad = _valid_event()
    bad["product"] = {"name": "Keyboard"}
    with pytest.raises(processor.InvalidRecordError):
        processor._validate(bad)


# ── PARTITIONING & KEYS ──────────────────────────────────────

def test_partition_for(processor):
    assert processor._partition_for({"occurred_at": "2026-06-24T15:30:00+00:00"}) == ("2026-06-24", "15")


def test_partition_for_bad_timestamp_uses_now(processor):
    d, h = processor._partition_for({"occurred_at": None})
    assert len(d) == 10 and len(h) == 2


def test_partition_for_datetime_object_uses_utc(processor):
    assert processor._partition_for({"occurred_at": datetime(2026, 6, 24, 15, 30, tzinfo=timezone.utc)}) == ("2026-06-24", "15")


def test_build_key_format(processor):
    key = processor._build_key("raw/")
    assert key.startswith("raw/")
    assert key.endswith(".json")


def test_build_key_adds_trailing_slash(processor):
    key = processor._build_key("raw")
    assert key.startswith("raw/")
    assert key.endswith(".json")


# ── HANDLER ──────────────────────────────────────────────────

def test_handler_writes_to_s3(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "demo-bucket", "RAW_PREFIX": "raw/"}), encoding="utf-8")

    puts = []
    monkeypatch.setattr(processor.S3, "put_object",
                        lambda **kw: puts.append(kw) or {})

    event = {
        "CONFIG_PATH": str(cfg),
        "Records": [_sqs_record(_valid_event()), _sqs_record(_valid_event(), message_id="2")],
    }
    result = processor.handler(event, None)

    assert result == {"batchItemFailures": []}
    assert len(puts) == 1
    assert puts[0]["Bucket"] == "demo-bucket"
    assert puts[0]["Key"].startswith("raw/")
    assert puts[0]["Body"].decode("utf-8").count("\n") == 2


def test_handler_drops_invalid_records(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "b"}), encoding="utf-8")

    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    bad = _valid_event()
    del bad["product"]
    event = {"CONFIG_PATH": str(cfg), "Records": [_sqs_record(bad)]}

    result = processor.handler(event, None)
    assert result == {"batchItemFailures": []}
    assert puts == []


def test_handler_reports_failure_on_s3_error(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "b"}), encoding="utf-8")

    def boom(**_):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(processor.S3, "put_object", boom)
    event = {"CONFIG_PATH": str(cfg), "Records": [_sqs_record(_valid_event(), message_id="42")]}

    result = processor.handler(event, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "42"}]
