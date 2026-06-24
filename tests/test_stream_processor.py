"""Unit tests for the stream_processor Lambda handler."""

import base64
import importlib.util
import json
import os
import sys
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


def _kinesis_record(event_obj, seq="1", event_id="shardId-000000000000:49"):
    data = base64.b64encode(json.dumps(event_obj).encode("utf-8")).decode("utf-8")
    return {
        "kinesis": {"data": data, "sequenceNumber": seq, "partitionKey": "pk"},
        "eventID": event_id,
        "eventSourceARN": "arn:aws:kinesis:::stream/x",
    }


def _valid_event():
    return {
        "observed_at": "2026-06-24T12:00:00+00:00",
        "location": {"city": "Tunis", "location_id": "tunis-tn"},
        "measurement": {"temperature": 30.0},
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
    rec = _kinesis_record({"hello": "world"})
    assert processor._decode_record(rec) == {"hello": "world"}


def test_decode_empty_raises(processor):
    rec = _kinesis_record("")
    rec["kinesis"]["data"] = base64.b64encode(b"   ").decode()
    with pytest.raises(processor.InvalidRecordError):
        processor._decode_record(rec)


def test_validate_ok(processor):
    processor._validate(_valid_event())  # should not raise


def test_validate_missing_key(processor):
    bad = _valid_event()
    del bad["measurement"]
    with pytest.raises(processor.InvalidRecordError):
        processor._validate(bad)


def test_validate_null_temperature(processor):
    bad = _valid_event()
    bad["measurement"]["temperature"] = None
    with pytest.raises(processor.InvalidRecordError):
        processor._validate(bad)


# ── PARTITIONING & KEYS ──────────────────────────────────────

def test_partition_for(processor):
    assert processor._partition_for({"observed_at": "2026-06-24T15:30:00+00:00"}) == ("2026-06-24", "15")


def test_partition_for_bad_timestamp_uses_now(processor):
    d, h = processor._partition_for({"observed_at": None})
    assert len(d) == 10 and len(h) == 2  # YYYY-MM-DD / HH


def test_shard_token(processor):
    rec = {"eventID": "shardId-000000000000:49590"}
    assert processor._shard_token(rec) == "s000000000000"


def test_build_key_format(processor):
    key = processor._build_key("2026-06-24", "15", "s00", "raw/")
    assert key.startswith("raw/dt=2026-06-24/hour=15/s00-")
    assert key.endswith(".json")


def test_build_key_adds_trailing_slash(processor):
    key = processor._build_key("2026-06-24", "15", "s00", "raw")  # no trailing /
    assert key.startswith("raw/dt=2026-06-24/")


# ── HANDLER ──────────────────────────────────────────────────

def test_handler_writes_to_s3(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "demo-bucket", "RAW_PREFIX": "raw/"}), encoding="utf-8")

    puts = []
    monkeypatch.setattr(processor.S3, "put_object",
                        lambda **kw: puts.append(kw) or {})

    event = {
        "CONFIG_PATH": str(cfg),
        "Records": [_kinesis_record(_valid_event()), _kinesis_record(_valid_event(), seq="2")],
    }
    result = processor.handler(event, None)

    assert result == {"batchItemFailures": []}
    assert len(puts) == 1
    assert puts[0]["Bucket"] == "demo-bucket"
    assert puts[0]["Key"].startswith("raw/dt=")
    # Two NDJSON lines written.
    assert puts[0]["Body"].decode("utf-8").count("\n") == 2


def test_handler_drops_invalid_records(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "b"}), encoding="utf-8")

    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    bad = _valid_event()
    del bad["measurement"]  # invalid -> dropped, must NOT block
    event = {"CONFIG_PATH": str(cfg), "Records": [_kinesis_record(bad)]}

    result = processor.handler(event, None)
    assert result == {"batchItemFailures": []}
    assert puts == []  # nothing valid to write


def test_handler_reports_failure_on_s3_error(processor, monkeypatch, tmp_path):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "b"}), encoding="utf-8")

    def boom(**_):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(processor.S3, "put_object", boom)
    event = {"CONFIG_PATH": str(cfg), "Records": [_kinesis_record(_valid_event(), seq="42")]}

    result = processor.handler(event, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "42"}]
