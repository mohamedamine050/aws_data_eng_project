"""Unit tests for the stream_processor's v3 capabilities.

Covers the input modes added alongside SQS (Step Functions, direct invoke), the
partitioned ``bronze/events`` zone, the ``quarantine`` zone and in-batch
idempotency. The original SQS happy-path tests live in
``test_stream_processor.py``. Partner files are no longer this Lambda's job —
see ``tests/test_glue_landing_ingest.py``.
"""

import importlib.util
import json
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parent.parent / "src"


@pytest.fixture
def processor():
    spec = importlib.util.spec_from_file_location(
        "processor_handler_inputs", _SRC / "lambdas/stream_processor/handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _valid_event():
    return {
        "occurred_at": "2026-06-24T12:00:00+00:00",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1", "name": "Keyboard"},
        "customer": {"customer_id": "cust-7"},
    }


def _sqs_record(event_obj, message_id="1"):
    return {
        "messageId": message_id,
        "receiptHandle": "rh",
        "body": json.dumps(event_obj),
        "eventSourceARN": "arn:aws:sqs:::ecommerce-queue",
    }


def _config(tmp_path, **overrides):
    cfg = tmp_path / "sp.json"
    cfg.write_text(json.dumps({"OUTPUT_BUCKET": "lake", **overrides}), encoding="utf-8")
    return cfg


def _raw(puts):
    return [p for p in puts if p["Key"].startswith("bronze/events/")]


def _rejected(puts):
    return [p for p in puts if p["Key"].startswith("quarantine/events/")]


# ── INPUT MODE DETECTION ─────────────────────────────────────

def test_detect_source_sqs(processor):
    assert processor.detect_source({"Records": [{"messageId": "1", "body": "{}"}]}) == "sqs"


def test_an_s3_notification_is_no_longer_this_lambda_s_business(processor):
    """Partner files go to the Glue landing job; a stray notification here must
    not be silently treated as a batch of messages to decode."""
    event = {"Records": [{"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}]}

    assert processor.detect_source(event) != "s3"


def test_detect_source_stepfunctions(processor):
    assert processor.detect_source({"messages": []}) == "stepfunctions"


def test_detect_source_direct(processor):
    assert processor.detect_source({"event_type": "product_viewed"}) == "direct"


def test_detect_source_of_an_empty_records_list_is_direct(processor):
    assert processor.detect_source({"Records": []}) == "direct"


# ── S3 BATCH ON-RAMP ─────────────────────────────────────────


# ── PARTITIONED WRITES ───────────────────────────────────────

def test_build_key_is_hive_partitioned(processor):
    key = processor._build_key("raw/", "2026-06-24", "15")
    assert key.startswith("raw/dt=2026-06-24/hour=15/")
    assert key.endswith(".json")


def test_build_key_without_partitions_stays_flat(processor):
    assert processor._build_key("raw/").startswith("raw/2")


def test_handler_writes_one_object_per_event_time_partition(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    noon = _valid_event()
    evening = {**_valid_event(), "occurred_at": "2026-06-24T20:00:00+00:00"}
    next_day = {**_valid_event(), "occurred_at": "2026-06-25T09:00:00+00:00"}

    processor.handler({
        "CONFIG_PATH": str(cfg),
        "Records": [_sqs_record(noon, "1"), _sqs_record(evening, "2"), _sqs_record(next_day, "3")],
    }, None)

    folders = sorted(p["Key"].rsplit("/", 1)[0] for p in _raw(puts))
    assert folders == [
        "bronze/events/dt=2026-06-24/hour=12",
        "bronze/events/dt=2026-06-24/hour=20",
        "bronze/events/dt=2026-06-25/hour=09",
    ]


def test_late_arriving_events_land_in_their_own_event_time_partition(processor, monkeypatch, tmp_path):
    """Partitioning on event time, not arrival time, is what makes replays correct."""
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    late = {**_valid_event(), "occurred_at": "2025-01-01T03:00:00+00:00"}
    processor.handler({"CONFIG_PATH": str(cfg), "Records": [_sqs_record(late)]}, None)

    assert _raw(puts)[0]["Key"].startswith("bronze/events/dt=2025-01-01/hour=03/")


# ── IDEMPOTENCY ──────────────────────────────────────────────

def test_handler_deduplicates_on_the_idempotency_key(processor, monkeypatch, tmp_path):
    """SQS is at-least-once: the same event can arrive twice in one batch."""
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    duplicated = {**_valid_event(), "idempotency_key": "abc123"}
    processor.handler({
        "CONFIG_PATH": str(cfg),
        "Records": [_sqs_record(duplicated, "1"), _sqs_record(duplicated, "2")],
    }, None)

    assert len(_raw(puts)) == 1
    assert _raw(puts)[0]["Body"].decode("utf-8").count("\n") == 1


def test_distinct_idempotency_keys_are_both_kept(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    processor.handler({
        "CONFIG_PATH": str(cfg),
        "Records": [
            _sqs_record({**_valid_event(), "idempotency_key": "a"}, "1"),
            _sqs_record({**_valid_event(), "idempotency_key": "b"}, "2"),
        ],
    }, None)

    assert _raw(puts)[0]["Body"].decode("utf-8").count("\n") == 2


# ── REJECTED ZONE ────────────────────────────────────────────

def test_handler_archives_an_undecodable_payload(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    result = processor.handler(
        {"CONFIG_PATH": str(cfg), "Records": [{"messageId": "9", "body": "{not json"}]}, None
    )

    assert result == {"batchItemFailures": []}
    assert len(_rejected(puts)) == 1
    archived = json.loads(_rejected(puts)[0]["Body"].decode("utf-8").strip())
    assert archived["reasons"] == ["undecodable"]
    assert archived["record"]["message_id"] == "9"


def test_rejected_records_keep_their_rule_names(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    bad = _valid_event()
    bad["product"] = {"name": "no id"}
    processor.handler({"CONFIG_PATH": str(cfg), "Records": [_sqs_record(bad, "7")]}, None)

    archived = json.loads(_rejected(puts)[0]["Body"].decode("utf-8").strip())
    assert "product_id_present" in archived["reasons"]
    assert archived["message_id"] == "7"


def test_a_failure_to_archive_never_breaks_the_run(processor, monkeypatch, tmp_path):
    """The good records already landed; archiving a bad one must not force a retry."""
    cfg = _config(tmp_path)

    def only_rejected_fails(**kw):
        if kw["Key"].startswith("quarantine/events/"):
            raise RuntimeError("s3 down")
        return {}

    monkeypatch.setattr(processor.S3, "put_object", only_rejected_fails)

    bad = _valid_event()
    del bad["product"]

    result = processor.handler({
        "CONFIG_PATH": str(cfg),
        "Records": [_sqs_record(_valid_event(), "1"), _sqs_record(bad, "2")],
    }, None)

    assert result == {"batchItemFailures": []}


# ── STEP FUNCTIONS / DIRECT ──────────────────────────────────

def test_handler_accepts_a_step_functions_payload(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    processor.handler({"CONFIG_PATH": str(cfg), "messages": [_valid_event()]}, None)

    assert len(_raw(puts)) == 1


def test_handler_accepts_a_direct_invocation(processor, monkeypatch, tmp_path):
    cfg = _config(tmp_path)
    puts = []
    monkeypatch.setattr(processor.S3, "put_object", lambda **kw: puts.append(kw) or {})

    processor.handler({"CONFIG_PATH": str(cfg), **_valid_event()}, None)

    assert len(_raw(puts)) == 1
