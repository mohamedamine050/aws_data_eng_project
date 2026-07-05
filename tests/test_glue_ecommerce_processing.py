import json
import sys
from io import BytesIO
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))

import jobs.glue_ecommerce_processing as glue_job

from jobs.glue_ecommerce_processing import (
    _clean_numeric,
    _clean_string,
    _enrich_record,
    _validate_record,
    run_job,
)


def test_clean_string():
    assert _clean_string("  hello  ") == "hello"
    assert _clean_string("") is None
    assert _clean_string(None) is None
    assert _clean_string("a" * 2000)[:1000] == "a" * 1000


def test_clean_numeric():
    assert _clean_numeric(42) == 42.0
    assert _clean_numeric("3.14") == 3.14
    assert _clean_numeric(None) is None
    assert _clean_numeric("invalid") is None


def test_validate_record_valid():
    record = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1"},
        "customer": {"customer_id": "cust-7"},
    }
    is_valid, error = _validate_record(record)
    assert is_valid is True
    assert error is None


def test_validate_record_missing_fields():
    record = {
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1"},
    }
    is_valid, error = _validate_record(record)
    assert is_valid is False
    assert "missing_fields" in error


def test_validate_record_missing_product_id():
    record = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "product_viewed",
        "product": {},
        "customer": {"customer_id": "cust-7"},
    }
    is_valid, error = _validate_record(record)
    assert is_valid is False
    assert error == "missing_product_id"


def test_validate_record_invalid_customer_format():
    record = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1"},
        "customer": "not-a-dict",
    }
    is_valid, error = _validate_record(record)
    assert is_valid is False
    assert error == "invalid_customer_format"


def test_enrich_record_adds_partition_fields():
    raw_record = {
        "occurred_at": "2026-06-24T15:30:00+00:00",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1", "name": "Keyboard", "price": 89.99},
        "customer": {"customer_id": "cust-7", "segment": "premium"},
    }

    processed = _enrich_record(raw_record)

    assert processed["event_type"] == "product_viewed"
    assert processed["product_id"] == "sku-1"
    assert processed["product_name"] == "Keyboard"
    assert processed["customer_id"] == "cust-7"
    assert processed["partition_date"] == "2026-06-24"
    assert processed["partition_hour"] == "15"
    assert processed["product_price"] == 89.99


def test_enrich_record_price_categorization():
    # Budget
    record1 = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "purchase",
        "product": {"product_id": "sku-1", "price": 19.99},
        "customer": {"customer_id": "cust-1"},
    }
    assert _enrich_record(record1)["price_category"] == "budget"

    # Mid
    record2 = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "purchase",
        "product": {"product_id": "sku-2", "price": 99.99},
        "customer": {"customer_id": "cust-2"},
    }
    assert _enrich_record(record2)["price_category"] == "mid"

    # Premium
    record3 = {
        "occurred_at": "2026-06-24T15:30:00Z",
        "event_type": "purchase",
        "product": {"product_id": "sku-3", "price": 599.99},
        "customer": {"customer_id": "cust-3"},
    }
    assert _enrich_record(record3)["price_category"] == "premium"


def test_enrich_record_handles_invalid_timestamp_and_missing_price():
    raw_record = {
        "occurred_at": "not-a-timestamp",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1"},
        "customer": {"customer_id": "cust-7"},
    }

    processed = _enrich_record(raw_record)

    assert processed["partition_date"] == "unknown"
    assert processed["partition_hour"] == "unknown"
    assert processed["price_category"] == "unknown"


def test_load_config_from_s3(monkeypatch):
    payload = {"OUTPUT_BUCKET": "demo-bucket", "RAW_PREFIX": "raw/"}
    monkeypatch.setattr(
        glue_job.s3,
        "get_object",
        lambda **kwargs: {"Body": BytesIO(json.dumps(payload).encode("utf-8"))},
    )

    assert glue_job.load_config("s3://demo/config.json") == payload


def test_list_s3_files_filters_json(monkeypatch):
    class DummyPaginator:
        def paginate(self, **kwargs):
            return [
                {"Contents": [{"Key": "raw/a.json"}, {"Key": "raw/b.txt"}]},
                {"Contents": [{"Key": "raw/c.json"}, {"Key": "raw/d.csv"}]},
            ]

    monkeypatch.setattr(glue_job.s3, "get_paginator", lambda name: DummyPaginator())

    assert glue_job.list_s3_files("demo-bucket", "raw/") == ["raw/a.json", "raw/c.json"]


def test_load_json_and_write_json(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        glue_job.s3,
        "get_object",
        lambda **kwargs: {"Body": BytesIO(b'{"hello":"world"}')},
    )
    monkeypatch.setattr(glue_job.s3, "put_object", lambda **kwargs: captured.update(kwargs) or {})

    assert glue_job.load_json("demo-bucket", "raw/item.json") == {"hello": "world"}

    glue_job.write_json("demo-bucket", "processed/output.json", {"ok": True})
    assert captured["Bucket"] == "demo-bucket"
    assert captured["Key"] == "processed/output.json"
    assert captured["ContentType"] == "application/json"
    assert json.loads(captured["Body"].decode("utf-8")) == {"ok": True}


def test_list_local_files_missing_directory_returns_empty(tmp_path):
    assert glue_job.list_local_files(str(tmp_path / "missing")) == []


def test_load_local_json_skips_invalid_lines(tmp_path):
    input_file = tmp_path / "input.json"
    input_file.write_text(
        '{"id": 1}\n'
        'not-json\n'
        '{"id": 2}\n',
        encoding="utf-8",
    )

    assert glue_job.load_local_json(str(input_file)) == [{"id": 1}, {"id": 2}]


def test_write_local_json_creates_file(tmp_path):
    output_file = tmp_path / "nested" / "result.json"

    glue_job.write_local_json(str(output_file), {"status": "ok"})

    assert json.loads(output_file.read_text(encoding="utf-8")) == {"status": "ok"}


def test_run_job_returns_empty_result_when_no_input(tmp_path):
    input_dir = tmp_path / "missing-input"
    output_dir = tmp_path / "output"

    result = run_job(
        input_prefix=str(input_dir) + "/",
        output_prefix=str(output_dir) + "/",
        local_fs=True,
    )

    assert result["status"] == "success"
    assert result["output_path"] is None
    assert result["metrics"]["input_records"] == 0


def test_run_job_counts_invalid_records(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    input_file = input_dir / "part-0000.json"
    input_file.write_text(
        '{"occurred_at":"2026-06-24T15:30:00Z","event_type":"product_viewed","product":{"product_id":"sku-1"},"customer":{"customer_id":"cust-7"}}\n'
        '{"occurred_at":"2026-06-24T15:31:00Z","event_type":"purchase","product":{"product_id":"sku-2"}}\n',
        encoding="utf-8",
    )

    result = run_job(
        input_prefix=str(input_dir) + "/",
        output_prefix=str(output_dir) + "/",
        local_fs=True,
    )

    metrics = result["metrics"]
    assert metrics["input_records"] == 2
    assert metrics["invalid_records"] == 1
    assert metrics["valid_records"] == 1
    assert metrics["output_records"] == 1


def test_run_job_uses_s3_branch(monkeypatch):
    captured = {}
    monkeypatch.setattr(glue_job, "list_s3_files", lambda bucket, prefix: ["raw/item.json"])
    monkeypatch.setattr(
        glue_job,
        "load_json",
        lambda bucket, key: {
            "occurred_at": "2026-06-24T15:30:00Z",
            "event_type": "product_viewed",
            "product": {"product_id": "sku-1", "name": "Keyboard", "price": 89.99},
            "customer": {"customer_id": "cust-7"},
        },
    )
    monkeypatch.setattr(
        glue_job,
        "write_json",
        lambda bucket, key, data: captured.update({"bucket": bucket, "key": key, "data": data}),
    )

    result = run_job(
        bucket="demo-bucket",
        input_prefix="raw/",
        output_prefix="processed/",
        local_fs=False,
    )

    assert result["status"] == "success"
    assert result["output_path"] == "s3://demo-bucket/processed/processed_output.json"
    assert captured["bucket"] == "demo-bucket"
    assert captured["key"] == "processed/processed_output.json"
    assert captured["data"]["output_count"] == 1


def test_run_job_processes_valid_records(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    input_file = input_dir / "part-0000.json"
    input_file.write_text(
        '{"occurred_at":"2026-06-24T15:30:00+00:00","event_type":"product_viewed","product":{"product_id":"sku-1","name":"Keyboard","price":89.99},"customer":{"customer_id":"cust-7","segment":"premium"}}\n'
        '{"occurred_at":"2026-06-24T15:31:00+00:00","event_type":"purchase","product":{"product_id":"sku-2","name":"Mouse","price":29.99},"customer":{"customer_id":"cust-8"}}\n',
        encoding="utf-8",
    )

    result = run_job(
        input_prefix=str(input_dir) + "/",
        output_prefix=str(output_dir) + "/",
        local_fs=True,
    )

    assert result["status"] == "success"
    metrics = result["metrics"]
    assert metrics["input_records"] == 2
    assert metrics["valid_records"] == 2
    assert metrics["output_records"] == 2
    assert metrics["quality_pct"] == 100.0

    output_files = list(output_dir.rglob("*.json"))
    assert len(output_files) >= 1


def test_run_job_deduplicates_records(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    input_file = input_dir / "part-0000.json"
    # Write same record twice
    input_file.write_text(
        '{"occurred_at":"2026-06-24T15:30:00Z","event_type":"product_viewed","product":{"product_id":"sku-1"},"customer":{"customer_id":"cust-7"}}\n'
        '{"occurred_at":"2026-06-24T15:30:00Z","event_type":"product_viewed","product":{"product_id":"sku-1"},"customer":{"customer_id":"cust-7"}}\n',
        encoding="utf-8",
    )

    result = run_job(
        input_prefix=str(input_dir) + "/",
        output_prefix=str(output_dir) + "/",
        local_fs=True,
    )

    metrics = result["metrics"]
    assert metrics["input_records"] == 2
    assert metrics["duplicate_records"] == 1
    assert metrics["output_records"] == 1
