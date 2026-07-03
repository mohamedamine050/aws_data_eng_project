import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))

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
