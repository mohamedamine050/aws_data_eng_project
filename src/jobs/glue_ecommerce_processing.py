"""E-commerce data processing job with quality checks, cleaning, and transformations.

Implements data engineering best practices:
  - Data validation & quality checks
  - Cleaning & normalization
  - Deduplication
  - Business transformations
  - Quality metrics & logging
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import boto3

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ─────────────────────────────────────────────
# CONFIG & QUALITY METRICS
# ─────────────────────────────────────────────

class DataQualityMetrics:
    """Track data quality during processing."""

    def __init__(self):
        self.input_records = 0
        self.valid_records = 0
        self.invalid_records = 0
        self.duplicate_records = 0
        self.output_records = 0
        self.errors: Dict[str, int] = {}

    def record_error(self, error_type: str) -> None:
        self.errors[error_type] = self.errors.get(error_type, 0) + 1

    def report(self) -> Dict[str, Any]:
        return {
            "input_records": self.input_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_records": self.duplicate_records,
            "output_records": self.output_records,
            "error_breakdown": self.errors,
            "quality_pct": round(100 * self.output_records / max(self.input_records, 1), 2),
        }


# ─────────────────────────────────────────────
# VALIDATION & CLEANING
# ─────────────────────────────────────────────

REQUIRED_FIELDS = ("occurred_at", "event_type", "product", "customer")


def _validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate record structure and required fields."""
    if not isinstance(record, dict):
        return False, "not_a_dict"

    missing = [k for k in REQUIRED_FIELDS if k not in record]
    if missing:
        return False, f"missing_fields:{','.join(missing)}"

    product = record.get("product") or {}
    if not product.get("product_id"):
        return False, "missing_product_id"

    customer = record.get("customer") or {}
    if not customer.get("customer_id"):
        return False, "missing_customer_id"

    return True, None


def _clean_string(value: Optional[str], max_len: int = 1000) -> Optional[str]:
    """Clean and normalize a string field."""
    if not value:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned[:max_len]


def _clean_numeric(value: Any) -> Optional[float]:
    """Clean and normalize a numeric field."""
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_timestamp(ts: Any) -> datetime:
    """Parse timestamp robustly, fallback to now()."""
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.strip().replace("Z", "+00:00"))
        except ValueError:
            dt = None
    else:
        dt = None

    if dt is None:
        return datetime.now(timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _compute_record_hash(record: Dict[str, Any]) -> str:
    """Compute a deterministic hash for deduplication."""
    key_parts = [
        record.get("event_type", ""),
        record.get("product", {}).get("product_id", ""),
        record.get("customer", {}).get("customer_id", ""),
        record.get("occurred_at", ""),
    ]
    key_str = "|".join(str(p) for p in key_parts)
    return hashlib.md5(key_str.encode()).hexdigest()


# ─────────────────────────────────────────────
# TRANSFORMATIONS
# ─────────────────────────────────────────────

def _enrich_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply business transformations and enrichments."""
    product = raw_record.get("product") or {}
    customer = raw_record.get("customer") or {}
    order = raw_record.get("order") or {}

    dt = _parse_timestamp(raw_record.get("occurred_at"))

    # Build processed record with cleaned fields
    processed = {
        # Timestamp partitioning
        "occurred_at": dt.isoformat(),
        "partition_date": dt.strftime("%Y-%m-%d"),
        "partition_hour": dt.strftime("%H"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        # Core fields (cleaned)
        "event_type": _clean_string(raw_record.get("event_type")),
        "channel": _clean_string(raw_record.get("channel")),
        # Product dimension
        "product_id": _clean_string(product.get("product_id")),
        "product_name": _clean_string(product.get("name")),
        "product_category": _clean_string(product.get("category")),
        "product_price": _clean_numeric(product.get("price")),
        "product_sku": _clean_string(product.get("sku")),
        # Customer dimension
        "customer_id": _clean_string(customer.get("customer_id")),
        "customer_segment": _clean_string(customer.get("segment")),
        # Order facts
        "order_amount": _clean_numeric(order.get("amount")),
        "order_currency": _clean_string(order.get("currency")),
        # Metadata
        "event_id": _clean_string(raw_record.get("event_id")),
        "schema_version": _clean_string(raw_record.get("schema_version")),
    }

    # Preserve enriched metadata if present
    if "_meta" in raw_record:
        processed["_meta"] = raw_record["_meta"]

    # Computed fields
    processed["record_hash"] = _compute_record_hash(raw_record)

    # Price category (business logic)
    price = processed.get("product_price")
    if price is not None:
        if price < 50:
            processed["price_category"] = "budget"
        elif price < 200:
            processed["price_category"] = "mid"
        else:
            processed["price_category"] = "premium"

    return processed


def get_args(event: Dict[str, Any]) -> Dict[str, str]:
    config_path = (event or {}).get("CONFIG_PATH") or os.getenv("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH not provided (event argument or environment).")
    return {"CONFIG_PATH": config_path}


def load_config(path: str) -> Dict[str, Any]:
    if path.startswith("s3://"):
        parsed = urlparse(path)
        obj = boto3.client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read().decode("utf-8"))
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def run_job(input_prefix: str, output_prefix: str, local_fs: bool = False) -> Dict[str, Any]:
    """Process raw e-commerce data with validation, cleaning, and enrichment.

    Returns:
        A dictionary with quality metrics and processing summary.
    """
    input_path = Path(input_prefix)
    output_path = Path(output_prefix)

    if not input_path.exists():
        LOGGER.warning("Input path does not exist: %s", input_prefix)
        return {"status": "no_input", "metrics": {}}

    files = sorted(input_path.glob("*.json"))
    if not files:
        LOGGER.warning("No JSON files found in %s", input_prefix)
        return {"status": "no_files", "metrics": {}}

    output_path.mkdir(parents=True, exist_ok=True)
    metrics = DataQualityMetrics()
    seen_hashes: Set[str] = set()

    for input_file in files:
        LOGGER.info("Processing file: %s", input_file.name)
        processed_records: List[Dict[str, Any]] = []

        try:
            with input_file.open("r", encoding="utf-8") as handle:
                for line_num, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    metrics.input_records += 1

                    try:
                        raw_record = json.loads(line)
                    except json.JSONDecodeError as exc:
                        LOGGER.warning("Line %d: JSON parse error - %s", line_num, exc)
                        metrics.invalid_records += 1
                        metrics.record_error("json_parse_error")
                        continue

                    # Validate
                    is_valid, error = _validate_record(raw_record)
                    if not is_valid:
                        LOGGER.debug("Line %d: Validation failed - %s", line_num, error)
                        metrics.invalid_records += 1
                        metrics.record_error(error or "validation_error")
                        continue

                    # Deduplicate
                    record_hash = _compute_record_hash(raw_record)
                    if record_hash in seen_hashes:
                        LOGGER.debug("Line %d: Duplicate record (hash=%s)", line_num, record_hash)
                        metrics.duplicate_records += 1
                        continue

                    seen_hashes.add(record_hash)

                    # Enrich & transform
                    processed = _enrich_record(raw_record)
                    processed_records.append(processed)
                    metrics.valid_records += 1

        except Exception as exc:
            LOGGER.error("Error processing file %s: %s", input_file.name, exc)
            metrics.record_error("file_processing_error")
            continue

        # Write processed records
        if processed_records:
            output_file = output_path / f"{input_file.stem}.processed.json"
            try:
                with output_file.open("w", encoding="utf-8") as handle:
                    for record in processed_records:
                        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
                metrics.output_records += len(processed_records)
                LOGGER.info("Wrote %d records to %s", len(processed_records), output_file.name)
            except Exception as exc:
                LOGGER.error("Error writing output file %s: %s", output_file.name, exc)
                metrics.record_error("write_error")

    report = metrics.report()
    LOGGER.info("Job completed. Quality report: %s", json.dumps(report, indent=2))
    return {"status": "success", "metrics": report}


if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    import sys

    # Récupère les arguments Glue correctement
    glue_args = getResolvedOptions(sys.argv, ["CONFIG_PATH"])

    # Injecte les arguments dans ton parser
    args = get_args(glue_args)

    # Charge config S3/local
    config = load_config(args["CONFIG_PATH"])

    input_prefix = config.get("RAW_PREFIX", "raw/")
    output_prefix = config.get("PROCESSED_PREFIX", "processed/")

    result = run_job(input_prefix, output_prefix)

    print(json.dumps(result, indent=2))
