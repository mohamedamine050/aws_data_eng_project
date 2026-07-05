"""SQS / Step Functions -> S3 raw landing Lambda.

Supports 3 invocation modes:
  1. SQS event source mapping (Records[])
  2. Step Functions payload (messages[])
  3. Direct invocation (single event)

Writes validated events into S3 raw zone as NDJSON.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

S3 = boto3.client("s3")

REQUIRED_KEYS = ("occurred_at", "event_type", "product", "customer")


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

def get_args(event: Dict[str, Any]) -> Dict[str, str]:
    config_path = (event or {}).get("CONFIG_PATH") or os.getenv("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH not provided.")
    return {"CONFIG_PATH": config_path}


def load_config(path: str) -> Dict[str, Any]:
    LOGGER.info("Loading config from %s", path)

    if path.startswith("s3://"):
        parsed = urlparse(path)
        obj = S3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read().decode("utf-8"))

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────

class InvalidRecordError(ValueError):
    pass


def _decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Supports:
    - SQS: record["body"]
    - Step Functions: record already JSON
    """
    if "body" in record:
        raw = record["body"]
    else:
        raw = record

    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            raise InvalidRecordError("empty payload")
        return json.loads(raw)

    return raw


def _validate(event: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_KEYS if not event.get(k)]
    if missing:
        raise InvalidRecordError(f"missing keys: {missing}")

    if not event["product"].get("product_id"):
        raise InvalidRecordError("product.product_id is null")


# ─────────────────────────────────────────────
# PARTITIONING
# ─────────────────────────────────────────────

def _partition_for(event: Dict[str, Any]) -> Tuple[str, str]:
    ts = event.get("occurred_at")

    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, date):
        dt = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            dt = None
    else:
        dt = None

    if not dt:
        dt = datetime.now(timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def _enrich(event: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    event["_meta"] = {
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "source": meta.get("source"),
        "message_id": meta.get("message_id"),
    }
    return event


def _build_key(date_str: str, hour: str, prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    prefix = prefix if prefix.endswith("/") else prefix + "/"
    return f"{prefix}dt={date_str}/hour={hour}/{ts}-{uid}.json"


# ─────────────────────────────────────────────
# S3 WRITE
# ─────────────────────────────────────────────

def _flush(bucket: str, key: str, events: List[Dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(e, separators=(",", ":")) for e in events) + "\n"

    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    LOGGER.info("Wrote %d events to s3://%s/%s", len(events), bucket, key)


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])

    bucket = config["OUTPUT_BUCKET"]
    prefix = config.get("RAW_PREFIX", "raw/")

    # ── UNIFIED INPUT NORMALIZATION ──

    if "Records" in event:              # SQS mode
        raw_records = event["Records"]
        source = "sqs"

    elif "messages" in event:          # Step Functions batch mode
        raw_records = event["messages"]
        source = "stepfunctions"

    else:                               # direct invoke
        raw_records = [event]
        source = "direct"

    LOGGER.info("Processing %d events (source=%s)", len(raw_records), source)

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    batch_item_failures: List[Dict[str, str]] = []

    for record in raw_records:
        item_id = record.get("messageId") or record.get("id")
        try:
            decoded = _decode_record(record)
            _validate(decoded)

            enriched = _enrich(decoded, {
                "source": source,
                "message_id": item_id,
            })

            date_str, hour = _partition_for(enriched)
            groups[(date_str, hour)].append(enriched)

        except Exception as exc:
            LOGGER.warning("Skipping invalid record: %s", exc)
            # Invalid records are silently dropped, not reported as failures

    # ── WRITE TO S3 ──

    for (date_str, hour), events in groups.items():
        key = _build_key(date_str, hour, prefix)
        try:
            _flush(bucket, key, events)
        except Exception as exc:
            LOGGER.error("Failed to write to S3: %s", exc)
            # Mark all events in this group as failed
            for event_obj in events:
                msg_id = event_obj.get("_meta", {}).get("message_id")
                if msg_id:
                    batch_item_failures.append({"itemIdentifier": msg_id})

    return {"batchItemFailures": batch_item_failures}


handler = lambda_handler