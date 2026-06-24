"""Kinesis -> S3 raw landing Lambda.

Triggered by the Kinesis Data Stream (event source mapping). For each batch of
records it:

  1. Decodes and validates each weather event.
  2. Lightly enriches it (adds processing metadata + derived partition keys).
  3. Buffers valid events and writes them as newline-delimited JSON (NDJSON)
     to the raw zone of the S3 data lake, partitioned by event date/hour:

         s3://<bucket>/raw/dt=YYYY-MM-DD/hour=HH/<stream>-<shard>-<ts>.json

This is the *Serverless Stream Processing* stage:

    Kinesis ──> [this Lambda] ──> S3 (raw)

Configuration
-------------
    Driven by a JSON config file, pointed to by the CONFIG_PATH environment
    variable (local path or s3://bucket/key). Config keys:

        OUTPUT_BUCKET      (required)  Target S3 bucket.
        RAW_PREFIX         (optional)  Key prefix for raw data. Default "raw/".

    Env vars: CONFIG_PATH (required), LOG_LEVEL (optional).

The function uses Kinesis partial-batch-response: only the records that fail
are reported back so successfully processed records are not redelivered.
Configure the event source mapping with
``FunctionResponseTypes = ["ReportBatchItemFailures"]``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# ─────────────────────────────────────────────
# CONFIG / CONSTANTS
# ─────────────────────────────────────────────

S3 = boto3.client("s3")

# Required top-level keys for an event to be considered valid.
REQUIRED_KEYS = ("observed_at", "location", "measurement")


def get_args(event: Dict[str, Any]) -> Dict[str, str]:
    """Resolve runtime args. CONFIG_PATH is passed as an argument via the
    invocation event, with a fallback to the CONFIG_PATH env var. (A Kinesis
    event carries records, not config, so this Lambda typically uses the env
    fallback.) It points to a JSON config (local file or s3://...)."""
    config_path = (event or {}).get("CONFIG_PATH") or os.environ.get("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH not provided (event argument or environment).")
    return {"CONFIG_PATH": config_path}


def load_config(path: str) -> Dict[str, Any]:
    """Load the JSON config from S3 (s3://bucket/key) or a local file."""
    LOGGER.info("Loading config from %s", path)
    if path.startswith("s3://"):
        parsed = urlparse(path)
        obj = S3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read().decode("utf-8"))
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ─────────────────────────────────────────────
# DECODE & VALIDATE
# ─────────────────────────────────────────────

class InvalidRecordError(ValueError):
    """Raised when a decoded record fails validation."""


def _decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Base64-decode and JSON-parse a single Kinesis record's data."""
    raw = base64.b64decode(record["kinesis"]["data"])
    text = raw.decode("utf-8").strip()
    if not text:
        raise InvalidRecordError("empty payload")
    return json.loads(text)


def _validate(event: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_KEYS if k not in event or event[k] in (None, {}, "")]
    if missing:
        raise InvalidRecordError(f"missing/empty keys: {missing}")
    if event["measurement"].get("temperature") is None:
        raise InvalidRecordError("measurement.temperature is null")


# ─────────────────────────────────────────────
# PARTITIONING & KEYS
# ─────────────────────────────────────────────

def _partition_for(event: Dict[str, Any]) -> Tuple[str, str]:
    """Return (date_str, hour_str) partition values from the observed timestamp."""
    ts = event.get("observed_at")
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except (AttributeError, ValueError):
        dt = datetime.now(timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def _enrich(event: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """Attach processing metadata useful for lineage/debugging."""
    event["_meta"] = {
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "kinesis_sequence_number": record["kinesis"].get("sequenceNumber"),
        "kinesis_partition_key": record["kinesis"].get("partitionKey"),
        "event_source_arn": record.get("eventSourceARN"),
    }
    return event


def _build_key(partition_date: str, hour: str, shard_token: str, raw_prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique = uuid.uuid4().hex[:8]
    prefix = raw_prefix if raw_prefix.endswith("/") else raw_prefix + "/"
    return f"{prefix}dt={partition_date}/hour={hour}/{shard_token}-{ts}-{unique}.json"


def _shard_token(record: Dict[str, Any]) -> str:
    """Derive a short, filename-safe token from the source shard, for object names."""
    eid = record.get("eventID", "")  # format: "shardId-000000000000:49590..."
    shard = eid.split(":", 1)[0] if ":" in eid else "shard"
    return shard.replace("shardId-", "s")


# ─────────────────────────────────────────────
# WRITE (S3 raw)
# ─────────────────────────────────────────────

def _flush_group(bucket: str, key: str, events: List[Dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(e, separators=(",", ":")) for e in events) + "\n"
    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    LOGGER.info("Wrote %d records to s3://%s/%s", len(events), bucket, key)


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])

    bucket = config.get("OUTPUT_BUCKET")
    if not bucket:
        raise RuntimeError("OUTPUT_BUCKET is missing from the config.")
    raw_prefix = config.get("RAW_PREFIX", "raw/")

    records = event.get("Records", [])
    LOGGER.info("Received %d Kinesis records", len(records))

    # Group valid events by (partition, shard) so each S3 object holds one
    # partition's worth of data from one shard -> clean date/hour partitioning.
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    failures: List[Dict[str, str]] = []
    last_seq_by_group: Dict[Tuple[str, str, str], str] = {}

    for record in records:
        seq = record["kinesis"].get("sequenceNumber", "unknown")
        try:
            decoded = _decode_record(record)
            _validate(decoded)
            enriched = _enrich(decoded, record)
            pdate, phour = _partition_for(enriched)
            key = (pdate, phour, _shard_token(record))
            groups[key].append(enriched)
            last_seq_by_group[key] = seq
        except (InvalidRecordError, json.JSONDecodeError, KeyError, UnicodeDecodeError) as exc:
            # Bad data: log and skip. We do NOT add it to batchItemFailures,
            # otherwise a poison record would block the shard forever.
            LOGGER.warning("Dropping invalid record %s: %s", seq, exc)

    # Write each group to S3. If a write fails, mark that group's records so
    # Kinesis retries them (via the last sequence number in the group).
    for key, events in groups.items():
        pdate, phour, shard_token = key
        s3_key = _build_key(pdate, phour, shard_token, raw_prefix)
        try:
            _flush_group(bucket, s3_key, events)
        except Exception as exc:  # noqa: BLE001 - report to Kinesis for retry
            LOGGER.error("Failed writing group %s: %s", key, exc)
            failures.append({"itemIdentifier": last_seq_by_group[key]})

    return {"batchItemFailures": failures}
