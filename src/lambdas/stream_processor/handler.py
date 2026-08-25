"""Landing-zone Lambda — sources 1 and 2 -> ``bronze/events`` + ``quarantine/events``.

Three invocation modes, auto-detected from the event shape:

1. **SQS** event source mapping — ``Records[]`` with a ``body``. The normal one.
2. **Step Functions** — ``messages[]``
3. **Direct invoke** — a single event object, for a manual replay

Partner *files* are not this function's job: they are a batch that can outgrow a
15-minute function, so ``glue_landing_ingest`` reads them straight from
``landing/partners/``.

Every record is run through the shared quality rules
(:mod:`common.quality`). Records that pass land in
``bronze/events/dt=YYYY-MM-DD/hour=HH/`` as NDJSON, one object per partition —
keyed on *event* time, so a late or replayed message stays in the hour it
belongs to. Records that fail land in ``quarantine/events/dt=…/hour=…/`` **with
their failing rule names attached**, so a bad batch can be diagnosed and
replayed instead of vanishing into a log line. Run counters go to CloudWatch.

Zone paths come from :mod:`common.lakehouse`, so the pre-medallion
``RAW_PREFIX`` / ``REJECTED_PREFIX`` keys still win when a config sets them.

Configure the SQS event source mapping with
``FunctionResponseTypes = ["ReportBatchItemFailures"]``: on an S3 write failure
the handler returns the affected ``messageId``s so only those are retried.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3

from common import lakehouse, quality
from common.metrics import MetricsEmitter

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
    """Unwrap an SQS ``body`` (JSON string) or pass a plain object through."""
    raw = record["body"] if "body" in record else record

    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            raise InvalidRecordError("empty payload")
        return json.loads(raw)

    return raw


def _validate(event: Dict[str, Any]) -> None:
    """Cheap structural gate. The full rule set runs in :mod:`common.quality`."""
    missing = [k for k in REQUIRED_KEYS if not event.get(k)]
    if missing:
        raise InvalidRecordError(f"missing keys: {missing}")

    if not event["product"].get("product_id"):
        raise InvalidRecordError("product.product_id is null")


# ─────────────────────────────────────────────
# INPUT NORMALIZATION
# ─────────────────────────────────────────────

def detect_source(event: Dict[str, Any]) -> str:
    """Identify the invocation mode from the event shape.

    An empty ``Records`` list is still an SQS invocation. Treating it as a
    direct invoke turns the envelope itself into a record, which then fails
    validation and lands in the quarantine zone — junk written by a batch that
    contained nothing at all. The ``messages`` branch below already accepted an
    empty list; this one has to as well.
    """
    if isinstance((event or {}).get("Records"), list):
        return "sqs"
    if isinstance((event or {}).get("messages"), list):
        return "stepfunctions"
    return "direct"


def collect_records(event: Dict[str, Any], source: str) -> List[Tuple[Optional[str], Dict[str, Any]]]:
    """Flatten any invocation mode into ``(item_id, raw_record)`` pairs.

    ``item_id`` is the SQS ``messageId`` when there is one — it is what
    ``batchItemFailures`` must reference for a partial-batch retry.
    """
    if source == "sqs":
        raw_records = event.get("Records", [])
    elif source == "stepfunctions":
        raw_records = event.get("messages", [])
    else:
        raw_records = [event]

    return [((r.get("messageId") or r.get("id")) if isinstance(r, dict) else None, r) for r in raw_records]


# ─────────────────────────────────────────────
# PARTITIONING
# ─────────────────────────────────────────────

def _partition_for(event: Dict[str, Any]) -> Tuple[str, str]:
    """Derive the ``(date, hour)`` partition from ``occurred_at``.

    Partitioning on *event* time (not arrival time) is what keeps a late or
    replayed message in the hour it actually belongs to.
    """
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
        "source_object": meta.get("source_object"),
    }
    return event


def _build_key(prefix: str, date_str: Optional[str] = None, hour: Optional[str] = None) -> str:
    """Build a Hive-style partitioned S3 key.

    With partitions: ``bronze/events/dt=2026-06-24/hour=15/20260624T151234-a1b2c3d4.json``
    Without:         ``bronze/events/20260624T151234-a1b2c3d4.json``
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    prefix = prefix.rstrip("/") + "/"
    partition = f"dt={date_str}/hour={hour}/" if date_str and hour else ""
    return f"{prefix}{partition}{ts}-{uid}.json"


# ─────────────────────────────────────────────
# S3 WRITE
# ─────────────────────────────────────────────

def _flush(bucket: str, key: str, events: List[Dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(e, separators=(",", ":"), default=str) for e in events) + "\n"

    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    LOGGER.info("Wrote %d events to s3://%s/%s", len(events), bucket, key)


def _write_rejected(bucket: str, prefix: str, rejected: List[Dict[str, Any]]) -> int:
    """Persist rejected records with their reasons. Never raises.

    A failure to archive a bad record must not turn into a retry of the whole
    batch — the good records already landed.
    """
    if not rejected or not prefix:
        return 0

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for item in rejected:
        groups[_partition_for(item.get("record") or {})].append(item)

    written = 0
    for (date_str, hour), items in groups.items():
        try:
            _flush(bucket, _build_key(prefix, date_str, hour), items)
            written += len(items)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to archive %d rejected records: %s", len(items), exc)
    return written


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])
    log_io(config)

    bucket = config["OUTPUT_BUCKET"]
    # bronze/events and quarantine/events — RAW_PREFIX / REJECTED_PREFIX still
    # win when an older config sets them.
    raw_prefix = lakehouse.dataset_prefix(config, "bronze/events")
    rejected_prefix = lakehouse.dataset_prefix(config, "quarantine/events")
    metrics = MetricsEmitter.from_config(config, stage="stream_processor")

    source = detect_source(event)
    items = collect_records(event, source)
    LOGGER.info("Processing %d items (source=%s)", len(items), source)

    decoded: List[Tuple[Optional[str], Dict[str, Any]]] = []
    rejected: List[Dict[str, Any]] = []

    for item_id, raw in items:
        try:
            record = _decode_record(raw)
            if not isinstance(record, dict):
                raise InvalidRecordError("payload is not an object")
        except Exception as exc:  # noqa: BLE001 - undecodable payloads never block the queue
            LOGGER.warning("Undecodable record %s: %s", item_id, exc)
            rejected.append({
                "rejected_at": datetime.now(timezone.utc).isoformat(),
                "reasons": ["undecodable"],
                "warnings": [],
                "detail": str(exc),
                "record": {"raw": str(raw)[:2000], "message_id": item_id},
            })
            continue

        decoded.append((item_id, record))

    # ── quality-gate, dedupe, then group by event-time partition ──
    # Same rule set the Glue job reports on, so "rejected here" and "flagged
    # there" can never mean two different things.
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    seen_keys: set = set()
    duplicates = 0

    for item_id, record in decoded:
        outcome = quality.check_record(record)
        if outcome["errors"]:
            rejected.append({
                "rejected_at": datetime.now(timezone.utc).isoformat(),
                "reasons": outcome["errors"],
                "warnings": outcome["warnings"],
                "record": record,
                "message_id": item_id,
            })
            continue

        # Idempotency: SQS is at-least-once, so the same event can arrive twice
        # in one batch after a visibility-timeout expiry.
        dedup_key = record.get("idempotency_key")
        if dedup_key and dedup_key in seen_keys:
            duplicates += 1
            continue
        if dedup_key:
            seen_keys.add(dedup_key)

        enriched = _enrich(record, {"source": source, "message_id": item_id})
        groups[_partition_for(enriched)].append(enriched)

    # ── write the bronze zone, one object per partition ──
    batch_item_failures: List[Dict[str, str]] = []
    written = 0
    objects = 0

    for (date_str, hour), events in groups.items():
        key = _build_key(raw_prefix, date_str, hour)
        try:
            _flush(bucket, key, events)
            written += len(events)
            objects += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to write s3://%s/%s: %s", bucket, key, exc)
            for event_obj in events:
                msg_id = (event_obj.get("_meta") or {}).get("message_id")
                if msg_id:
                    batch_item_failures.append({"itemIdentifier": msg_id})

    archived = _write_rejected(bucket, rejected_prefix, rejected)

    report = quality.evaluate([record for _, record in decoded], duplicates=duplicates)
    summary = {
        "source": source,
        "received": len(items),
        "written": written,
        "objects": objects,
        "rejected": len(rejected),
        "rejected_archived": archived,
        "duplicates": duplicates,
        "partitions": [f"dt={d}/hour={h}" for d, h in groups],
        "retries": len(batch_item_failures),
        "quality_verdict": report["verdict"],
        "quality_pass_pct": report["totals"]["pass_pct"],
    }

    metrics.count("RecordsReceived", len(items))
    metrics.count("RecordsWritten", written)
    metrics.count("RecordsRejected", len(rejected))
    metrics.count("RecordsDuplicate", duplicates)
    metrics.count("ObjectsWritten", objects)
    metrics.count("RetriesRequested", len(batch_item_failures))
    metrics.gauge("QualityPassPct", report["totals"]["pass_pct"], unit="Percent")
    metrics.flush()

    LOGGER.info("Stream processor run: %s", json.dumps(summary, default=str))

    # The SQS event source mapping requires exactly this shape.
    return {"batchItemFailures": batch_item_failures}


handler = lambda_handler


# ─────────────────────────────────────────────
# INPUT / OUTPUT CONTRACT
# ─────────────────────────────────────────────

def describe_io(config: dict) -> dict:
    """Where this function reads and writes, and in what format.

    It shares ``bronze/events/`` with the landing Glue job, so both must write
    the same NDJSON shape — pinned by tests/test_format_contracts.py.
    """
    return {
        "job": "stream_processor",
        "reads": [
            {"what": "queued events", "format": "JSON message body",
             "where": config.get("QUEUE_URL") or "<QUEUE_URL>"},
        ],
        "writes": [
            {"what": "bronze events", "format": "NDJSON, one event per line",
             "where": lakehouse.dataset_path(config, "bronze/events")},
            {"what": "rejected records", "format": "NDJSON + failing rules",
             "where": lakehouse.dataset_path(config, "quarantine/events")},
        ],
    }


def log_io(config: dict) -> None:
    """Print the contract at start-up, before any work."""
    # Never raises. This is diagnostics printed before any work — a job killed
    # by its own logging is the worst possible trade.
    try:
        contract = describe_io(config)
        LOGGER.info("--- %s : input/output contract ---", contract["job"])
        for side in ("reads", "writes"):
            for item in contract[side]:
                LOGGER.info(
                    "  %-6s %-26s %-12s %s",
                    side.upper(), item.get("what"), f"[{item.get('format')}]", item.get("where"),
                )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not stop the job
        LOGGER.warning("Could not describe the input/output contract: %s", exc)
