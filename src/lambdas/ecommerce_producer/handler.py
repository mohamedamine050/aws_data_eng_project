"""E-commerce producer Lambda (Step Functions -> Lambda -> SQS).

A *single-batch* producer, invoked as the first step of the pipeline:

    Step Functions ──> [this Lambda] ──> Amazon SQS queue

There is no loop — the state machine controls the cadence, the function does one batch
and returns.

Two generation modes
--------------------
**Simulation** (``SIMULATION.ENABLED: true``) — the interesting one. Each run
produces N browsing *sessions* through :mod:`common.event_simulator`: views,
carts, checkouts, orders, payment failures, cancellations and refunds, from a
stable customer pool. That is what gives the downstream funnel, session and RFM
tables something real to compute.

**Legacy** (default) — one ``product_viewed`` per catalog product per run, the
original v2 behaviour, kept so existing schedules keep working unchanged.

Inputs come from :mod:`common.sources`, which merges the inline ``PRODUCTS``
list, S3 JSON/CSV catalogs and an external catalog API — a failing source is
skipped, never fatal. Records are quality-gated with :mod:`common.quality`
before they reach the queue, and run counters go to CloudWatch through
:mod:`common.metrics`.

Standard library + ``boto3`` only, so the deployment package needs no layer and
cold starts stay fast. Package this handler together with ``src/common/``.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common import quality, sources
from common.ecommerce_schema import normalize_record
from common.event_simulator import simulate
from common.metrics import MetricsEmitter

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_SQS = boto3.client("sqs")
SQS_BATCH_SIZE = 10
#: SQS rejects a SendMessageBatch payload above 256 KiB; stay under it.
SQS_MAX_BATCH_BYTES = 240_000


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

def get_args(event: Dict[str, Any]) -> Dict[str, str]:
    config_path = (event or {}).get("CONFIG_PATH") or os.getenv("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH not provided (event argument or environment).")
    return {"CONFIG_PATH": config_path}


def load_config(path: str) -> Dict[str, Any]:
    LOGGER.info("Loading config from %s", path)
    if path.startswith("s3://"):
        parsed = urllib.parse.urlparse(path)
        obj = boto3.client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read().decode("utf-8"))
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ─────────────────────────────────────────────
# CATALOG
# ─────────────────────────────────────────────

def resolve_products(
    config_products: Optional[List[Dict[str, Any]]], api_url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Resolve products from an inline list and/or a catalog API.

    Thin wrapper over :func:`common.sources.load_products` so there is exactly
    one place that knows how to read a catalog. Kept for callers (and schedules)
    that only ever used these two sources.
    """
    products, _ = sources.load_products({
        "PRODUCTS": config_products or [],
        "ECOMMERCE_API_URL": api_url,
    })
    return products


# ─────────────────────────────────────────────
# EVENT GENERATION
# ─────────────────────────────────────────────

def fetch_event(
    product: Dict[str, Any], channel: str, timeout: int, config: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """Legacy single-event generator: one event per product, per run."""
    del timeout
    config = config or {}
    occurred_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    event = {
        "event_type": config.get("EVENT_TYPE", "product_viewed"),
        "occurred_at": occurred_at,
        "customer_id": config.get("CUSTOMER_ID", "cust-demo"),
        "segment": config.get("CUSTOMER_SEGMENT", "new"),
        "currency": config.get("CURRENCY", "EUR"),
        "amount": product.get("price"),
        "device_type": config.get("DEVICE_TYPE", "unknown"),
        "country": config.get("COUNTRY"),
    }
    return normalize_record(product, event, channel)


def generate_records(
    products: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Produce this run's records using whichever mode the config selects."""
    simulation = config.get("SIMULATION") or {}
    channel = config.get("CHANNEL", "web")

    if simulation.get("ENABLED"):
        merged = {"CURRENCY": config.get("CURRENCY", "EUR"), **simulation}
        records = simulate(products, merged, customers=customers or None)
        LOGGER.info("Simulated %d events over %d sessions", len(records), merged.get("SESSIONS", 25))
        return records

    records = []
    for product in products:
        record = fetch_event(product, channel, 10, config=config)
        if record is not None:
            records.append(record)
    return records


# ─────────────────────────────────────────────
# SQS PUBLISH
# ─────────────────────────────────────────────

def _entry(index: int, record: Dict[str, Any], is_fifo: bool) -> Dict[str, Any]:
    """Build one SendMessageBatch entry.

    Message attributes let a consumer filter without parsing the body, and on a
    FIFO queue the schema's ``idempotency_key`` doubles as the SQS dedup id — so
    a retried producer run cannot enqueue the same business event twice.
    """
    entry: Dict[str, Any] = {
        "Id": str(index),
        "MessageBody": json.dumps(record, separators=(",", ":")),
        "MessageAttributes": {
            "event_type": {"DataType": "String", "StringValue": str(record.get("event_type", "unknown"))},
            "schema_version": {"DataType": "String", "StringValue": str(record.get("schema_version", "3.0"))},
        },
    }
    if is_fifo:
        session = record.get("session") or {}
        entry["MessageGroupId"] = str(session.get("session_id") or record.get("event_type") or "default")
        dedup = record.get("idempotency_key")
        if dedup:
            entry["MessageDeduplicationId"] = str(dedup)
    return entry


def _chunks(records: List[Dict[str, Any]], is_fifo: bool):
    """Yield batches bounded by both the 10-message and 256 KiB SQS limits."""
    batch: List[Dict[str, Any]] = []
    size = 0
    for index, record in enumerate(records):
        entry = _entry(index, record, is_fifo)
        entry_size = len(entry["MessageBody"].encode("utf-8")) + 256
        if batch and (len(batch) >= SQS_BATCH_SIZE or size + entry_size > SQS_MAX_BATCH_BYTES):
            yield batch
            batch, size = [], 0
        entry["Id"] = str(len(batch))
        batch.append(entry)
        size += entry_size
    if batch:
        yield batch


def send_messages(queue_url: str, records: List[Dict[str, Any]]) -> int:
    """Publish records to SQS. Returns the number that failed to send."""
    if not records:
        return 0

    is_fifo = str(queue_url).endswith(".fifo")
    failed = 0

    for entries in _chunks(records, is_fifo):
        try:
            response = _SQS.send_message_batch(QueueUrl=queue_url, Entries=entries)
        except (BotoCoreError, ClientError) as exc:
            LOGGER.error("SendMessageBatch failed entirely: %s", exc)
            failed += len(entries)
            continue

        for failure in response.get("Failed", []):
            LOGGER.warning("Message failed: %s - %s", failure.get("Code"), failure.get("Message"))
        failed += len(response.get("Failed", []))

    return failed


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])

    queue_url = config.get("QUEUE_URL")
    if not queue_url:
        raise RuntimeError("QUEUE_URL is missing from the config.")

    metrics = MetricsEmitter.from_config(config, stage="producer")

    products, product_stats = sources.load_products(config)
    customers, customer_stats = sources.load_customers(config)
    LOGGER.info("Catalog: %s | Customers: %s", product_stats, customer_stats)

    records = generate_records(products, customers, config)

    # Quality-gate before publishing: a malformed record costs an SQS message, a
    # Lambda invocation and a line in the lake if it gets through here.
    accepted, rejected = quality.partition(records)
    for bad in rejected[:10]:
        LOGGER.warning("Dropping invalid record: %s", bad["reasons"])

    failed = send_messages(queue_url, accepted)
    sent = len(accepted) - failed

    by_event_type: Dict[str, int] = {}
    for record in accepted:
        key = str(record.get("event_type", "unknown"))
        by_event_type[key] = by_event_type.get(key, 0) + 1

    result = {
        # v2 keys, unchanged for existing dashboards
        "products_requested": len(products),
        "generated": len(records),
        "sent": sent,
        "failed": failed,
        # v3 additions
        "mode": "simulation" if (config.get("SIMULATION") or {}).get("ENABLED") else "legacy",
        "customers_resolved": len(customers),
        "rejected": len(rejected),
        "by_event_type": by_event_type,
        "catalog_stats": product_stats,
        "customer_stats": customer_stats,
    }

    metrics.count("ProductsResolved", len(products))
    metrics.count("CustomersResolved", len(customers))
    metrics.count("EventsGenerated", len(records))
    metrics.count("EventsRejected", len(rejected))
    metrics.count("MessagesSent", sent)
    metrics.count("MessagesFailed", failed)
    for event_type, count in by_event_type.items():
        metrics.count("EventsByType", count, EventType=event_type)
    metrics.flush()

    LOGGER.info("Producer run: %s", json.dumps(result, default=str))
    return result
