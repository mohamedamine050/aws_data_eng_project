"""Scheduled e-commerce producer Lambda (EventBridge -> Lambda -> SQS).

A *single-batch* producer, triggered on a schedule by Amazon EventBridge
(e.g. once per hour):

    EventBridge (rate/cron) ──> [this Lambda] ──> Amazon SQS queue

On each invocation it generates e-commerce events for the configured products
and sends one message per product to an SQS queue. There is no loop: EventBridge
controls the cadence, the function does one batch and returns.

For a fast, dependency-light cold start this handler uses only the standard
library and `boto3` for SQS. The record schema lives in `common.ecommerce_schema`
so the event shape is defined in one place.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common.ecommerce_schema import normalize_record

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_SQS = boto3.client("sqs")
SQS_BATCH_SIZE = 10


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


def _slugify(name: str, category: Optional[str]) -> str:
    base = name.strip().lower().replace(" ", "-")
    return f"{base}-{category.strip().lower()}" if category else base


def resolve_products(config_products: Optional[List[Dict[str, Any]]], api_url: Optional[str] = None) -> List[Dict[str, Any]]:
    if config_products:
        source = config_products
    elif api_url:
        try:
            with urllib.request.urlopen(api_url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            source = []
            for entry in payload:
                source.append({
                    "product_id": str(entry.get("id") or entry.get("product_id") or entry.get("sku") or ""),
                    "sku": entry.get("sku") or entry.get("id"),
                    "name": entry.get("title") or entry.get("name"),
                    "category": entry.get("category"),
                    "price": entry.get("price"),
                })
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.warning("Failed to load products from %s: %s", api_url, exc)
            source = []
    else:
        source = []

    products: List[Dict[str, Any]] = []
    for entry in source:
        try:
            products.append({
                "product_id": entry["product_id"],
                "sku": entry.get("sku"),
                "name": entry["name"],
                "category": entry.get("category"),
                "price": float(entry["price"]),
            })
        except (KeyError, TypeError, ValueError) as exc:
            LOGGER.warning("Skipping malformed product %r: %s", entry, exc)
    return products


def fetch_event(product: Dict[str, Any], channel: str, timeout: int, config: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    del timeout
    occurred_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    config = config or {}
    event = {
        "event_type": config.get("EVENT_TYPE", "product_viewed"),
        "occurred_at": occurred_at,
        "customer_id": config.get("CUSTOMER_ID", "cust-demo"),
        "segment": config.get("CUSTOMER_SEGMENT", "new"),
        "currency": config.get("CURRENCY", "EUR"),
        "amount": product.get("price"),
    }
    return normalize_record(product, event, channel)


def send_messages(queue_url: str, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    failed = 0
    for start in range(0, len(records), SQS_BATCH_SIZE):
        chunk = records[start:start + SQS_BATCH_SIZE]
        entries = [{"Id": str(i), "MessageBody": json.dumps(rec)} for i, rec in enumerate(chunk)]
        try:
            resp = _SQS.send_message_batch(QueueUrl=queue_url, Entries=entries)
        except (BotoCoreError, ClientError) as exc:
            LOGGER.error("SendMessageBatch failed entirely: %s", exc)
            failed += len(chunk)
            continue

        for fail in resp.get("Failed", []):
            LOGGER.warning("Message failed: %s - %s", fail.get("Code"), fail.get("Message"))
        failed += len(resp.get("Failed", []))
    return failed


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])

    queue_url = config.get("QUEUE_URL")
    if not queue_url:
        raise RuntimeError("QUEUE_URL is missing from the config.")

    channel = config.get("CHANNEL", "web")
    api_url = config.get("ECOMMERCE_API_URL")
    products = resolve_products(config.get("PRODUCTS"), api_url=api_url)

    records: List[Dict[str, Any]] = []
    for product in products:
        rec = fetch_event(product, channel, 10, config=config)
        if rec is not None:
            records.append(rec)

    failed = send_messages(queue_url, records)
    sent = len(records) - failed
    result = {
        "products_requested": len(products),
        "generated": len(records),
        "sent": sent,
        "failed": failed,
    }
    LOGGER.info("Producer run: %s", result)
    return result
