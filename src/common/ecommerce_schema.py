"""E-commerce event schema.

Stdlib-only (no third-party imports) so the scheduled producer Lambda
(`src/lambdas/ecommerce_producer/handler.py`) can import it without dragging extra
dependencies into its deployment package.

Centralizing `normalize_record` here keeps the record schema sent to SQS in
one place, so the downstream stream-processor Lambda always sees a single shape.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

SCHEMA_VERSION = "2.0"


def _to_utc_iso(time_str: Optional[str]) -> str:
    """Parse an event timestamp into an ISO-8601 string."""
    if time_str:
        try:
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def normalize_record(product: Dict[str, Any], event: Dict[str, Any], channel: str) -> Dict[str, Any]:
    """Map an e-commerce event to our stable event schema."""
    occurred_at = _to_utc_iso(event.get("occurred_at"))
    product_id = product.get("product_id") or product.get("sku") or "unknown"
    event_type = event.get("event_type") or "unknown"

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{event_type}-{product_id}-{event.get('occurred_at') or 'unknown'}",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "occurred_at": occurred_at,
        "channel": channel,
        "event_type": event_type,
        "product": {
            "product_id": product_id,
            "sku": product.get("sku"),
            "name": product.get("name"),
            "category": product.get("category"),
            "price": product.get("price"),
        },
        "customer": {
            "customer_id": event.get("customer_id"),
            "segment": event.get("segment"),
        },
        "order": {
            "amount": event.get("amount"),
            "currency": event.get("currency"),
        },
    }
