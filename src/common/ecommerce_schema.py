"""E-commerce event schema (v3) — the single contract of the pipeline.

Stdlib-only (no third-party imports) so the scheduled producer Lambda
(`src/lambdas/ecommerce_producer/handler.py`) can import it without dragging extra
dependencies into its deployment package. The stream processor and the Glue jobs
import the *same* module, so producer, landing zone and warehouse can never drift
apart.

What lives here
---------------
* ``normalize_record`` — maps any upstream product/event payload onto the stable
  record shape written to SQS, then to S3 ``raw/``.
* ``validate_record`` — strict, typed validation (enums, bounds, required
  fields). Returns a list of error codes instead of raising, so callers can route
  a bad record to the ``rejected/`` zone with its reason attached.
* ``idempotency_key`` — a stable hash of the business identity of an event, used
  to deduplicate replays across SQS at-least-once delivery and Glue re-runs.

Version history
---------------
``2.0`` flat product/customer/order blocks.
``3.0`` adds ``session``, ``device``, ``geo``, ``marketing``, quantities,
discounts and gross/net amounts, plus ``idempotency_key``. ``order.amount`` is
retained as an alias of ``order.net_amount`` so v2 consumers keep working.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCHEMA_VERSION = "3.0"

#: Event types the pipeline understands. Anything else is rejected at ingest so
#: a typo upstream never silently pollutes the funnel metrics.
EVENT_TYPES = (
    "product_viewed",
    "product_searched",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_failed",
    "order_placed",
    "order_cancelled",
    "refund_issued",
)

#: Event types that carry money and feed the revenue aggregates.
REVENUE_EVENT_TYPES = ("order_placed",)
NEGATIVE_REVENUE_EVENT_TYPES = ("order_cancelled", "refund_issued")

#: Ordered funnel stages, used by the curated funnel table.
FUNNEL_STAGES = (
    "product_viewed",
    "add_to_cart",
    "checkout_started",
    "order_placed",
)

CHANNELS = ("web", "mobile_app", "mobile_web", "marketplace", "store", "api")
DEVICE_TYPES = ("desktop", "mobile", "tablet", "unknown")
CURRENCIES = ("EUR", "USD", "GBP", "CHF", "CAD")
PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "wallet", "gift_card", "unknown")
CUSTOMER_SEGMENTS = ("new", "returning", "loyal", "vip", "churn_risk", "unknown")

MAX_QUANTITY = 100
MAX_UNIT_PRICE = 100_000.0
MAX_STRING_LEN = 1000

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


# ─────────────────────────────────────────────
# COERCION HELPERS
# ─────────────────────────────────────────────

def _to_utc_iso(time_str: Optional[str]) -> str:
    """Parse an event timestamp into an ISO-8601 UTC string.

    Falls back to *now* when the value is missing or unparseable — the record is
    still worth keeping, and ``validate_record`` flags the drift separately.
    """
    if time_str:
        try:
            dt = datetime.fromisoformat(str(time_str).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def _clean_str(value: Any, max_len: int = MAX_STRING_LEN) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:max_len]


def _to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _round2(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(value + 0.0, 2)


def _category_name(category: Any) -> Optional[str]:
    """Upstream APIs send either ``"Home"`` or ``{"name": "Home"}``."""
    if isinstance(category, dict):
        return _clean_str(category.get("name") or category.get("slug"))
    return _clean_str(category)


# ─────────────────────────────────────────────
# IDENTITY
# ─────────────────────────────────────────────

def idempotency_key(record: Dict[str, Any]) -> str:
    """Stable hash of an event's business identity.

    Two deliveries of the same business event — an SQS retry, a Glue re-run over
    an overlapping window — produce the same key, so downstream deduplication is
    a plain ``DISTINCT`` on one column instead of a fuzzy multi-field match.
    """
    session = record.get("session") or {}
    product = record.get("product") or {}
    customer = record.get("customer") or {}
    order = record.get("order") or {}

    parts = [
        str(record.get("event_type") or ""),
        str(record.get("occurred_at") or ""),
        str(session.get("session_id") or ""),
        str(session.get("sequence") if session.get("sequence") is not None else ""),
        str(product.get("product_id") or ""),
        str(customer.get("customer_id") or ""),
        str(order.get("order_id") or ""),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()  # noqa: S324 - not a security hash


# ─────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────

def normalize_record(product: Dict[str, Any], event: Dict[str, Any], channel: str) -> Dict[str, Any]:
    """Map an upstream product + event pair onto the stable v3 record.

    ``product`` may come from the config file, an external catalog API or a CSV
    dropped on S3 — all three shapes (``product_id`` / ``id`` / ``sku``,
    ``name`` / ``title``, string or nested category) are accepted here so no
    caller has to pre-massage its payload.
    """
    product = product or {}
    event = event or {}

    occurred_at = _to_utc_iso(event.get("occurred_at"))
    product_id = product.get("product_id") or product.get("sku") or product.get("id") or "unknown"
    product_id = str(product_id)

    event_type = _clean_str(event.get("event_type")) or "unknown"

    unit_price = _to_float(event.get("unit_price"))
    if unit_price is None:
        unit_price = _to_float(product.get("price"))

    quantity = _to_int(event.get("quantity"))
    if quantity is None:
        quantity = 1

    discount_pct = _to_float(event.get("discount_pct")) or 0.0

    gross_amount = None if unit_price is None else _round2(unit_price * quantity)
    discount_amount = None if gross_amount is None else _round2(gross_amount * discount_pct / 100.0)
    net_amount = None if gross_amount is None else _round2(gross_amount - (discount_amount or 0.0))

    # `amount` may be supplied directly by upstream (v2 behaviour); it wins so a
    # caller can send an authoritative figure that our arithmetic must not override.
    explicit_amount = _to_float(event.get("amount"))
    if explicit_amount is not None:
        net_amount = _round2(explicit_amount)
        if gross_amount is None:
            gross_amount = net_amount

    record: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{event_type}-{product_id}-{event.get('occurred_at') or 'unknown'}",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "occurred_at": occurred_at,
        "channel": _clean_str(channel) or "web",
        "event_type": event_type,
        "session": {
            "session_id": _clean_str(event.get("session_id")),
            "sequence": _to_int(event.get("sequence")),
        },
        "device": {
            "type": _clean_str(event.get("device_type")) or "unknown",
            "os": _clean_str(event.get("device_os")),
            "user_agent": _clean_str(event.get("user_agent"), 400),
        },
        "geo": {
            "country": _clean_str(event.get("country")),
            "city": _clean_str(event.get("city")),
        },
        "product": {
            "product_id": product_id,
            "sku": _clean_str(product.get("sku") or product.get("id")),
            "name": _clean_str(product.get("title") or product.get("name")),
            "category": _category_name(product.get("category")),
            "brand": _clean_str(product.get("brand")),
            "price": _to_float(product.get("price")),
        },
        "customer": {
            "customer_id": _clean_str(event.get("customer_id")),
            "segment": _clean_str(event.get("segment")),
            "country": _clean_str(event.get("customer_country") or event.get("country")),
            "is_returning": bool(event.get("is_returning", False)),
        },
        "order": {
            "order_id": _clean_str(event.get("order_id")),
            "quantity": quantity,
            "unit_price": _round2(unit_price),
            "discount_pct": _round2(discount_pct),
            "gross_amount": gross_amount,
            "discount_amount": discount_amount,
            "net_amount": net_amount,
            # v2 alias — kept so existing consumers of `order.amount` keep working.
            "amount": net_amount,
            "currency": _clean_str(event.get("currency")) or "EUR",
            "payment_method": _clean_str(event.get("payment_method")) or "unknown",
        },
        "marketing": {
            "campaign": _clean_str(event.get("campaign")),
            "source": _clean_str(event.get("utm_source")),
            "medium": _clean_str(event.get("utm_medium")),
        },
    }

    record["idempotency_key"] = idempotency_key(record)
    return record


# ─────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────

def validate_record(record: Any, strict: bool = False) -> List[str]:
    """Validate a record against the v3 contract.

    Returns a list of machine-readable error codes (empty list = valid) rather
    than raising, so a caller can attach every reason to the rejected record in
    one pass instead of discovering them one exception at a time.

    ``strict=True`` additionally enforces the controlled vocabularies
    (``event_type``, ``channel``, ``currency``, …). Left off by default at ingest
    so a new event type shipped by the front-end team lands in ``raw/`` rather
    than being thrown away before anyone notices.
    """
    errors: List[str] = []

    if not isinstance(record, dict):
        return ["not_a_dict"]

    # ── required top-level blocks ──
    for field in ("occurred_at", "event_type", "product", "customer"):
        if record.get(field) in (None, "", {}, []):
            errors.append(f"missing:{field}")

    for block in ("product", "customer", "order", "session", "device", "geo"):
        value = record.get(block)
        if value is not None and not isinstance(value, dict):
            errors.append(f"not_an_object:{block}")

    # ── timestamps ──
    occurred_at = record.get("occurred_at")
    if isinstance(occurred_at, str) and occurred_at:
        if not _ISO_DATE_RE.match(occurred_at):
            errors.append("invalid_format:occurred_at")
        else:
            try:
                datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
            except ValueError:
                errors.append("invalid_format:occurred_at")
    elif occurred_at is not None and not isinstance(occurred_at, str):
        errors.append("invalid_type:occurred_at")

    # ── product ──
    product = record.get("product")
    if isinstance(product, dict):
        if not product.get("product_id"):
            errors.append("missing:product.product_id")
        price = product.get("price")
        if price is not None:
            if _to_float(price) is None:
                errors.append("invalid_type:product.price")
            elif not 0 <= float(price) <= MAX_UNIT_PRICE:
                errors.append("out_of_range:product.price")

    # ── order ──
    order = record.get("order")
    if isinstance(order, dict):
        quantity = order.get("quantity")
        if quantity is not None:
            if _to_int(quantity) is None:
                errors.append("invalid_type:order.quantity")
            elif not 1 <= int(quantity) <= MAX_QUANTITY:
                errors.append("out_of_range:order.quantity")

        discount = order.get("discount_pct")
        if discount is not None:
            if _to_float(discount) is None:
                errors.append("invalid_type:order.discount_pct")
            elif not 0 <= float(discount) <= 100:
                errors.append("out_of_range:order.discount_pct")

        net = order.get("net_amount")
        if net is not None and _to_float(net) is None:
            errors.append("invalid_type:order.net_amount")
        elif net is not None and float(net) < 0:
            errors.append("out_of_range:order.net_amount")

        if strict and order.get("currency") and order["currency"] not in CURRENCIES:
            errors.append("unknown_value:order.currency")
        if strict and order.get("payment_method") and order["payment_method"] not in PAYMENT_METHODS:
            errors.append("unknown_value:order.payment_method")

    # ── controlled vocabularies ──
    if strict:
        if record.get("event_type") and record["event_type"] not in EVENT_TYPES:
            errors.append("unknown_value:event_type")
        if record.get("channel") and record["channel"] not in CHANNELS:
            errors.append("unknown_value:channel")
        device = record.get("device")
        if isinstance(device, dict) and device.get("type") and device["type"] not in DEVICE_TYPES:
            errors.append("unknown_value:device.type")
        customer = record.get("customer")
        if isinstance(customer, dict) and customer.get("segment") and customer["segment"] not in CUSTOMER_SEGMENTS:
            errors.append("unknown_value:customer.segment")

    return errors


def is_valid(record: Any, strict: bool = False) -> bool:
    """Convenience wrapper around :func:`validate_record`."""
    return not validate_record(record, strict=strict)
