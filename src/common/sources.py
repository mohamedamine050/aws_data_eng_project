"""Input connectors — where the pipeline's reference data comes from.

The producer originally accepted products from exactly two places: an inline
``PRODUCTS`` list in the config, or one hard-coded HTTP catalog. That is a
single point of failure and it makes onboarding a new catalog a code change.

This module turns every input into a plugin selected by config:

===========================  ===========================================
``PRODUCTS``                 inline list in the config JSON
``PRODUCTS_S3_JSON``         ``s3://bucket/key`` — JSON array or NDJSON
``PRODUCTS_S3_CSV``          ``s3://bucket/key`` — CSV with a header row
``PRODUCTS_LOCAL``           local path (JSON / NDJSON / CSV) for tests
``ECOMMERCE_API_URL``        external catalog API (JSON array or wrapped)
===========================  ===========================================

Customers follow the same pattern (``CUSTOMERS``, ``CUSTOMERS_S3_JSON``,
``CUSTOMERS_S3_CSV``, ``CUSTOMERS_LOCAL``).

Sources are *additive and fault-tolerant*: every configured source is tried, a
failing one is logged and skipped rather than aborting the run, and the union is
deduplicated on ``product_id``. A flaky catalog API can therefore never take the
whole ingest down.

Stdlib + ``boto3`` only — the producer Lambda needs no layer.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

LOGGER = logging.getLogger(__name__)

DEFAULT_HTTP_TIMEOUT = 10

#: Keys an upstream payload may use for each of our canonical product fields,
#: in priority order.
_PRODUCT_ALIASES: Dict[str, Tuple[str, ...]] = {
    "product_id": ("product_id", "id", "sku", "productId"),
    "sku": ("sku", "product_id", "id"),
    "name": ("name", "title", "product_name", "label"),
    "category": ("category", "category_name", "categoryName", "department"),
    "brand": ("brand", "manufacturer", "vendor"),
    "price": ("price", "unit_price", "amount", "list_price"),
}

_CUSTOMER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "customer_id": ("customer_id", "id", "customerId", "user_id"),
    "segment": ("segment", "tier", "customer_segment"),
    "country": ("country", "country_code", "geo_country"),
    "city": ("city", "town"),
}


# ─────────────────────────────────────────────
# LOW-LEVEL READERS
# ─────────────────────────────────────────────

def _s3_client():
    """Imported lazily so this module stays importable without AWS credentials."""
    import boto3

    return boto3.client("s3")


def read_s3_text(uri: str) -> str:
    parsed = urllib.parse.urlparse(uri)
    obj = _s3_client().get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    return obj["Body"].read().decode("utf-8")


def read_local_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def read_text(location: str) -> str:
    """Read a text payload from ``s3://…`` or a local path."""
    if location.startswith("s3://"):
        return read_s3_text(location)
    return read_local_text(location)


def read_http_json(url: str, timeout: int = DEFAULT_HTTP_TIMEOUT) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "ecommerce-pipeline/3.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - config-supplied URL
        return json.loads(response.read().decode("utf-8"))


# ─────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────

def parse_json_payload(text: str) -> List[Dict[str, Any]]:
    """Parse a JSON array, a wrapped object, or NDJSON into a list of dicts."""
    text = (text or "").strip()
    if not text:
        return []

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return parse_ndjson(text)

    return unwrap_items(payload)


def parse_ndjson(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line_no, line in enumerate((text or "").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            LOGGER.warning("Skipping bad NDJSON line %d: %s", line_no, exc)
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def unwrap_items(payload: Any) -> List[Dict[str, Any]]:
    """Pull the item list out of the many shapes catalog APIs return."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("products", "items", "data", "results", "records", "customers"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        # A single object is a one-element list.
        return [payload]
    LOGGER.warning("Unexpected payload type: %s", type(payload).__name__)
    return []


def parse_csv(text: str) -> List[Dict[str, Any]]:
    """Parse a CSV with a header row into a list of dicts (empty cells → None)."""
    if not (text or "").strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows: List[Dict[str, Any]] = []
    for row in reader:
        rows.append({
            (key or "").strip(): (value.strip() if isinstance(value, str) and value.strip() else None)
            for key, value in row.items()
        })
    return rows


def parse_auto(text: str, location: str = "") -> List[Dict[str, Any]]:
    """Pick the parser from the file extension, falling back to sniffing."""
    if location.lower().endswith(".csv"):
        return parse_csv(text)
    if location.lower().endswith((".json", ".ndjson", ".jsonl")):
        return parse_json_payload(text)

    stripped = (text or "").lstrip()
    if stripped.startswith(("{", "[")):
        return parse_json_payload(text)
    return parse_csv(text)


# ─────────────────────────────────────────────
# FIELD MAPPING
# ─────────────────────────────────────────────

def _first(entry: Dict[str, Any], aliases: Tuple[str, ...]) -> Any:
    for alias in aliases:
        if entry.get(alias) not in (None, ""):
            return entry[alias]
    return None


def _category(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        value = value.get("name") or value.get("slug")
    if value in (None, ""):
        return None
    return str(value).strip() or None


def normalize_product(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map an arbitrary catalog row onto our product shape.

    Returns ``None`` when the row cannot yield a usable product — a missing id,
    a missing name, or a price that will not parse. Callers count the drops.
    """
    if not isinstance(entry, dict):
        return None

    product_id = _first(entry, _PRODUCT_ALIASES["product_id"])
    name = _first(entry, _PRODUCT_ALIASES["name"])
    if product_id in (None, "") or name in (None, ""):
        return None

    raw_price = _first(entry, _PRODUCT_ALIASES["price"])
    try:
        price = float(str(raw_price).replace(",", ".")) if raw_price is not None else None
    except (TypeError, ValueError):
        return None
    if price is None or price < 0:
        return None

    return {
        "product_id": str(product_id),
        "sku": str(_first(entry, _PRODUCT_ALIASES["sku"]) or product_id),
        "name": str(name).strip(),
        "category": _category(_first(entry, _PRODUCT_ALIASES["category"])),
        "brand": _category(_first(entry, _PRODUCT_ALIASES["brand"])),
        "price": price,
    }


def normalize_customer(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    customer_id = _first(entry, _CUSTOMER_ALIASES["customer_id"])
    if customer_id in (None, ""):
        return None
    return {
        "customer_id": str(customer_id),
        "segment": _category(_first(entry, _CUSTOMER_ALIASES["segment"])) or "unknown",
        "country": _category(_first(entry, _CUSTOMER_ALIASES["country"])),
        "city": _category(_first(entry, _CUSTOMER_ALIASES["city"])),
    }


# ─────────────────────────────────────────────
# SOURCE RESOLUTION
# ─────────────────────────────────────────────

def _collect(
    config: Dict[str, Any],
    inline_key: str,
    location_keys: Tuple[str, ...],
    api_key: Optional[str],
    timeout: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Gather raw rows from every configured source. Never raises."""
    rows: List[Dict[str, Any]] = []
    used: List[str] = []

    inline = config.get(inline_key)
    if isinstance(inline, list) and inline:
        rows.extend(item for item in inline if isinstance(item, dict))
        used.append(inline_key)

    for key in location_keys:
        location = config.get(key)
        if not location:
            continue
        try:
            rows.extend(parse_auto(read_text(location), location))
            used.append(key)
        except Exception as exc:  # noqa: BLE001 - one bad source must not kill the run
            LOGGER.warning("Source %s (%s) failed: %s", key, location, exc)

    if api_key and config.get(api_key):
        url = config[api_key]
        try:
            rows.extend(unwrap_items(read_http_json(url, timeout)))
            used.append(api_key)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, OSError) as exc:
            LOGGER.warning("Source %s (%s) failed: %s", api_key, url, exc)

    return rows, used


def _dedupe(items: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for item in items:
        identity = item.get(key)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(item)
    return unique


def _resolve(
    config: Dict[str, Any],
    inline_key: str,
    location_keys: Tuple[str, ...],
    api_key: Optional[str],
    normalizer: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    identity_key: str,
    timeout: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    raw_rows, used = _collect(config, inline_key, location_keys, api_key, timeout)

    normalized = []
    dropped = 0
    for row in raw_rows:
        item = normalizer(row)
        if item is None:
            dropped += 1
            continue
        normalized.append(item)

    unique = _dedupe(normalized, identity_key)

    stats = {
        "sources_used": used,
        "rows_read": len(raw_rows),
        "rows_dropped": dropped,
        "duplicates_removed": len(normalized) - len(unique),
        "resolved": len(unique),
    }
    if dropped:
        LOGGER.warning("Dropped %d malformed rows from %s", dropped, used or ["<none>"])
    return unique, stats


def load_products(
    config: Dict[str, Any], timeout: int = DEFAULT_HTTP_TIMEOUT
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolve the product catalog from every source configured.

    Returns ``(products, stats)``; ``stats`` is surfaced in the Lambda's return
    value so a run that silently read zero rows is visible in CloudWatch.
    """
    return _resolve(
        config,
        inline_key="PRODUCTS",
        location_keys=("PRODUCTS_S3_JSON", "PRODUCTS_S3_CSV", "PRODUCTS_LOCAL"),
        api_key="ECOMMERCE_API_URL",
        normalizer=normalize_product,
        identity_key="product_id",
        timeout=timeout,
    )


def load_customers(
    config: Dict[str, Any], timeout: int = DEFAULT_HTTP_TIMEOUT
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolve the customer catalog. Empty is fine — the simulator synthesizes one."""
    return _resolve(
        config,
        inline_key="CUSTOMERS",
        location_keys=("CUSTOMERS_S3_JSON", "CUSTOMERS_S3_CSV", "CUSTOMERS_LOCAL"),
        api_key="CUSTOMER_API_URL",
        normalizer=normalize_customer,
        identity_key="customer_id",
        timeout=timeout,
    )
