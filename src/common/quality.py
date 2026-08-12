"""Declarative data-quality rules and reporting.

Quality used to be one boolean buried in the Glue job (``_validate_record``),
which could tell you a record was bad but never *how bad the batch was* or
*which rule fired*. Here rules are data, not control flow:

    >>> report = evaluate(records)
    >>> report["verdict"]              # 'pass' | 'warn' | 'fail'
    >>> report["rules"]["price_positive"]["failed"]

Two consumers share these definitions:

* the **stream processor**, which runs them per record at ingest and routes the
  failures to the ``rejected/`` zone with their rule names attached;
* the **Glue job**, which runs them over the whole batch and writes a
  ``quality/`` report next to the data, plus CloudWatch metrics.

Stdlib-only, so the Lambda keeps its dependency-free package.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence

from common.ecommerce_schema import (
    CHANNELS,
    CURRENCIES,
    EVENT_TYPES,
    MAX_QUANTITY,
    MAX_UNIT_PRICE,
    validate_record,
)

ERROR = "error"
WARN = "warn"


class Rule(NamedTuple):
    """One quality rule.

    ``check`` returns ``True`` when the record *passes*. A rule that raises is
    counted as a failure — a rule crashing on unexpected input is itself a
    quality signal, and must never abort the batch.
    """

    name: str
    severity: str
    description: str
    check: Callable[[Dict[str, Any]], bool]


# ─────────────────────────────────────────────
# RULE PREDICATES
# ─────────────────────────────────────────────

def _block(record: Dict[str, Any], name: str) -> Dict[str, Any]:
    value = record.get(name)
    return value if isinstance(value, dict) else {}


def _schema_valid(record: Dict[str, Any]) -> bool:
    return not validate_record(record)


def _has_product_id(record: Dict[str, Any]) -> bool:
    return bool(_block(record, "product").get("product_id"))


def _has_customer_id(record: Dict[str, Any]) -> bool:
    return bool(_block(record, "customer").get("customer_id"))


def _known_event_type(record: Dict[str, Any]) -> bool:
    return record.get("event_type") in EVENT_TYPES


def _known_channel(record: Dict[str, Any]) -> bool:
    return record.get("channel") in CHANNELS


def _known_currency(record: Dict[str, Any]) -> bool:
    return _block(record, "order").get("currency") in CURRENCIES


def _price_in_range(record: Dict[str, Any]) -> bool:
    price = _block(record, "product").get("price")
    if price is None:
        return True  # absence is covered by `price_present`, not by this rule
    return isinstance(price, (int, float)) and 0 <= float(price) <= MAX_UNIT_PRICE


def _price_present(record: Dict[str, Any]) -> bool:
    return _block(record, "product").get("price") is not None


def _quantity_in_range(record: Dict[str, Any]) -> bool:
    quantity = _block(record, "order").get("quantity")
    if quantity is None:
        return True
    return isinstance(quantity, int) and 1 <= quantity <= MAX_QUANTITY


def _amount_non_negative(record: Dict[str, Any]) -> bool:
    amount = _block(record, "order").get("net_amount")
    return amount is None or float(amount) >= 0


def _amount_consistent(record: Dict[str, Any]) -> bool:
    """net = gross − discount, to the cent."""
    order = _block(record, "order")
    gross, discount, net = order.get("gross_amount"), order.get("discount_amount"), order.get("net_amount")
    if None in (gross, net):
        return True
    return abs(float(gross) - float(discount or 0.0) - float(net)) < 0.011


def _timestamp_parseable(record: Dict[str, Any]) -> bool:
    value = record.get("occurred_at")
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _not_in_future(record: Dict[str, Any]) -> bool:
    """Clock skew beyond an hour means a broken client, not a slow network."""
    value = record.get("occurred_at")
    if not isinstance(value, str):
        return True
    try:
        occurred = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return True
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    return (occurred - datetime.now(timezone.utc)).total_seconds() <= 3600


def _has_session(record: Dict[str, Any]) -> bool:
    return bool(_block(record, "session").get("session_id"))


def _has_idempotency_key(record: Dict[str, Any]) -> bool:
    return bool(record.get("idempotency_key"))


def _order_event_has_order_id(record: Dict[str, Any]) -> bool:
    if record.get("event_type") not in ("checkout_started", "order_placed", "order_cancelled", "refund_issued"):
        return True
    return bool(_block(record, "order").get("order_id"))


#: The rule set. ``error`` rules reject the record; ``warn`` rules only degrade
#: the batch's quality score, so a missing brand never costs you an event.
RULES: Sequence[Rule] = (
    Rule("schema_valid", ERROR, "Record satisfies the v3 schema contract", _schema_valid),
    Rule("product_id_present", ERROR, "product.product_id is set", _has_product_id),
    Rule("timestamp_parseable", ERROR, "occurred_at parses as ISO-8601", _timestamp_parseable),
    Rule("quantity_in_range", ERROR, f"1 <= order.quantity <= {MAX_QUANTITY}", _quantity_in_range),
    Rule("amount_non_negative", ERROR, "order.net_amount >= 0", _amount_non_negative),
    Rule("price_in_range", ERROR, f"0 <= product.price <= {MAX_UNIT_PRICE}", _price_in_range),
    Rule("customer_id_present", WARN, "customer.customer_id is set", _has_customer_id),
    Rule("known_event_type", WARN, "event_type is in the controlled vocabulary", _known_event_type),
    Rule("known_channel", WARN, "channel is in the controlled vocabulary", _known_channel),
    Rule("known_currency", WARN, "order.currency is a supported ISO code", _known_currency),
    Rule("price_present", WARN, "product.price is populated", _price_present),
    Rule("amount_consistent", WARN, "net_amount == gross_amount - discount_amount", _amount_consistent),
    Rule("timestamp_not_in_future", WARN, "occurred_at is not more than 1h ahead", _not_in_future),
    Rule("session_present", WARN, "session.session_id is set", _has_session),
    Rule("idempotency_key_present", WARN, "idempotency_key is set", _has_idempotency_key),
    Rule("order_event_has_order_id", WARN, "order-stage events carry an order_id", _order_event_has_order_id),
)

RULES_BY_NAME: Dict[str, Rule] = {rule.name: rule for rule in RULES}

#: Batch-level gates. Override any of them via the ``QUALITY`` block in the job config.
DEFAULT_THRESHOLDS = {
    "min_pass_pct": 95.0,       # below this → fail
    "warn_pass_pct": 99.0,      # below this → warn
    "max_duplicate_pct": 5.0,
    "min_records": 0,
}


# ─────────────────────────────────────────────
# EVALUATION
# ─────────────────────────────────────────────

def check_record(record: Dict[str, Any], rules: Sequence[Rule] = RULES) -> Dict[str, List[str]]:
    """Run every rule against one record.

    Returns ``{"errors": [...], "warnings": [...]}`` of failing rule names.
    """
    errors: List[str] = []
    warnings: List[str] = []
    for rule in rules:
        try:
            passed = bool(rule.check(record))
        except Exception:  # noqa: BLE001 - a crashing rule is a failing rule
            passed = False
        if passed:
            continue
        (errors if rule.severity == ERROR else warnings).append(rule.name)
    return {"errors": errors, "warnings": warnings}


def evaluate(
    records: Sequence[Dict[str, Any]],
    rules: Sequence[Rule] = RULES,
    thresholds: Optional[Dict[str, Any]] = None,
    duplicates: int = 0,
) -> Dict[str, Any]:
    """Score a batch and return a report ready to be written to ``quality/``.

    ``duplicates`` is supplied by the caller because deduplication happens where
    the data lives (a ``set`` in the Lambda, a window function in Spark) — the
    report just needs the count to apply the threshold.
    """
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    total = len(records)

    per_rule = {
        rule.name: {"severity": rule.severity, "description": rule.description, "failed": 0}
        for rule in rules
    }
    failed_records = 0
    warned_records = 0

    for record in records:
        outcome = check_record(record, rules)
        for name in outcome["errors"] + outcome["warnings"]:
            per_rule[name]["failed"] += 1
        if outcome["errors"]:
            failed_records += 1
        elif outcome["warnings"]:
            warned_records += 1

    passed_records = total - failed_records
    pass_pct = 100.0 * passed_records / total if total else 100.0
    duplicate_pct = 100.0 * duplicates / total if total else 0.0

    breaches: List[str] = []
    if total < thresholds["min_records"]:
        breaches.append("min_records")
    if pass_pct < thresholds["min_pass_pct"]:
        breaches.append("min_pass_pct")
    if duplicate_pct > thresholds["max_duplicate_pct"]:
        breaches.append("max_duplicate_pct")

    if breaches:
        verdict = "fail"
    elif pass_pct < thresholds["warn_pass_pct"] or warned_records:
        verdict = "warn"
    else:
        verdict = "pass"

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "verdict": verdict,
        "breaches": breaches,
        "thresholds": thresholds,
        "totals": {
            "records": total,
            "passed": passed_records,
            "failed": failed_records,
            "warned": warned_records,
            "duplicates": duplicates,
            "pass_pct": round(pass_pct, 2),
            "duplicate_pct": round(duplicate_pct, 2),
        },
        "rules": {
            name: {**info, "failed_pct": round(100.0 * info["failed"] / total, 2) if total else 0.0}
            for name, info in per_rule.items()
        },
    }


def partition(
    records: Sequence[Dict[str, Any]], rules: Sequence[Rule] = RULES
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split a batch into ``(accepted, rejected)``.

    Each rejected record is wrapped as ``{"record": …, "quality": {...}}`` so the
    ``rejected/`` zone is self-describing: you can replay it after a fix without
    having to re-derive why it was rejected.
    """
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for record in records:
        outcome = check_record(record, rules)
        if outcome["errors"]:
            rejected.append({
                "rejected_at": datetime.now(timezone.utc).isoformat(),
                "reasons": outcome["errors"],
                "warnings": outcome["warnings"],
                "record": record,
            })
        else:
            accepted.append(record)

    return accepted, rejected
