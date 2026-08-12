"""Session-based e-commerce traffic simulator.

The producer used to emit exactly one ``product_viewed`` per product per run,
which makes for a pipeline that can never exercise a funnel, a session, a basket
or a refund. This module generates *sessions* instead: a customer arrives on a
channel with a device, browses a few products, and drops out of the funnel at a
realistic rate.

    product_viewed → add_to_cart → checkout_started → order_placed
                                         ↘ payment_failed
                                                       ↘ order_cancelled / refund_issued

Stdlib-only (``random``, ``datetime``, ``uuid``) so the producer Lambda keeps a
dependency-free deployment package and a fast cold start.

Determinism: pass ``SEED`` in the config to get a byte-identical run — that is
what makes the downstream Glue and RDS assertions testable.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from common.ecommerce_schema import normalize_record

# ─────────────────────────────────────────────
# DEFAULT POPULATION
# ─────────────────────────────────────────────

DEFAULT_CHANNEL_WEIGHTS = {"web": 45, "mobile_web": 25, "mobile_app": 20, "marketplace": 7, "store": 3}
DEFAULT_DEVICE_WEIGHTS = {"desktop": 40, "mobile": 50, "tablet": 10}
DEFAULT_COUNTRIES = {"FR": 40, "DE": 15, "ES": 12, "IT": 10, "BE": 8, "GB": 8, "US": 7}
DEFAULT_CITIES = {
    "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nantes"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "ES": ["Madrid", "Barcelona", "Valencia"],
    "IT": ["Rome", "Milan", "Turin"],
    "BE": ["Brussels", "Antwerp"],
    "GB": ["London", "Manchester"],
    "US": ["New York", "Austin", "Seattle"],
}
DEFAULT_OS_BY_DEVICE = {
    "desktop": ["windows", "macos", "linux"],
    "mobile": ["android", "ios"],
    "tablet": ["android", "ipados"],
}
DEFAULT_PAYMENT_METHODS = {"card": 65, "paypal": 20, "wallet": 8, "bank_transfer": 5, "gift_card": 2}
DEFAULT_CAMPAIGNS = [
    {"campaign": "spring_sale", "utm_source": "google", "utm_medium": "cpc"},
    {"campaign": "newsletter_w24", "utm_source": "mailchimp", "utm_medium": "email"},
    {"campaign": "retargeting", "utm_source": "meta", "utm_medium": "social"},
    {"campaign": None, "utm_source": "direct", "utm_medium": "none"},
]
DEFAULT_SEGMENTS = {"new": 45, "returning": 30, "loyal": 15, "vip": 7, "churn_risk": 3}

#: Probability of moving from one funnel stage to the next.
DEFAULT_FUNNEL_RATES = {
    "add_to_cart": 0.35,
    "remove_from_cart": 0.12,
    "checkout_started": 0.55,
    "payment_failed": 0.08,
    "order_placed": 0.80,
    "order_cancelled": 0.04,
    "refund_issued": 0.03,
    "product_searched": 0.30,
}

DEFAULT_DISCOUNTS = [0.0, 0.0, 0.0, 5.0, 10.0, 15.0, 20.0]
DEFAULT_SEARCH_TERMS = ["laptop", "mouse", "usb-c", "chair", "monitor", "headphones", "desk lamp"]


# ─────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────

def _weighted_choice(rng: random.Random, weights: Dict[str, float]) -> str:
    keys = list(weights.keys())
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


def _user_agent(device_type: str, device_os: str) -> str:
    if device_type == "desktop":
        return f"Mozilla/5.0 ({device_os}; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36"
    if device_type == "tablet":
        return f"Mozilla/5.0 ({device_os}; Tablet) AppleWebKit/605.1 Mobile/15E148 Safari/604.1"
    return f"Mozilla/5.0 ({device_os}; Mobile) AppleWebKit/605.1 Mobile/15E148 Safari/604.1"


def build_customers(count: int, rng: random.Random, country_weights: Dict[str, float]) -> List[Dict[str, Any]]:
    """Synthesize a customer population.

    A stable customer pool (rather than a fresh id per event) is what makes
    repeat purchases, RFM segmentation and returning-visitor rates meaningful
    downstream.
    """
    customers = []
    for index in range(count):
        country = _weighted_choice(rng, country_weights)
        customers.append({
            "customer_id": f"cust-{index + 1:05d}",
            "segment": _weighted_choice(rng, DEFAULT_SEGMENTS),
            "country": country,
            "city": rng.choice(DEFAULT_CITIES.get(country, ["unknown"])),
        })
    return customers


def _session_context(
    rng: random.Random,
    customer: Dict[str, Any],
    config: Dict[str, Any],
    channel_weights: Dict[str, float],
    device_weights: Dict[str, float],
) -> Dict[str, Any]:
    device_type = _weighted_choice(rng, device_weights)
    device_os = rng.choice(DEFAULT_OS_BY_DEVICE.get(device_type, ["unknown"]))
    campaign = rng.choice(config.get("CAMPAIGNS") or DEFAULT_CAMPAIGNS)
    segment = customer.get("segment") or "new"

    return {
        "session_id": f"sess-{uuid.uuid4().hex[:16]}",
        "channel": _weighted_choice(rng, channel_weights),
        "customer_id": customer["customer_id"],
        "segment": segment,
        "country": customer.get("country"),
        "city": customer.get("city"),
        "device_type": device_type,
        "device_os": device_os,
        "user_agent": _user_agent(device_type, device_os),
        "currency": config.get("CURRENCY", "EUR"),
        "payment_method": _weighted_choice(rng, config.get("PAYMENT_METHODS") or DEFAULT_PAYMENT_METHODS),
        "is_returning": segment in ("returning", "loyal", "vip"),
        **{k: v for k, v in campaign.items()},
    }


def _emit(
    context: Dict[str, Any],
    product: Dict[str, Any],
    event_type: str,
    occurred_at: datetime,
    sequence: int,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    event = {
        "event_type": event_type,
        "occurred_at": occurred_at.replace(microsecond=0).isoformat(),
        "session_id": context["session_id"],
        "sequence": sequence,
        "customer_id": context["customer_id"],
        "segment": context["segment"],
        "country": context["country"],
        "city": context["city"],
        "device_type": context["device_type"],
        "device_os": context["device_os"],
        "user_agent": context["user_agent"],
        "currency": context["currency"],
        "payment_method": context["payment_method"],
        "is_returning": context["is_returning"],
        "campaign": context.get("campaign"),
        "utm_source": context.get("utm_source"),
        "utm_medium": context.get("utm_medium"),
    }
    if extra:
        event.update(extra)
    return normalize_record(product, event, context["channel"])


# ─────────────────────────────────────────────
# SESSION GENERATION
# ─────────────────────────────────────────────

def simulate_session(
    products: Sequence[Dict[str, Any]],
    customer: Dict[str, Any],
    started_at: datetime,
    rng: random.Random,
    config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Generate the full event trail of one browsing session.

    Returns records already in v3 shape — the caller only has to ship them.
    """
    config = config or {}
    rates = {**DEFAULT_FUNNEL_RATES, **(config.get("FUNNEL_RATES") or {})}
    channel_weights = config.get("CHANNEL_WEIGHTS") or DEFAULT_CHANNEL_WEIGHTS
    device_weights = config.get("DEVICE_WEIGHTS") or DEFAULT_DEVICE_WEIGHTS
    discounts = config.get("DISCOUNTS") or DEFAULT_DISCOUNTS
    max_views = int(config.get("MAX_VIEWS_PER_SESSION", 5))

    context = _session_context(rng, customer, config, channel_weights, device_weights)

    records: List[Dict[str, Any]] = []
    cursor = started_at
    sequence = 0
    cart: List[Dict[str, Any]] = []

    def step(seconds_range=(5, 120)) -> datetime:
        nonlocal cursor
        cursor = cursor + timedelta(seconds=rng.randint(*seconds_range))
        return cursor

    # ── discovery ──
    if rng.random() < rates["product_searched"]:
        sequence += 1
        records.append(_emit(
            context, products[rng.randrange(len(products))], "product_searched", cursor, sequence,
            {"search_term": rng.choice(DEFAULT_SEARCH_TERMS)},
        ))

    # ── browsing ──
    viewed = rng.sample(list(products), k=min(max_views, len(products))) if products else []
    for product in viewed:
        sequence += 1
        records.append(_emit(context, product, "product_viewed", step(), sequence))

        if rng.random() >= rates["add_to_cart"]:
            continue

        quantity = rng.choices([1, 1, 1, 2, 2, 3], k=1)[0]
        discount = rng.choice(discounts)
        sequence += 1
        records.append(_emit(
            context, product, "add_to_cart", step(), sequence,
            {"quantity": quantity, "discount_pct": discount},
        ))
        cart.append({"product": product, "quantity": quantity, "discount_pct": discount})

        # Second thoughts: an abandoned line item is a real, frequent signal.
        if rng.random() < rates["remove_from_cart"]:
            sequence += 1
            records.append(_emit(
                context, product, "remove_from_cart", step(), sequence,
                {"quantity": quantity, "discount_pct": discount},
            ))
            cart.pop()

    if not cart:
        return records

    # ── checkout ──
    if rng.random() >= rates["checkout_started"]:
        return records

    order_id = f"ord-{uuid.uuid4().hex[:12]}"
    for line in cart:
        sequence += 1
        records.append(_emit(
            context, line["product"], "checkout_started", step((10, 90)), sequence,
            {"quantity": line["quantity"], "discount_pct": line["discount_pct"], "order_id": order_id},
        ))

    if rng.random() < rates["payment_failed"]:
        sequence += 1
        records.append(_emit(
            context, cart[0]["product"], "payment_failed", step((5, 40)), sequence,
            {"quantity": cart[0]["quantity"], "order_id": order_id,
             "failure_reason": rng.choice(["insufficient_funds", "3ds_timeout", "card_declined"])},
        ))
        return records

    if rng.random() >= rates["order_placed"]:
        return records

    placed_at = step((10, 120))
    for line in cart:
        sequence += 1
        records.append(_emit(
            context, line["product"], "order_placed", placed_at, sequence,
            {"quantity": line["quantity"], "discount_pct": line["discount_pct"], "order_id": order_id},
        ))

    # ── post-purchase ──
    if rng.random() < rates["order_cancelled"]:
        sequence += 1
        line = cart[0]
        records.append(_emit(
            context, line["product"], "order_cancelled", placed_at + timedelta(minutes=rng.randint(5, 240)),
            sequence, {"quantity": line["quantity"], "order_id": order_id,
                       "cancel_reason": rng.choice(["customer_request", "out_of_stock", "fraud_check"])},
        ))
    elif rng.random() < rates["refund_issued"]:
        sequence += 1
        line = cart[0]
        records.append(_emit(
            context, line["product"], "refund_issued", placed_at + timedelta(days=rng.randint(1, 14)),
            sequence, {"quantity": line["quantity"], "order_id": order_id,
                       "refund_reason": rng.choice(["damaged", "not_as_described", "late_delivery"])},
        ))

    return records


def simulate(
    products: Sequence[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    customers: Optional[Iterable[Dict[str, Any]]] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Generate ``SESSIONS`` sessions spread over the last ``WINDOW_MINUTES``.

    Config keys (all optional, all under the producer's JSON config):

    ============================  =========================================
    ``SESSIONS``                  number of sessions per run (default 25)
    ``SEED``                      RNG seed — set it for reproducible runs
    ``WINDOW_MINUTES``            spread session starts over N minutes (60)
    ``CUSTOMER_POOL``             size of the synthetic customer pool (200)
    ``MAX_VIEWS_PER_SESSION``     product views per session (5)
    ``FUNNEL_RATES``              per-stage conversion overrides
    ``CHANNEL_WEIGHTS``           channel mix
    ``DEVICE_WEIGHTS``            device mix
    ``CAMPAIGNS``                 list of ``{campaign, utm_source, utm_medium}``
    ``DISCOUNTS``                 discount percentages drawn per cart line
    ``PAYMENT_METHODS``           payment-method mix
    ``CURRENCY``                  ISO currency code (default ``EUR``)
    ============================  =========================================
    """
    config = config or {}
    products = list(products or [])
    if not products:
        return []

    sessions = int(config.get("SESSIONS", 25))
    if sessions <= 0:
        return []

    window_minutes = int(config.get("WINDOW_MINUTES", 60))
    seed = config.get("SEED")
    rng = random.Random(seed) if seed is not None else random.Random()

    now = now or datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=window_minutes)

    pool = list(customers) if customers else build_customers(
        int(config.get("CUSTOMER_POOL", 200)), rng, config.get("COUNTRY_WEIGHTS") or DEFAULT_COUNTRIES
    )
    if not pool:
        return []

    records: List[Dict[str, Any]] = []
    for _ in range(sessions):
        customer = pool[rng.randrange(len(pool))]
        offset = rng.uniform(0, max(window_minutes, 1))
        started_at = window_start + timedelta(minutes=offset)
        records.extend(simulate_session(products, customer, started_at, rng, config))

    records.sort(key=lambda record: record["occurred_at"])
    return records
