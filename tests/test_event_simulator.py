"""Unit tests for common.event_simulator."""

import random
from collections import Counter
from datetime import datetime, timedelta, timezone

from common import event_simulator as sim
from common.ecommerce_schema import EVENT_TYPES, SCHEMA_VERSION


def _products():
    return [
        {"product_id": "sku-1", "sku": "SKU-1", "name": "Mouse", "category": "electronics", "price": 19.99},
        {"product_id": "sku-2", "sku": "SKU-2", "name": "Keyboard", "category": "electronics", "price": 89.0},
        {"product_id": "sku-3", "sku": "SKU-3", "name": "Monitor", "category": "electronics", "price": 249.0},
    ]


def _config(**overrides):
    return {"SEED": 42, "SESSIONS": 30, "CUSTOMER_POOL": 10, **overrides}


# ── DETERMINISM ──────────────────────────────────────────────

def test_same_seed_produces_the_same_events():
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)
    first = sim.simulate(_products(), _config(), now=now)
    second = sim.simulate(_products(), _config(), now=now)

    assert [r["event_type"] for r in first] == [r["event_type"] for r in second]
    assert [r["occurred_at"] for r in first] == [r["occurred_at"] for r in second]


def test_different_seeds_diverge():
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)
    first = sim.simulate(_products(), _config(SEED=1), now=now)
    second = sim.simulate(_products(), _config(SEED=2), now=now)

    assert [r["event_id"] for r in first] != [r["event_id"] for r in second]


# ── SHAPE ────────────────────────────────────────────────────

def test_every_record_is_a_valid_v3_record():
    records = sim.simulate(_products(), _config())
    assert records

    for record in records:
        assert record["schema_version"] == SCHEMA_VERSION
        assert record["event_type"] in EVENT_TYPES
        assert record["product"]["product_id"]
        assert record["customer"]["customer_id"]
        assert record["session"]["session_id"]
        assert record["idempotency_key"]


def test_records_are_sorted_by_event_time():
    records = sim.simulate(_products(), _config())
    timestamps = [r["occurred_at"] for r in records]
    assert timestamps == sorted(timestamps)


def test_events_land_inside_the_configured_window():
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)
    records = sim.simulate(_products(), _config(WINDOW_MINUTES=30), now=now)

    starts = [datetime.fromisoformat(r["occurred_at"]) for r in records]
    assert min(starts) >= now - timedelta(minutes=30)
    # Post-purchase refunds are deliberately dated days later.
    assert min(starts) <= now + timedelta(days=15)


def test_idempotency_keys_are_unique_within_a_run():
    records = sim.simulate(_products(), _config(SESSIONS=50))
    keys = [r["idempotency_key"] for r in records]
    assert len(keys) == len(set(keys))


# ── FUNNEL SEMANTICS ─────────────────────────────────────────

def test_funnel_is_monotonically_narrowing():
    records = sim.simulate(_products(), _config(SESSIONS=200, CUSTOMER_POOL=50))
    counts = Counter(r["event_type"] for r in records)

    assert counts["product_viewed"] > counts["add_to_cart"] > 0
    assert counts["add_to_cart"] >= counts["order_placed"]


def test_orders_share_one_order_id_per_session():
    rng = random.Random(7)
    for _ in range(50):
        records = sim.simulate_session(
            _products(),
            {"customer_id": "cust-1", "segment": "vip", "country": "FR", "city": "Paris"},
            datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
            rng,
            {"FUNNEL_RATES": {"add_to_cart": 1.0, "checkout_started": 1.0,
                              "order_placed": 1.0, "payment_failed": 0.0, "remove_from_cart": 0.0}},
        )
        order_ids = {r["order"]["order_id"] for r in records if r["order"]["order_id"]}
        assert len(order_ids) <= 1


def test_order_lines_carry_quantity_and_net_amount():
    rng = random.Random(3)
    records = sim.simulate_session(
        _products(),
        {"customer_id": "cust-1", "segment": "new", "country": "FR", "city": "Paris"},
        datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
        rng,
        {"FUNNEL_RATES": {"add_to_cart": 1.0, "checkout_started": 1.0,
                          "order_placed": 1.0, "payment_failed": 0.0, "remove_from_cart": 0.0},
         "DISCOUNTS": [10.0]},
    )

    orders = [r for r in records if r["event_type"] == "order_placed"]
    assert orders
    for order in orders:
        block = order["order"]
        assert block["quantity"] >= 1
        assert block["discount_pct"] == 10.0
        assert abs(block["gross_amount"] - block["discount_amount"] - block["net_amount"]) < 0.011


def test_sequence_increases_within_a_session():
    rng = random.Random(11)
    records = sim.simulate_session(
        _products(),
        {"customer_id": "cust-1", "segment": "new", "country": "FR", "city": "Paris"},
        datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
        rng,
        {"FUNNEL_RATES": {"add_to_cart": 1.0, "checkout_started": 1.0, "order_placed": 1.0}},
    )
    sequences = [r["session"]["sequence"] for r in records]
    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)


# ── EDGE CASES ───────────────────────────────────────────────

def test_no_products_produces_no_events():
    assert sim.simulate([], _config()) == []


def test_zero_sessions_produces_no_events():
    assert sim.simulate(_products(), _config(SESSIONS=0)) == []


def test_supplied_customers_are_the_only_ones_used():
    customers = [{"customer_id": "cust-only", "segment": "vip", "country": "FR", "city": "Paris"}]
    records = sim.simulate(_products(), _config(), customers=customers)

    assert {r["customer"]["customer_id"] for r in records} == {"cust-only"}


def test_build_customers_is_reproducible():
    rng = random.Random(5)
    first = sim.build_customers(5, rng, sim.DEFAULT_COUNTRIES)
    rng = random.Random(5)
    second = sim.build_customers(5, rng, sim.DEFAULT_COUNTRIES)

    assert first == second
    assert len(first) == 5
    assert first[0]["customer_id"] == "cust-00001"
