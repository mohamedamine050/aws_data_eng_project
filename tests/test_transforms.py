"""Unit tests for the Spark transformations, now inlined in the two jobs.

The whole module is skipped when PySpark is missing (the `spark` fixture calls
`importorskip`), so the pure-Python suite still runs in a bare environment.

Filesystem round-trips (`read_raw` / `write_dataset`) additionally need a
working local Hadoop filesystem, which Windows lacks without `winutils.exe`.
Those two tests probe for it and skip rather than fail — the transformation
tests, which are where the logic lives, run everywhere PySpark does.
"""

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="PySpark not installed")

import jobs.glue_ecommerce_processing as silver  # noqa: E402
import jobs.glue_silver_to_gold as gold  # noqa: E402
from common.ecommerce_schema import normalize_record  # noqa: E402
from common.event_simulator import simulate  # noqa: E402


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────

def _raw_row(record):
    """Project a v3 record onto RAW_SCHEMA's exact field set."""
    def block(name, fields):
        source = record.get(name) or {}
        return {field: source.get(field) for field in fields}

    return {
        "schema_version": record.get("schema_version"),
        "event_id": record.get("event_id"),
        "idempotency_key": record.get("idempotency_key"),
        "ingested_at": record.get("ingested_at"),
        "occurred_at": record.get("occurred_at"),
        "channel": record.get("channel"),
        "event_type": record.get("event_type"),
        "session": block("session", ["session_id", "sequence"]),
        "device": block("device", ["type", "os", "user_agent"]),
        "geo": block("geo", ["country", "city"]),
        "product": block("product", ["product_id", "sku", "name", "category", "brand", "price"]),
        "customer": block("customer", ["customer_id", "segment", "country", "is_returning"]),
        "order": block("order", [
            "order_id", "quantity", "unit_price", "discount_pct",
            "gross_amount", "discount_amount", "net_amount", "amount", "currency", "payment_method",
        ]),
        "marketing": block("marketing", ["campaign", "source", "medium"]),
        "_meta": {"processed_at": None, "source": "sqs", "message_id": None, "source_object": None},
    }


def _raw_df(spark, records):
    return spark.createDataFrame([_raw_row(r) for r in records], schema=silver.raw_schema())


def _event(event_type, product_id="sku-1", price=100.0, **overrides):
    product = {"product_id": product_id, "sku": product_id.upper(), "name": f"Product {product_id}",
               "category": "electronics", "brand": "Acme", "price": price}
    event = {
        "event_type": event_type,
        "occurred_at": "2026-06-24T12:00:00+00:00",
        "session_id": "sess-1",
        "sequence": 1,
        "customer_id": "cust-1",
        "segment": "new",
        "country": "FR",
        "device_type": "desktop",
        "currency": "EUR",
        "quantity": 1,
        "payment_method": "card",
        **overrides,
    }
    return normalize_record(product, event, overrides.get("channel", "web"))


@pytest.fixture(scope="module")
def simulated(spark):
    """A realistic multi-session batch, processed once and reused."""
    products = [
        {"product_id": f"sku-{i}", "sku": f"SKU-{i}", "name": f"Product {i}",
         "category": "electronics", "price": 20.0 * i}
        for i in range(1, 6)
    ]
    records = simulate(
        products,
        {"SEED": 7, "SESSIONS": 60, "CUSTOMER_POOL": 12, "WINDOW_MINUTES": 120},
        now=datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc),
    )
    processed = silver.to_processed(_raw_df(spark, records)).cache()
    processed.count()
    return processed


# ─────────────────────────────────────────────
# PROCESSED FACT TABLE
# ─────────────────────────────────────────────

def test_to_processed_produces_the_declared_columns(spark):
    processed = silver.to_processed(_raw_df(spark, [_event("product_viewed")]))
    assert processed.columns == silver.PROCESSED_COLUMNS


def test_flatten_lifts_nested_fields(spark):
    row = silver.to_processed(_raw_df(spark, [_event("order_placed", price=249.0)])).collect()[0]

    assert row["product_id"] == "sku-1"
    assert row["product_name"] == "Product sku-1"
    assert row["category"] == "electronics"
    assert row["customer_id"] == "cust-1"
    assert row["session_id"] == "sess-1"
    assert row["channel"] == "web"
    assert row["device_type"] == "desktop"
    assert row["currency"] == "EUR"


def test_partition_columns_come_from_event_time(spark):
    row = silver.to_processed(
        _raw_df(spark, [_event("product_viewed", occurred_at="2026-03-01T07:45:00+00:00")])
    ).collect()[0]

    assert row["partition_date"] == "2026-03-01"
    assert row["partition_hour"] == "07"


@pytest.mark.parametrize("price,expected", [
    (19.99, "budget"), (99.0, "mid"), (599.0, "premium"), (None, "unknown"),
])
def test_price_category_buckets(spark, price, expected):
    row = silver.to_processed(_raw_df(spark, [_event("product_viewed", price=price)])).collect()[0]
    assert row["price_category"] == expected


def test_refunds_and_cancellations_carry_negative_signed_revenue(spark):
    records = [
        _event("order_placed", price=100.0),
        _event("refund_issued", price=100.0, session_id="sess-2"),
        _event("order_cancelled", price=100.0, session_id="sess-3"),
        _event("product_viewed", price=100.0, session_id="sess-4"),
    ]
    rows = {r["event_type"]: r for r in silver.to_processed(_raw_df(spark, records)).collect()}

    assert rows["order_placed"]["signed_net_amount"] == 100.0
    assert rows["refund_issued"]["signed_net_amount"] == -100.0
    assert rows["order_cancelled"]["signed_net_amount"] == -100.0
    assert rows["product_viewed"]["signed_net_amount"] == 0.0


def test_deduplicate_keeps_one_row_per_idempotency_key(spark):
    record = _event("order_placed")
    duplicate = dict(record)
    processed = silver.to_processed(_raw_df(spark, [record, duplicate]))

    assert processed.count() == 1


def test_deduplicate_keeps_the_latest_arrival(spark):
    early = _event("order_placed")
    early["ingested_at"] = "2026-06-24T12:00:00+00:00"
    early["product"]["name"] = "old name"

    late = _event("order_placed")
    late["ingested_at"] = "2026-06-24T13:00:00+00:00"
    late["product"]["name"] = "new name"

    row = silver.to_processed(_raw_df(spark, [early, late])).collect()[0]
    assert row["product_name"] == "new name"


def test_clean_drops_rows_without_a_product_id(spark):
    good = _event("product_viewed")
    bad = _event("product_viewed", product_id="sku-2")
    bad["product"]["product_id"] = None

    assert silver.to_processed(_raw_df(spark, [good, bad])).count() == 1


def test_clean_drops_rows_with_an_unparseable_timestamp(spark):
    bad = _event("product_viewed")
    bad["occurred_at"] = "not-a-timestamp"

    assert silver.to_processed(_raw_df(spark, [bad])).count() == 0


def test_clean_clamps_absurd_quantities_and_discounts(spark):
    record = _event("order_placed")
    record["order"]["quantity"] = 9999
    record["order"]["discount_pct"] = 500.0

    row = silver.to_processed(_raw_df(spark, [record])).collect()[0]
    assert row["quantity"] == 1
    assert row["discount_pct"] == 0.0


# ─────────────────────────────────────────────
# SESSIONS
# ─────────────────────────────────────────────

def test_build_sessions_aggregates_the_funnel_per_session(spark):
    records = [
        _event("product_viewed", sequence=1, occurred_at="2026-06-24T12:00:00+00:00"),
        _event("add_to_cart", sequence=2, occurred_at="2026-06-24T12:01:00+00:00"),
        _event("checkout_started", sequence=3, occurred_at="2026-06-24T12:02:00+00:00", order_id="ord-1"),
        _event("order_placed", sequence=4, occurred_at="2026-06-24T12:05:00+00:00", order_id="ord-1"),
    ]
    row = gold.build_sessions(silver.to_processed(_raw_df(spark, records))).collect()[0]

    assert row["session_id"] == "sess-1"
    assert row["events"] == 4
    assert row["views"] == 1 and row["cart_adds"] == 1 and row["checkouts"] == 1 and row["orders"] == 1
    assert row["converted"] is True
    assert row["bounced"] is False
    assert row["duration_seconds"] == 300
    assert row["revenue"] == 100.0
    assert row["partition_date"] == "2026-06-24"


def test_build_sessions_flags_a_bounce(spark):
    row = gold.build_sessions(
        silver.to_processed(_raw_df(spark, [_event("product_viewed")]))
    ).collect()[0]

    assert row["bounced"] is True
    assert row["converted"] is False


def test_build_sessions_over_simulated_traffic(simulated):
    sessions = gold.build_sessions(simulated)
    assert sessions.count() > 0
    for row in sessions.collect():
        assert row["events"] >= 1
        assert row["duration_seconds"] >= 0
        assert row["converted"] == (row["orders"] > 0)


# ─────────────────────────────────────────────
# FUNNEL
# ─────────────────────────────────────────────

def test_build_funnel_daily_computes_stage_rates(spark):
    records = []
    # 4 sessions view, 2 add to cart, 1 orders.
    for index in range(4):
        session = f"sess-{index}"
        records.append(_event("product_viewed", session_id=session, sequence=1))
        if index < 2:
            records.append(_event("add_to_cart", session_id=session, sequence=2))
        if index < 1:
            records.append(_event("checkout_started", session_id=session, sequence=3, order_id=f"ord-{index}"))
            records.append(_event("order_placed", session_id=session, sequence=4, order_id=f"ord-{index}"))

    row = gold.build_funnel_daily(silver.to_processed(_raw_df(spark, records))).collect()[0]

    assert row["sessions"] == 4
    assert row["viewed"] == 4 and row["carted"] == 2 and row["ordered"] == 1
    assert row["view_to_cart_pct"] == 50.0
    assert row["cart_to_checkout_pct"] == 50.0
    assert row["checkout_to_order_pct"] == 100.0
    assert row["overall_conversion_pct"] == 25.0


def test_funnel_rates_are_zero_not_null_when_a_stage_is_empty(spark):
    row = gold.build_funnel_daily(
        silver.to_processed(_raw_df(spark, [_event("product_searched")]))
    ).collect()[0]

    assert row["view_to_cart_pct"] == 0.0
    assert row["overall_conversion_pct"] == 0.0


# ─────────────────────────────────────────────
# ORDERS
# ─────────────────────────────────────────────

def test_build_orders_aggregates_line_items(spark):
    records = [
        _event("order_placed", product_id="sku-1", price=100.0, quantity=2, order_id="ord-1", sequence=1),
        _event("order_placed", product_id="sku-2", price=50.0, quantity=1, order_id="ord-1", sequence=2),
    ]
    row = gold.build_orders(silver.to_processed(_raw_df(spark, records))).collect()[0]

    assert row["order_id"] == "ord-1"
    assert row["line_items"] == 2
    assert row["units"] == 3
    assert row["net_amount"] == 250.0
    assert row["status"] == "completed"
    assert row["realized_revenue"] == 250.0


def test_build_orders_marks_a_refund(spark):
    records = [
        _event("order_placed", price=100.0, order_id="ord-1", sequence=1),
        _event("refund_issued", price=100.0, order_id="ord-1", sequence=2,
               occurred_at="2026-06-26T12:00:00+00:00"),
    ]
    row = gold.build_orders(silver.to_processed(_raw_df(spark, records))).collect()[0]

    assert row["status"] == "refunded"
    assert row["refunded"] is True
    assert row["realized_revenue"] == 0.0


def test_build_orders_marks_a_cancellation(spark):
    records = [
        _event("order_placed", price=100.0, order_id="ord-1", sequence=1),
        _event("order_cancelled", price=100.0, order_id="ord-1", sequence=2,
               occurred_at="2026-06-24T14:00:00+00:00"),
    ]
    row = gold.build_orders(silver.to_processed(_raw_df(spark, records))).collect()[0]
    assert row["status"] == "cancelled"


def test_build_orders_ignores_non_order_events(spark):
    records = [_event("product_viewed"), _event("add_to_cart", sequence=2)]
    assert gold.build_orders(silver.to_processed(_raw_df(spark, records))).count() == 0


# ─────────────────────────────────────────────
# CUSTOMER RFM
# ─────────────────────────────────────────────

def test_build_customer_rfm_scores_buyers_and_browsers(spark):
    records = [
        _event("order_placed", price=500.0, order_id="ord-1", customer_id="cust-buyer",
               session_id="sess-a", occurred_at="2026-06-24T12:00:00+00:00"),
        _event("product_viewed", price=20.0, customer_id="cust-browser", session_id="sess-b"),
    ]
    processed = silver.to_processed(_raw_df(spark, records))
    rfm = {row["customer_id"]: row for row in gold.build_customer_rfm(processed).collect()}

    assert rfm["cust-buyer"]["is_buyer"] is True
    assert rfm["cust-buyer"]["monetary"] == 500.0
    assert rfm["cust-buyer"]["rfm_segment"] is not None
    assert rfm["cust-browser"]["is_buyer"] is False
    assert rfm["cust-browser"]["sessions"] == 1


def test_rfm_recency_is_measured_from_the_batch_high_water_mark(spark):
    records = [
        _event("order_placed", price=100.0, order_id="ord-1", customer_id="cust-old",
               session_id="sess-a", occurred_at="2026-06-01T12:00:00+00:00"),
        _event("order_placed", price=100.0, order_id="ord-2", customer_id="cust-new",
               session_id="sess-b", occurred_at="2026-06-24T12:00:00+00:00"),
    ]
    processed = silver.to_processed(_raw_df(spark, records))
    rfm = {row["customer_id"]: row for row in gold.build_customer_rfm(processed).collect()}

    assert rfm["cust-new"]["recency_days"] == 0
    assert rfm["cust-old"]["recency_days"] == 23
    # A recent buyer must score higher on recency than a stale one.
    assert rfm["cust-new"]["r_score"] > rfm["cust-old"]["r_score"]


def test_rfm_over_simulated_traffic_has_no_null_segments(simulated):
    rfm = gold.build_customer_rfm(simulated)
    buyers = [row for row in rfm.collect() if row["is_buyer"]]
    assert buyers
    for row in buyers:
        assert row["rfm_segment"] in ("champion", "loyal", "potential", "at_risk", "hibernating")
        assert 3 <= row["rfm_score"] <= 15


# ─────────────────────────────────────────────
# PRODUCT PERFORMANCE
# ─────────────────────────────────────────────

def test_build_product_daily_ranks_by_revenue(spark):
    records = [
        _event("order_placed", product_id="sku-1", price=500.0, order_id="ord-1", sequence=1),
        _event("order_placed", product_id="sku-2", price=100.0, order_id="ord-2", sequence=2),
        _event("product_viewed", product_id="sku-2", price=100.0, sequence=3),
    ]
    rows = gold.build_product_daily(silver.to_processed(_raw_df(spark, records))).collect()
    ranked = {row["product_id"]: row for row in rows}

    assert ranked["sku-1"]["revenue_rank"] == 1
    assert ranked["sku-1"]["revenue"] == 500.0
    assert ranked["sku-2"]["views"] == 1
    assert ranked["sku-2"]["view_to_order_pct"] == 100.0


def test_build_product_daily_top_n_truncates(spark):
    records = [
        _event("order_placed", product_id=f"sku-{i}", price=100.0 * i, order_id=f"ord-{i}", sequence=i)
        for i in range(1, 5)
    ]
    rows = gold.build_product_daily(silver.to_processed(_raw_df(spark, records)), top_n=2).collect()

    assert len(rows) == 2
    assert {row["product_id"] for row in rows} == {"sku-4", "sku-3"}


# ─────────────────────────────────────────────
# ANOMALIES
# ─────────────────────────────────────────────

def test_build_anomalies_flags_a_high_amount(spark):
    record = _event("order_placed", price=9000.0, order_id="ord-1")
    row = gold.build_anomalies(silver.to_processed(_raw_df(spark, [record]))).collect()[0]

    assert "high_amount" in row["reasons"]


def test_build_anomalies_flags_bulk_quantity_and_extreme_discount(spark):
    record = _event("order_placed", price=100.0, quantity=50, discount_pct=90.0, order_id="ord-1")
    row = gold.build_anomalies(silver.to_processed(_raw_df(spark, [record]))).collect()[0]

    assert "bulk_quantity" in row["reasons"]
    assert "extreme_discount" in row["reasons"]
    assert row["severity"] == "high"


def test_build_anomalies_flags_an_anonymous_purchase(spark):
    record = _event("order_placed", price=100.0, order_id="ord-1")
    record["customer"]["customer_id"] = None
    row = gold.build_anomalies(silver.to_processed(_raw_df(spark, [record]))).collect()[0]

    assert "anonymous_purchase" in row["reasons"]


def test_build_anomalies_returns_nothing_for_clean_traffic(spark):
    records = [_event("product_viewed"), _event("add_to_cart", sequence=2)]
    assert gold.build_anomalies(silver.to_processed(_raw_df(spark, records))).count() == 0


def test_anomaly_threshold_is_configurable(spark):
    record = _event("order_placed", price=1000.0, order_id="ord-1")
    processed = silver.to_processed(_raw_df(spark, [record]))

    assert gold.build_anomalies(processed, amount_threshold=5000.0).count() == 0
    assert gold.build_anomalies(processed, amount_threshold=500.0).count() == 1


# ─────────────────────────────────────────────
# QUALITY SUMMARY
# ─────────────────────────────────────────────

def test_quality_summary_counts_retention_and_nulls(spark):
    good = _event("product_viewed")
    dropped = _event("product_viewed", product_id="sku-2")
    dropped["product"]["product_id"] = None

    raw = _raw_df(spark, [good, dropped])
    processed = silver.to_processed(raw)
    summary = silver.quality_summary(raw, processed)

    assert summary["raw_records"] == 2
    assert summary["processed_records"] == 1
    assert summary["dropped_records"] == 1
    assert summary["retention_pct"] == 50.0
    assert summary["duplicate_records"] == 0
    assert summary["events_by_type"] == {"product_viewed": 1}
    assert summary["partitions"] == ["2026-06-24"]
    assert summary["null_counts"]["order_id"] == 1
    assert summary["null_pct"]["order_id"] == 100.0


def test_quality_summary_of_an_empty_batch(spark):
    empty = spark.createDataFrame([], schema=silver.raw_schema())
    summary = silver.quality_summary(empty, silver.to_processed(empty))

    assert summary["raw_records"] == 0
    assert summary["retention_pct"] == 0.0
    assert summary["partitions"] == []


# ─────────────────────────────────────────────
# FILESYSTEM ROUND-TRIP
# ─────────────────────────────────────────────

def _local_fs_works(spark, tmp_path) -> bool:
    """Windows needs winutils.exe for Spark's local filesystem; probe for it."""
    try:
        spark.createDataFrame([(1,)], ["n"]).write.mode("overwrite").parquet(
            str(tmp_path / "probe").replace("\\", "/")
        )
        return True
    except Exception:
        return False


def test_read_raw_and_write_dataset_round_trip(spark, tmp_path):
    if not _local_fs_works(spark, tmp_path):
        pytest.skip("Spark local filesystem unavailable (winutils.exe missing on Windows)")

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    records = [_event("product_viewed"), _event("order_placed", order_id="ord-1", sequence=2)]
    (raw_dir / "part-0000.json").write_text(
        "\n".join(json.dumps(_raw_row(r), default=str) for r in records) + "\n", encoding="utf-8"
    )

    raw = silver.read_raw(spark, str(raw_dir).replace("\\", "/"))
    assert raw.count() == 2

    processed = silver.to_processed(raw)
    out = str(tmp_path / "processed").replace("\\", "/")
    silver.write_dataset(processed, out, partition_by=["partition_date", "partition_hour"], coalesce=1)

    assert spark.read.parquet(out).count() == 2
