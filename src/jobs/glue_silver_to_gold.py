"""Glue job 3 — **silver → gold**, the analytical layer.

    s3://<lake>/silver/events/ ──> Glue Job ──> s3://<lake>/gold/<table>/
                                                + CloudWatch

Six aggregations of behaviour come out of here: ``sessions`` ``funnel_daily``
``orders`` ``customer_rfm`` ``product_daily`` ``anomalies``. Each is one table
per question, so answering it downstream costs no join.

Self-contained on purpose: every builder below lives in this file, so the Glue
job's *Script path* is the whole story and ``--extra-py-files`` only ever
carries ``common/``.

Why it is a separate job from ``glue_bronze_to_silver``
----------------------------------------------------------
Silver is written every hour off a small delta; gold recomputes windows that
span days. Splitting them lets each run on its own schedule, its own worker
count, and be retried without redoing the other. Keeping ``GOLD_DATASETS``
unset on the silver job makes it build the event-derived tables in the same
pass instead — the right call while volumes are small.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3


try:
    from awsglue.utils import getResolvedOptions
except Exception:  # pragma: no cover - allows local runs without the Glue libs
    getResolvedOptions = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# LAKE LAYOUT
# ─────────────────────────────────────────────
# A copy of common/py, deliberately: one file is one Glue job,
# complete, and common/ belongs to the Lambdas. Nothing here may be edited on
# its own — tests/test_job_self_containment.py pins every copy against the
# original, so a divergence fails the build rather than silently writing a day
# of data into the wrong prefix.

#: The medallion zones, in flow order.
ZONES: tuple = ("landing", "bronze", "silver", "gold", "quarantine", "quality")

#: Default prefix of each zone inside ``OUTPUT_BUCKET``.
DEFAULT_ZONE_PREFIXES: dict = {
    "landing": "landing/",
    "bronze": "bronze/",
    "silver": "silver/",
    "gold": "gold/",
    "quarantine": "quarantine/",
    "quality": "quality/",
}

#: Pre-medallion config key that still defines a whole zone.
LEGACY_ZONE_KEYS: dict = {
    "gold": "CURATED_PREFIX",
}

#: Pre-medallion config keys that define a single dataset:
#: ``dataset -> (prefix key, full-path key)``.
LEGACY_DATASET_KEYS: dict = {
    "bronze/events": ("RAW_PREFIX", "RAW_S3_PATH"),
    "silver/events": ("PROCESSED_PREFIX", "PROCESSED_S3_PATH"),
    "quarantine/events": ("REJECTED_PREFIX", None),
}

#: Gold datasets and the columns they are partitioned by (``None`` = unpartitioned).
GOLD_DATASETS: dict = {
    "sessions": ["partition_date"],
    "funnel_daily": ["partition_date"],
    "orders": ["partition_date"],
    "customer_rfm": None,
    "product_daily": ["partition_date"],
    "anomalies": ["partition_date"],
}


# -------------------------------------------------
# PREFIXES
# -------------------------------------------------

def _norm(prefix: str) -> str:
    """``"/gold"`` -> ``"gold/"``; the empty prefix stays empty (bucket root)."""
    prefix = str(prefix).strip().strip("/")
    return f"{prefix}/" if prefix else ""


def zone_prefix(config: dict, zone: str) -> str:
    """Prefix of a zone: ``<ZONE>_PREFIX``, then its legacy key, then the default."""
    if zone not in DEFAULT_ZONE_PREFIXES:
        raise ValueError(f"Unknown zone '{zone}' (expected one of {list(ZONES)})")

    for key in (f"{zone.upper()}_PREFIX", LEGACY_ZONE_KEYS.get(zone)):
        if key and config.get(key) is not None:
            return _norm(config[key])
    return DEFAULT_ZONE_PREFIXES[zone]


def _split(dataset: str) -> Tuple[str, str]:
    zone, _, name = dataset.partition("/")
    return zone, name.strip("/")


def dataset_prefix(config: dict, dataset: str) -> str:
    """Prefix of a dataset such as ``bronze/events`` or ``gold/orders``.

    Precedence: the derived key (``BRONZE_EVENTS_PREFIX``), then the legacy key
    (``RAW_PREFIX``), then ``<zone prefix>/<name>/``.
    """
    zone, name = _split(dataset)
    if not name:
        return zone_prefix(config, zone)

    derived = f"{zone.upper()}_{name.upper().replace('/', '_')}_PREFIX"
    legacy = (LEGACY_DATASET_KEYS.get(f"{zone}/{name}") or (None, None))[0]
    for key in (derived, legacy):
        if key and config.get(key) is not None:
            return _norm(config[key])

    return f"{zone_prefix(config, zone)}{name}/"


# -------------------------------------------------
# PATHS
# -------------------------------------------------

def s3_path(bucket: str, prefix: str) -> str:
    return f"s3://{bucket}/{_norm(prefix)}"


def zone_path(config: dict, zone: str) -> str:
    return s3_path(config["OUTPUT_BUCKET"], zone_prefix(config, zone))


def dataset_path(config: dict, dataset: str) -> str:
    """Full ``s3://`` path of a dataset, honouring any full-path override.

    ``dataset`` may itself be an ``s3://`` URI, which is returned as-is — that
    is what lets a config point one target at another bucket entirely.
    """
    if dataset.startswith("s3://"):
        return dataset if dataset.endswith("/") else dataset + "/"

    zone, name = _split(dataset)
    derived = (
        f"{zone.upper()}_{name.upper().replace('/', '_')}_S3_PATH" if name
        else f"{zone.upper()}_S3_PATH"
    )
    legacy = (LEGACY_DATASET_KEYS.get(dataset) or (None, None))[1]
    for key in (derived, legacy):
        if key and config.get(key):
            value = str(config[key])
            return value if value.endswith("/") else value + "/"

    return s3_path(config["OUTPUT_BUCKET"], dataset_prefix(config, dataset))


def gold_path(config: dict, name: str) -> str:
    return dataset_path(config, f"gold/{name}")


def build_paths(config: dict) -> dict:
    """Every path a job might need, resolved once.

    Legacy aliases (``raw``/``processed``/``curated``/``rejected``) are kept so
    older call sites keep reading the same keys.
    """
    bucket = config["OUTPUT_BUCKET"]
    paths = {"bucket": bucket}
    paths.update({zone: zone_path(config, zone) for zone in ZONES})
    paths.update({
        "bronze_events": dataset_path(config, "bronze/events"),
        "silver_events": dataset_path(config, "silver/events"),
        "quarantine_events": dataset_path(config, "quarantine/events"),
    })
    paths.update({
        "raw": paths["bronze_events"],
        "processed": paths["silver_events"],
        "curated": paths["gold"],
        "rejected": paths["quarantine_events"],
    })
    return paths


def resolve_gold_datasets(config: dict) -> List[str]:
    """Which gold tables to build. An explicit empty list means *none*.

    ``None``/absent means "all of them" — an empty list has to mean something
    different, otherwise the silver job could never hand the gold layer over to
    the gold job.
    """
    requested = config.get("GOLD_DATASETS", config.get("CURATED_DATASETS"))
    if requested is None:
        return list(GOLD_DATASETS)

    unknown = [name for name in requested if name not in GOLD_DATASETS]
    if unknown:
        raise ValueError(f"Unknown gold dataset(s): {unknown}")
    return list(requested)


# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────

def as_bool(value: Any, default: bool = False) -> bool:
    """Read a boolean that may have arrived as a string.

    A config file written by Terraform, or built from environment variables,
    carries ``"true"`` and ``"false"`` as strings — and ``bool("false")`` is
    ``True``, which silently turns a kill switch into an on switch. Anything
    that is already a bool passes through.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() not in ("", "false", "0", "no", "off", "none")

class JobMetrics:
    """CloudWatch counters for this job.

    Never raises and never requires AWS: an observability failure must not fail
    a data run, and ``METRICS_ENABLED: false`` silences it entirely.
    """

    MAX_ITEMS_PER_CALL = 20

    def __init__(self, namespace: str, dimensions: dict, enabled: bool, client=None) -> None:
        self.namespace = namespace
        self.dimensions = {k: str(v) for k, v in (dimensions or {}).items() if v is not None}
        self.enabled = bool(enabled and namespace)
        self._client = client
        self._buffer: list = []

    @classmethod
    def from_config(cls, config: dict, stage: str, client=None) -> "JobMetrics":
        config = config or {}
        dimensions = {"Stage": stage, **(config.get("METRICS_DIMENSIONS") or {})}
        if config.get("ENVIRONMENT"):
            dimensions.setdefault("Environment", config["ENVIRONMENT"])

        # The env var is the operator's kill switch — it silences metrics in a
        # sandbox without editing every config file.
        env_default = os.environ.get("METRICS_ENABLED", "true").strip().lower() not in ("false", "0", "no")

        return cls(
            namespace=config.get("METRICS_NAMESPACE", "Ecommerce/Pipeline"),
            dimensions=dimensions,
            enabled=as_bool(config.get("METRICS_ENABLED"), env_default),
            client=client,
        )

    def _put(self, name: str, value: float, unit: str, dimensions: dict) -> None:
        if not self.enabled:
            return
        merged = {**self.dimensions, **{k: str(v) for k, v in (dimensions or {}).items() if v is not None}}
        self._buffer.append({
            "MetricName": name,
            "Value": float(value),
            "Unit": unit,
            "Dimensions": [{"Name": k, "Value": v} for k, v in merged.items()],
        })

    def count(self, name: str, value: float = 1, **dimensions: str) -> None:
        self._put(name, value, "Count", dimensions)

    def gauge(self, name: str, value: float, unit: str = "None", **dimensions: str) -> None:
        self._put(name, value, unit, dimensions)

    def flush(self) -> int:
        """Ship the buffer. Returns how many metrics were accepted."""
        if not self.enabled or not self._buffer:
            self._buffer.clear()
            return 0

        pending, self._buffer = self._buffer, []
        try:
            client = self._client or boto3.client("cloudwatch")
        except Exception as exc:  # noqa: BLE001 - observability must not break the run
            logger.warning("CloudWatch unavailable, dropping %d metrics: %s", len(pending), exc)
            return 0

        sent = 0
        for offset in range(0, len(pending), self.MAX_ITEMS_PER_CALL):
            chunk = pending[offset:offset + self.MAX_ITEMS_PER_CALL]
            try:
                client.put_metric_data(Namespace=self.namespace, MetricData=chunk)
                sent += len(chunk)
            except Exception as exc:  # noqa: BLE001
                logger.warning("put_metric_data failed for %d metrics: %s", len(chunk), exc)
        return sent


# ─────────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────────
# Guarded: the pure-Python engine and the unit tests import this module on
# machines with no PySpark. Every Spark object below is built inside a function,
# so an absent import only matters once a Spark path is actually taken.
try:  # pragma: no cover - Spark is absent in the unit tests
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except Exception:  # pragma: no cover
    DataFrame = SparkSession = F = Window = None

# Declared here rather than shared: one file is one Glue job, complete.

def load_config(path: str, s3: Any = None) -> dict:
    """Read a job's JSON config, from S3 or from disk."""
    logger.info("Loading config from %s", path)
    if path.startswith("s3://"):
        bucket, _, key = path[len("s3://"):].partition("/")
        s3 = s3 or boto3.client("s3")
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))

    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


REVENUE_EVENTS = ["order_placed"]
NEGATIVE_REVENUE_EVENTS = ["order_cancelled", "refund_issued"]

# ─────────────────────────────────────────────
# CURATED: SESSIONS
# ─────────────────────────────────────────────

def build_sessions(processed: DataFrame) -> DataFrame:
    """One row per browsing session — the unit users actually behave in."""
    def stage_count(stage: str):
        return F.sum(F.when(F.col("event_type") == stage, 1).otherwise(0))

    sessions = (
        processed
        .filter(F.col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(
            F.first("customer_id", ignorenulls=True).alias("customer_id"),
            F.first("channel", ignorenulls=True).alias("channel"),
            F.first("device_type", ignorenulls=True).alias("device_type"),
            F.first("customer_country", ignorenulls=True).alias("country"),
            F.first("campaign", ignorenulls=True).alias("campaign"),
            F.first("utm_source", ignorenulls=True).alias("utm_source"),
            F.min("occurred_ts").alias("session_start"),
            F.max("occurred_ts").alias("session_end"),
            F.count("*").alias("events"),
            F.countDistinct("product_id").alias("distinct_products"),
            stage_count("product_viewed").alias("views"),
            stage_count("add_to_cart").alias("cart_adds"),
            stage_count("remove_from_cart").alias("cart_removals"),
            stage_count("checkout_started").alias("checkouts"),
            stage_count("payment_failed").alias("payment_failures"),
            stage_count("order_placed").alias("orders"),
            F.sum("signed_net_amount").alias("revenue"),
        )
        .withColumn(
            "duration_seconds",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
        .withColumn("converted", F.col("orders") > 0)
        .withColumn("bounced", (F.col("events") == 1) & (F.col("views") == 1))
        .withColumn("partition_date", F.date_format(F.col("session_start"), "yyyy-MM-dd"))
    )
    return sessions


# ─────────────────────────────────────────────
# CURATED: FUNNEL
# ─────────────────────────────────────────────

def build_funnel_daily(processed: DataFrame) -> DataFrame:
    """Daily funnel by channel, counted in *sessions* rather than events.

    Counting distinct sessions per stage is the honest denominator: an event
    count would let one indecisive user with five cart adds inflate the
    view→cart rate.
    """
    def sessions_at(stage: str):
        return F.countDistinct(F.when(F.col("event_type") == stage, F.col("session_id")))

    def rate(numerator: str, denominator: str):
        return F.round(
            F.when(F.col(denominator) > 0, 100.0 * F.col(numerator) / F.col(denominator)).otherwise(F.lit(0.0)),
            2,
        )

    return (
        processed
        .filter(F.col("session_id").isNotNull())
        .groupBy("partition_date", "channel")
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("customer_id").alias("customers"),
            sessions_at("product_viewed").alias("viewed"),
            sessions_at("add_to_cart").alias("carted"),
            sessions_at("checkout_started").alias("checked_out"),
            sessions_at("order_placed").alias("ordered"),
            F.sum("signed_net_amount").alias("revenue"),
        )
        .withColumn("view_to_cart_pct", rate("carted", "viewed"))
        .withColumn("cart_to_checkout_pct", rate("checked_out", "carted"))
        .withColumn("checkout_to_order_pct", rate("ordered", "checked_out"))
        .withColumn("overall_conversion_pct", rate("ordered", "sessions"))
        .withColumn(
            "revenue_per_session",
            F.round(F.when(F.col("sessions") > 0, F.col("revenue") / F.col("sessions")).otherwise(F.lit(0.0)), 2),
        )
    )


# ─────────────────────────────────────────────
# CURATED: ORDERS
# ─────────────────────────────────────────────

def build_orders(processed: DataFrame) -> DataFrame:
    """One row per order, aggregating its line items and final status."""
    order_events = processed.filter(
        F.col("order_id").isNotNull()
        & F.col("event_type").isin(REVENUE_EVENTS + NEGATIVE_REVENUE_EVENTS)
    )

    placed = (
        order_events.filter(F.col("event_type") == "order_placed")
        .groupBy("order_id")
        .agg(
            F.first("customer_id", ignorenulls=True).alias("customer_id"),
            F.first("session_id", ignorenulls=True).alias("session_id"),
            F.first("channel", ignorenulls=True).alias("channel"),
            F.first("device_type", ignorenulls=True).alias("device_type"),
            F.first("customer_country", ignorenulls=True).alias("country"),
            F.first("currency", ignorenulls=True).alias("currency"),
            F.first("payment_method", ignorenulls=True).alias("payment_method"),
            F.first("campaign", ignorenulls=True).alias("campaign"),
            F.min("occurred_ts").alias("ordered_at"),
            F.countDistinct("product_id").alias("line_items"),
            F.sum("quantity").alias("units"),
            F.sum(F.coalesce(F.col("gross_amount"), F.lit(0.0))).alias("gross_amount"),
            F.sum(F.coalesce(F.col("discount_amount"), F.lit(0.0))).alias("discount_amount"),
            F.sum(F.coalesce(F.col("net_amount"), F.lit(0.0))).alias("net_amount"),
        )
    )

    reversals = (
        order_events.filter(F.col("event_type").isin(NEGATIVE_REVENUE_EVENTS))
        .groupBy("order_id")
        .agg(
            F.max(F.when(F.col("event_type") == "order_cancelled", True).otherwise(False)).alias("cancelled"),
            F.max(F.when(F.col("event_type") == "refund_issued", True).otherwise(False)).alias("refunded"),
            F.sum(F.coalesce(F.col("net_amount"), F.lit(0.0))).alias("reversed_amount"),
        )
    )

    return (
        placed.join(reversals, on="order_id", how="left")
        .withColumn("cancelled", F.coalesce(F.col("cancelled"), F.lit(False)))
        .withColumn("refunded", F.coalesce(F.col("refunded"), F.lit(False)))
        .withColumn("reversed_amount", F.coalesce(F.col("reversed_amount"), F.lit(0.0)))
        .withColumn(
            "status",
            F.when(F.col("cancelled"), F.lit("cancelled"))
             .when(F.col("refunded"), F.lit("refunded"))
             .otherwise(F.lit("completed")),
        )
        .withColumn("realized_revenue", F.round(F.col("net_amount") - F.col("reversed_amount"), 2))
        .withColumn(
            "avg_item_value",
            F.round(F.when(F.col("units") > 0, F.col("net_amount") / F.col("units")).otherwise(F.lit(0.0)), 2),
        )
        .withColumn("partition_date", F.date_format(F.col("ordered_at"), "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────
# CURATED: CUSTOMER RFM
# ─────────────────────────────────────────────

def build_customer_rfm(processed: DataFrame, orders: Optional[DataFrame] = None) -> DataFrame:
    """Recency / Frequency / Monetary scores and the segment they imply.

    Scores are quintiles (``ntile(5)``) computed over the batch, so they stay
    meaningful whatever the absolute scale of the data. Recency is inverted —
    a recent buyer must score 5, not 1.
    """
    orders = orders if orders is not None else build_orders(processed)
    completed = orders.filter(F.col("status") != "cancelled")

    as_of = processed.select(F.max("occurred_ts").alias("m")).collect()[0]["m"]

    base = (
        completed
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("orders"),
            F.sum("realized_revenue").alias("monetary"),
            F.min("ordered_at").alias("first_order_at"),
            F.max("ordered_at").alias("last_order_at"),
            F.sum("units").alias("units"),
            F.avg("realized_revenue").alias("avg_order_value"),
            F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)).alias("refunded_orders"),
        )
        .filter(F.col("customer_id").isNotNull())
        .withColumn("recency_days", F.datediff(F.lit(as_of).cast("timestamp"), F.col("last_order_at")))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .withColumn("monetary", F.round(F.col("monetary"), 2))
        .withColumn(
            "refund_rate_pct",
            F.round(100.0 * F.col("refunded_orders") / F.col("orders"), 2),
        )
    )

    # Engagement counted over *all* events, so a browser who never bought still
    # gets a row's worth of context next to the buyers.
    engagement = (
        processed.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.count("*").alias("events"),
            F.first("customer_segment", ignorenulls=True).alias("declared_segment"),
            F.first("customer_country", ignorenulls=True).alias("country"),
        )
    )

    scored = (
        base
        .withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recency_days").desc())))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.col("orders").asc())))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy(F.col("monetary").asc())))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        .withColumn(
            "rfm_segment",
            F.when(F.col("rfm_score") >= 13, F.lit("champion"))
             .when(F.col("rfm_score") >= 10, F.lit("loyal"))
             .when(F.col("rfm_score") >= 7, F.lit("potential"))
             .when(F.col("r_score") <= 2, F.lit("at_risk"))
             .otherwise(F.lit("hibernating")),
        )
    )

    return engagement.join(scored, on="customer_id", how="left").withColumn(
        "is_buyer", F.coalesce(F.col("orders"), F.lit(0)) > 0
    )


# ─────────────────────────────────────────────
# CURATED: PRODUCT PERFORMANCE
# ─────────────────────────────────────────────

def build_product_daily(processed: DataFrame, top_n: int = 0) -> DataFrame:
    """Daily product performance with a revenue rank per day.

    ``top_n > 0`` keeps only that many products per day — say so explicitly in
    the job output when you use it, so nobody reads a truncated table as
    complete.
    """
    def stage_count(stage: str):
        return F.sum(F.when(F.col("event_type") == stage, 1).otherwise(0))

    daily = (
        processed
        .groupBy("partition_date", "product_id")
        .agg(
            F.first("product_name", ignorenulls=True).alias("product_name"),
            F.first("category", ignorenulls=True).alias("category"),
            F.first("brand", ignorenulls=True).alias("brand"),
            F.first("price_category", ignorenulls=True).alias("price_category"),
            F.avg("product_price").alias("avg_price"),
            stage_count("product_viewed").alias("views"),
            stage_count("add_to_cart").alias("cart_adds"),
            stage_count("order_placed").alias("orders"),
            F.sum(F.when(F.col("event_type") == "order_placed", F.col("quantity")).otherwise(0)).alias("units_sold"),
            F.sum("signed_net_amount").alias("revenue"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .withColumn("avg_price", F.round(F.col("avg_price"), 2))
        .withColumn("revenue", F.round(F.col("revenue"), 2))
        .withColumn(
            "view_to_order_pct",
            F.round(F.when(F.col("views") > 0, 100.0 * F.col("orders") / F.col("views")).otherwise(F.lit(0.0)), 2),
        )
        .withColumn(
            "revenue_rank",
            F.row_number().over(Window.partitionBy("partition_date").orderBy(F.col("revenue").desc())),
        )
    )

    return daily.filter(F.col("revenue_rank") <= top_n) if top_n and top_n > 0 else daily


# ─────────────────────────────────────────────
# CURATED: ANOMALIES
# ─────────────────────────────────────────────

def build_anomalies(processed: DataFrame, amount_threshold: float = 5000.0) -> DataFrame:
    """Flag events worth a human look, with the reason attached.

    Deliberately rule-based and explainable rather than a model: at this volume
    an analyst needs to know *why* a row was flagged before trusting it.
    """
    session_orders = Window.partitionBy("session_id").orderBy("occurred_ts")

    flagged = (
        processed
        .withColumn(
            "_prev_order_ts",
            F.lag("occurred_ts").over(session_orders),
        )
        .withColumn(
            "reasons",
            F.array_remove(
                F.array(
                    F.when(F.col("net_amount") > amount_threshold, F.lit("high_amount")).otherwise(F.lit("")),
                    F.when(F.col("quantity") >= 20, F.lit("bulk_quantity")).otherwise(F.lit("")),
                    F.when(F.col("discount_pct") >= 80, F.lit("extreme_discount")).otherwise(F.lit("")),
                    F.when(F.col("net_amount") < 0, F.lit("negative_amount")).otherwise(F.lit("")),
                    F.when(F.col("customer_id").isNull(), F.lit("anonymous_purchase"))
                     .otherwise(F.lit("")),
                    F.when(
                        (F.col("event_type") == "order_placed")
                        & (F.col("occurred_ts").cast("long") - F.col("_prev_order_ts").cast("long") < 5),
                        F.lit("rapid_fire"),
                    ).otherwise(F.lit("")),
                    F.when(F.col("product_price").isNull(), F.lit("missing_price")).otherwise(F.lit("")),
                ),
                "",
            ),
        )
        .filter(F.size(F.col("reasons")) > 0)
        .withColumn("severity", F.when(F.size(F.col("reasons")) >= 2, F.lit("high")).otherwise(F.lit("low")))
        .drop("_prev_order_ts")
    )

    return flagged.select(
        "partition_date", "occurred_ts", "event_type", "idempotency_key",
        "session_id", "customer_id", "product_id", "order_id",
        "quantity", "discount_pct", "net_amount", "channel",
        "reasons", "severity",
    )

def plan(config: Dict[str, Any]) -> List[str]:
    """Which gold tables this run will build.

    Pure function: what a run *would* do, without a cluster. That is what makes
    a scheduling mistake visible in a unit test instead of in a 40-minute job.
    """
    return resolve_gold_datasets(config)


def run(config: Dict[str, Any], spark: Any = None) -> Dict[str, Any]:
    started = datetime.now(timezone.utc)
    paths = build_paths(config)
    wanted = plan(config)
    metrics = JobMetrics.from_config(config, stage="glue_silver_to_gold")

    spark = spark or SparkSession.builder.appName(config.get("JOB_NAME", "silver-to-gold")).getOrCreate()

    logger.info("Reading silver from %s", paths["silver_events"])
    silver = spark.read.parquet(paths["silver_events"])

    process_date = config.get("PROCESS_DATE")
    lookback_days = int(config.get("GOLD_LOOKBACK_DAYS", 0))
    if process_date and lookback_days > 0:
        # Sessions and RFM are window functions: recomputing only the current
        # day would truncate every window that started yesterday.
        floor_date = F.date_sub(F.to_date(F.lit(process_date)), lookback_days)
        silver = silver.filter(F.to_date(F.col("partition_date")) >= floor_date)
    elif process_date:
        silver = silver.filter(F.col("partition_date") == process_date)

    silver = silver.cache()

    write_mode = config.get("WRITE_MODE", "overwrite")
    coalesce = int(config.get("COALESCE", 4))
    outputs: Dict[str, str] = {}
    row_counts: Dict[str, int] = {}

    def emit(name: str, dataframe) -> None:
        path = gold_path(config, name)
        # Coalesced on the way out, against the small-files problem.
        writer = (dataframe.coalesce(coalesce) if coalesce > 0 else dataframe).write.mode(write_mode)
        partitions = GOLD_DATASETS.get(name)
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.parquet(path)
        outputs[name] = path
        row_counts[name] = dataframe.count()
        logger.info("Gold %s -> %s (%d rows)", name, path, row_counts[name])

    orders_df = None
    if {"orders", "customer_rfm"} & set(wanted):
        orders_df = build_orders(silver).cache()

    rfm_df: Optional[Any] = None
    product_daily_df: Optional[Any] = None

    if "sessions" in wanted:
        emit("sessions", build_sessions(silver))
    if "funnel_daily" in wanted:
        emit("funnel_daily", build_funnel_daily(silver))
    if "orders" in wanted:
        emit("orders", orders_df)
    if "customer_rfm" in wanted:
        rfm_df = build_customer_rfm(silver, orders=orders_df).cache()
        emit("customer_rfm", rfm_df)
    if "product_daily" in wanted:
        top_n = int(config.get("TOP_N_PRODUCTS", 0))
        if top_n:
            logger.warning("product_daily truncated to the top %d products per day", top_n)
        product_daily_df = build_product_daily(silver, top_n=top_n).cache()
        emit("product_daily", product_daily_df)
    if "anomalies" in wanted:
        emit("anomalies", build_anomalies(
            silver, amount_threshold=float(config.get("ANOMALY_AMOUNT_THRESHOLD", 5000.0))
        ))

    for frame in (silver, orders_df, rfm_df, product_daily_df):
        if frame is not None:
            frame.unpersist()

    total_rows = sum(row_counts.values())
    metrics.count("GoldRowsWritten", total_rows)
    metrics.count("GoldTablesWritten", len(outputs))
    metrics.gauge("JobDurationSeconds", (datetime.now(timezone.utc) - started).total_seconds(), unit="Seconds")
    for name, count in row_counts.items():
        metrics.count("GoldRowsByTable", count, Dataset=name)
    metrics.flush()

    return {
        "status": "success",
        "process_date": process_date,
        "outputs": outputs,
        "row_counts": row_counts,
        "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }


def main(argv=None) -> Dict[str, Any]:
    args = getResolvedOptions(argv if argv is not None else sys.argv, ["JOB_NAME", "CONFIG_PATH"])
    config = load_config(args["CONFIG_PATH"])
    # The Glue argument wins: it is the job's real identity, and one shared
    # config file must not make all four jobs report the same name.
    if args.get("JOB_NAME"):
        config["JOB_NAME"] = args["JOB_NAME"]
    return run(config)


if __name__ == "__main__":
    print(json.dumps(main(), indent=2, default=str))
