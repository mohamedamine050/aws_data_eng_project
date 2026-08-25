"""Glue job 2 — **bronze → silver**.

    S3 bronze/events/ ──> Glue Job ──┬──> S3 silver/events/  (the fact table)
                                     └──> S3 quality/        (report) + CloudWatch

Self-contained on purpose: everything this job runs is in this file, so the
Glue job's *Script path* is the whole story and ``--extra-py-files`` only ever
carries ``common/``. The gold layer is built by ``glue_silver_to_gold``, which
holds its own copy of the aggregation code for the same reason.

Two engines
-----------
**Spark** (``ENGINE: "spark"``, the default under Glue) — the real one. Every
transformation is a DataFrame operation, so the job parallelises across the
cluster instead of looping in the driver.

**Python** (``ENGINE: "python"``) — a dependency-free path that reads NDJSON with
``boto3`` and processes in-process. Kept because it is what makes local
development, CI and the unit tests possible without a Spark install. It produces
the ``processed`` layer only.

IMPORTANT (Glue packaging)
  Ne pas embarquer boto3/botocore dans ``--extra-py-files`` (dependencies.zip).
  Glue fournit déjà une version complète et fonctionnelle de boto3/botocore.
  Si dependencies.zip contient boto3/botocore, cela écrase la version native
  et provoque des erreurs du type: DataNotFoundError: Unable to load data for: endpoints
"""

import sys
import json
import logging
import os
import boto3

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from awsglue.utils import getResolvedOptions
except Exception:  # pragma: no cover - allows local runs without the Glue libs
    getResolvedOptions = None


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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
# AWS CLIENT
# ─────────────────────────────────────────────

try:
    s3 = boto3.client("s3")
except Exception as e:
    logger.error(f"Impossible d'initialiser le client boto3 s3: {e}")
    logger.error(
        "Vérifiez que dependencies.zip (--extra-py-files) n'embarque pas "
        "boto3/botocore. Glue fournit déjà ces librairies nativement."
    )
    raise

# ─────────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────────
# Guarded: the pure-Python engine and the unit tests import this module on
# machines with no PySpark. Every Spark object below is built inside a function,
# so an absent import only matters once a Spark path is actually taken.
try:  # pragma: no cover - Spark is absent in the unit tests
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    from pyspark.sql.window import Window
except Exception:  # pragma: no cover
    DataFrame = SparkSession = F = Window = None
    BooleanType = DoubleType = IntegerType = StringType = StructField = StructType = None

# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────

#: Columns of the ``processed`` fact table, in order. The RDS loader and the
#: curated builders both read this list, so adding a column is a one-line change.
PROCESSED_COLUMNS: List[str] = [
    "idempotency_key", "event_id", "event_type",
    "occurred_ts", "occurred_at", "ingested_at",
    "channel", "device_type", "device_os",
    "session_id", "event_sequence",
    "product_id", "sku", "product_name", "category", "brand", "product_price",
    "customer_id", "customer_segment", "customer_country", "is_returning",
    "order_id", "quantity", "unit_price", "discount_pct",
    "gross_amount", "discount_amount", "net_amount", "signed_net_amount",
    "currency", "payment_method",
    "campaign", "utm_source", "utm_medium",
    "price_category", "is_revenue_event", "is_conversion", "day_of_week", "is_weekend",
    "partition_date", "partition_hour",
]

REVENUE_EVENTS = ["order_placed"]
NEGATIVE_REVENUE_EVENTS = ["order_cancelled", "refund_issued"]
FUNNEL_STAGES = ["product_viewed", "add_to_cart", "checkout_started", "order_placed"]


# ─────────────────────────────────────────────
# READ
# ─────────────────────────────────────────────

#: How Spark reports a prefix that no job has written to yet. Matched on the
#: message because the exception class differs between Spark builds.
_MISSING_PATH_MARKERS = ("PATH_NOT_FOUND", "Path does not exist", "does not exist")


def _is_missing_path(exc: Exception) -> bool:
    return any(marker in str(exc) for marker in _MISSING_PATH_MARKERS)


def _has_objects(s3_uri: str, client=None) -> bool:
    """Does anything exist under this ``s3://`` prefix?

    Asked before Spark reads, because ``spark.read`` is lazy: the missing path
    can surface at the read, at the first count, or at the write, and only one
    of those is inside a try block anyone thought to write.
    """
    if not s3_uri.startswith("s3://"):
        return True  # a local path in the tests — let Spark decide

    bucket, _, prefix = s3_uri[len("s3://"):].partition("/")
    client = client or s3
    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    except Exception as exc:  # noqa: BLE001 - a listing failure is not an empty lake
        logger.warning("Could not check %s (%s) — letting Spark try", s3_uri, exc)
        return True
    return response.get("KeyCount", 0) > 0


def _key_of(s3_uri: str) -> str:
    """``s3://bucket/a/b/`` -> ``a/b/``."""
    if not s3_uri.startswith("s3://"):
        return s3_uri
    _bucket, _, key = s3_uri[len("s3://"):].partition("/")
    return key


def sample_keys(bucket: str, prefix: str, client=None, limit: int = 10) -> List[str]:
    """A handful of the keys under ``prefix`` — for diagnostics, not for work.

    Never raises: this runs on a path the job is already reporting a problem
    about, and losing the real message to a listing error would be perverse.
    """
    client = client or s3
    keys: List[str] = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])
                if len(keys) >= limit:
                    return keys
    except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the real error
        logger.debug("Could not list %s for diagnostics: %s", prefix, exc)
    return keys


def _nothing_to_process(config: dict, paths: dict, metrics, started, detail: str) -> dict:
    """Bronze has never been written to — say so instead of raising Spark's error.

    ``[PATH_NOT_FOUND] Path does not exist: s3://…/bronze/events`` is true but
    unhelpful: the fault is upstream, in the job or the Lambda that should have
    filled bronze. Name that, and let the config decide whether an empty lake
    stops the chain.
    """
    message = (
        f"Nothing to process: {paths['bronze_events']} does not exist yet, so "
        f"{paths['silver_events']} was not written. Bronze is filled by the "
        f"glue_landing_ingest job (partner files) and by the stream_processor "
        f"Lambda (the queue) — check that one of them has run and actually wrote "
        f"records. Underlying error: {detail}"
    )

    # Nearly always the objects are in the bucket, one prefix off: dropped at
    # `bronze/` instead of `bronze/events/`. Show what is actually up there, so
    # the log answers the next question rather than prompting it.
    nearby = sample_keys(paths["bucket"], zone_prefix(config, "bronze"))
    strays = [key for key in nearby if not key.startswith(_key_of(paths["bronze_events"]))]
    if strays:
        message += (
            f"\nFound under s3://{paths['bucket']}/{zone_prefix(config, 'bronze')} but "
            f"outside the prefix this job reads: {', '.join(strays)}"
        )

    metrics.count("RecordsProcessed", 0)
    metrics.flush()

    if as_bool(config.get("FAIL_ON_EMPTY_BRONZE")):
        raise ValueError(message)

    logger.warning(message)
    return {
        "status": "success",
        "records": 0,
        "reason": "bronze is empty",
        "bronze_path": paths["bronze_events"],
        "outputs": {},
        "row_counts": {"processed": 0},
        "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }


def _read_nothing(config: dict, paths: dict, metrics, started, raw_count: int) -> dict:
    """Bronze holds objects, but not one readable event.

    Always a shape mismatch. PERMISSIVE mode turns it into empty or all-NULL
    rows rather than an error — the right call for one bad line, a silent
    disaster for a whole prefix. Measured on Spark 4, from the same file:

        NDJSON, one event per line   -> N rows, N usable   (the contract)
        JSON array on a single line  -> N rows, N usable   (tolerated)
        pretty-printed JSON          -> one row per LINE, none usable
        CSV                          -> rows, none usable
        anything else                -> rows, none usable

    So a non-zero row count proves nothing; only a non-NULL ``event_type`` does.
    """
    seen = (
        "the prefix holds no readable object"
        if raw_count == 0
        else f"the prefix parsed into {raw_count} row(s), none of which carry an event_type"
    )
    message = "\n".join([
        f"No usable event in {paths['bronze_events']} — {seen}, so "
        f"{paths['silver_events']} was not written.",
        "Bronze must be NDJSON: one complete JSON event per line, with the nested "
        "product/session/customer blocks. Pretty-printed JSON gives one row per line "
        "of formatting; CSV and Parquet give rows with every field NULL. None of them "
        "raise an error.",
        f"Check with: aws s3 ls {paths['bronze_events']} --recursive, then download one "
        "object and confirm its first line is a complete JSON event.",
    ])
    metrics.count("RecordsProcessed", 0)
    metrics.flush()

    if as_bool(config.get("FAIL_ON_EMPTY_BRONZE")):
        raise ValueError(message)

    logger.warning(message)
    return {
        "status": "success",
        "records": 0,
        "reason": "bronze parsed to zero rows",
        "bronze_path": paths["bronze_events"],
        "outputs": {},
        "row_counts": {"raw": 0, "processed": 0},
        "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }


def _kept_nothing(config: dict, paths: dict, metrics, started, raw_count: int, process_date) -> dict:
    """Rows were read, none survived. Name the filter that ate them."""
    cause = (
        f"PROCESS_DATE={process_date} matched none of them"
        if process_date
        else "they were all dropped by cleaning or deduplication"
    )
    message = (
        f"Read {raw_count} row(s) from {paths['bronze_events']} but kept 0: {cause}. "
        f"{paths['silver_events']} was not written."
    )
    metrics.count("RecordsRead", raw_count)
    metrics.count("RecordsProcessed", 0)
    metrics.flush()

    if as_bool(config.get("FAIL_ON_EMPTY_BRONZE")):
        raise ValueError(message)

    logger.warning(message)
    return {
        "status": "success",
        "records": 0,
        "reason": "nothing survived processing",
        "bronze_path": paths["bronze_events"],
        "outputs": {},
        "row_counts": {"raw": raw_count, "processed": 0},
        "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }


def read_raw(spark: SparkSession, path: str, schema: Optional["StructType"] = None) -> DataFrame:
    """Read the NDJSON raw zone.

    ``mode=PERMISSIVE`` with ``_corrupt_record`` dropped: a single malformed line
    in a partition must not fail the whole job — the count difference shows up
    in the quality report instead.
    """
    return (
        spark.read
        .schema(schema or v3_schema())
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .json(path)
    )


# The four helpers below are declared in every job that needs them: one file is
# one Glue job, complete. The v3 schema in particular is the contract between
# the job that writes bronze and the job that reads it — keep the two copies in
# step.

def v3_schema() -> "StructType":
    """The v3 event record, as Spark sees it.

    Built on call rather than at import, so a module that only needs the path
    helpers can import this one without PySpark installed.

    Declaring the schema — instead of letting Spark infer it — removes a full
    scan of the input and, more importantly, means a field the producer stops
    sending shows up as NULL rather than silently changing the shape of every
    downstream table.
    """
    return StructType([
        StructField("schema_version", StringType()),
        StructField("event_id", StringType()),
        StructField("idempotency_key", StringType()),
        StructField("ingested_at", StringType()),
        StructField("occurred_at", StringType()),
        StructField("channel", StringType()),
        StructField("event_type", StringType()),
        StructField("session", StructType([
            StructField("session_id", StringType()),
            StructField("sequence", IntegerType()),
        ])),
        StructField("device", StructType([
            StructField("type", StringType()),
            StructField("os", StringType()),
            StructField("user_agent", StringType()),
        ])),
        StructField("geo", StructType([
            StructField("country", StringType()),
            StructField("city", StringType()),
        ])),
        StructField("product", StructType([
            StructField("product_id", StringType()),
            StructField("sku", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("brand", StringType()),
            StructField("price", DoubleType()),
        ])),
        StructField("customer", StructType([
            StructField("customer_id", StringType()),
            StructField("segment", StringType()),
            StructField("country", StringType()),
            StructField("is_returning", BooleanType()),
        ])),
        StructField("order", StructType([
            StructField("order_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("discount_pct", DoubleType()),
            StructField("gross_amount", DoubleType()),
            StructField("discount_amount", DoubleType()),
            StructField("net_amount", DoubleType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType()),
            StructField("payment_method", StringType()),
        ])),
        StructField("marketing", StructType([
            StructField("campaign", StringType()),
            StructField("source", StringType()),
            StructField("medium", StringType()),
        ])),
        StructField("_meta", StructType([
            StructField("processed_at", StringType()),
            StructField("source", StringType()),
            StructField("message_id", StringType()),
            StructField("source_object", StringType()),
        ])),
    ])


#: Timestamps we accept. Anything else becomes NULL rather than an exception.
ISO_TIMESTAMP_RE = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"


def parse_timestamp(column):
    """Parse an ISO-8601 string, yielding NULL on garbage.

    Spark runs ANSI mode by default from 4.0, where a bare ``to_timestamp`` on a
    malformed string *raises* — one unparseable date in a partner export would
    abort a whole job instead of quarantining one row. Screening with a regex
    first keeps the NULL-on-bad-input behaviour on every Spark version, and
    documents the accepted format in one place.
    """
    normalized = F.regexp_replace(column, "Z$", "+00:00")
    return F.to_timestamp(F.when(normalized.rlike(ISO_TIMESTAMP_RE), normalized))


# ─────────────────────────────────────────────
# PROCESSED FACT TABLE
# ─────────────────────────────────────────────

def _price_category(price_col) -> Any:
    return (
        F.when(price_col.isNull(), F.lit("unknown"))
        .when(price_col < 50, F.lit("budget"))
        .when(price_col < 200, F.lit("mid"))
        .otherwise(F.lit("premium"))
    )


def flatten(df: DataFrame) -> DataFrame:
    """Flatten the nested v3 record into typed top-level columns."""
    occurred_ts = parse_timestamp(F.col("occurred_at"))
    net = F.coalesce(F.col("order.net_amount"), F.col("order.amount"))
    price = F.coalesce(F.col("order.unit_price"), F.col("product.price"))

    return df.select(
        F.col("idempotency_key"),
        F.col("event_id"),
        F.coalesce(F.col("event_type"), F.lit("unknown")).alias("event_type"),
        occurred_ts.alias("occurred_ts"),
        F.col("occurred_at"),
        F.col("ingested_at"),
        F.coalesce(F.col("channel"), F.lit("unknown")).alias("channel"),
        F.coalesce(F.col("device.type"), F.lit("unknown")).alias("device_type"),
        F.col("device.os").alias("device_os"),
        F.col("session.session_id").alias("session_id"),
        F.col("session.sequence").alias("event_sequence"),
        F.col("product.product_id").alias("product_id"),
        F.col("product.sku").alias("sku"),
        F.trim(F.col("product.name")).alias("product_name"),
        F.col("product.category").alias("category"),
        F.col("product.brand").alias("brand"),
        price.alias("product_price"),
        F.col("customer.customer_id").alias("customer_id"),
        F.coalesce(F.col("customer.segment"), F.lit("unknown")).alias("customer_segment"),
        F.coalesce(F.col("customer.country"), F.col("geo.country")).alias("customer_country"),
        F.coalesce(F.col("customer.is_returning"), F.lit(False)).alias("is_returning"),
        F.col("order.order_id").alias("order_id"),
        F.coalesce(F.col("order.quantity"), F.lit(1)).alias("quantity"),
        F.col("order.unit_price").alias("unit_price"),
        F.coalesce(F.col("order.discount_pct"), F.lit(0.0)).alias("discount_pct"),
        F.col("order.gross_amount").alias("gross_amount"),
        F.coalesce(F.col("order.discount_amount"), F.lit(0.0)).alias("discount_amount"),
        net.alias("net_amount"),
        F.coalesce(F.col("order.currency"), F.lit("EUR")).alias("currency"),
        F.coalesce(F.col("order.payment_method"), F.lit("unknown")).alias("payment_method"),
        F.col("marketing.campaign").alias("campaign"),
        F.col("marketing.source").alias("utm_source"),
        F.col("marketing.medium").alias("utm_medium"),
    )


def derive(df: DataFrame) -> DataFrame:
    """Add the derived analytical columns and the partition keys."""
    is_revenue = F.col("event_type").isin(REVENUE_EVENTS)
    is_negative = F.col("event_type").isin(NEGATIVE_REVENUE_EVENTS)

    return (
        df
        .withColumn("price_category", _price_category(F.col("product_price")))
        .withColumn("is_revenue_event", is_revenue)
        .withColumn("is_conversion", is_revenue)
        # Cancellations and refunds carry the same positive amount as the order
        # they undo; signing them here means net revenue is a plain SUM downstream.
        .withColumn(
            "signed_net_amount",
            F.when(is_negative, -F.coalesce(F.col("net_amount"), F.lit(0.0)))
             .when(is_revenue, F.coalesce(F.col("net_amount"), F.lit(0.0)))
             .otherwise(F.lit(0.0)),
        )
        .withColumn("day_of_week", F.date_format(F.col("occurred_ts"), "EEEE"))
        .withColumn("is_weekend", F.dayofweek(F.col("occurred_ts")).isin([1, 7]))
        .withColumn(
            "partition_date",
            F.coalesce(F.date_format(F.col("occurred_ts"), "yyyy-MM-dd"), F.lit("unknown")),
        )
        .withColumn(
            "partition_hour",
            F.coalesce(F.date_format(F.col("occurred_ts"), "HH"), F.lit("unknown")),
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest arrival of each business event.

    Deduplicating on ``idempotency_key`` (falling back to the natural key when
    an older v2 record has none) makes the job safe to re-run over an
    overlapping window — the usual cost of at-least-once delivery upstream.
    """
    dedup_key = F.coalesce(
        F.col("idempotency_key"),
        F.concat_ws("|",
                    F.coalesce(F.col("event_type"), F.lit("")),
                    F.coalesce(F.col("occurred_at"), F.lit("")),
                    F.coalesce(F.col("session_id"), F.lit("")),
                    F.coalesce(F.col("product_id"), F.lit("")),
                    F.coalesce(F.col("customer_id"), F.lit("")),
                    F.coalesce(F.col("order_id"), F.lit(""))),
    )
    ordering = Window.partitionBy(dedup_key).orderBy(F.col("ingested_at").desc_nulls_last())

    return (
        df.withColumn("_row", F.row_number().over(ordering))
        .filter(F.col("_row") == 1)
        .drop("_row")
    )


def clean(df: DataFrame) -> DataFrame:
    """Drop rows that cannot carry analytical meaning, and clamp absurd values."""
    return (
        df
        .filter(F.col("product_id").isNotNull() & (F.length(F.trim(F.col("product_id"))) > 0))
        .filter(F.col("occurred_ts").isNotNull())
        .withColumn("quantity", F.when(F.col("quantity").between(1, 100), F.col("quantity")).otherwise(F.lit(1)))
        .withColumn(
            "discount_pct",
            F.when(F.col("discount_pct").between(0, 100), F.col("discount_pct")).otherwise(F.lit(0.0)),
        )
        .withColumn("product_name", F.substring(F.col("product_name"), 1, 1000))
    )


def to_processed(df: DataFrame) -> DataFrame:
    """Full raw → processed pipeline: flatten, clean, derive, deduplicate."""
    return deduplicate(derive(clean(flatten(df)))).select(*PROCESSED_COLUMNS)

# ─────────────────────────────────────────────
# QUALITY (SPARK SIDE)
# ─────────────────────────────────────────────

def quality_summary(raw: DataFrame, processed: DataFrame) -> Dict[str, Any]:
    """Aggregate quality facts about the batch, computed in one Spark pass each.

    Returned as plain Python so the caller can drop it straight into the
    ``quality/`` report and into CloudWatch.
    """
    raw_count = raw.count()
    processed_count = processed.count()

    nullable = ["customer_id", "session_id", "product_name", "category", "product_price", "order_id"]
    null_row = processed.agg(*[
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column) for column in nullable
    ]).collect()
    nulls = null_row[0].asDict() if null_row else {}

    distinct_keys = processed.select("idempotency_key").distinct().count()

    event_rows = processed.groupBy("event_type").count().collect()
    partition_rows = processed.select("partition_date").distinct().collect()

    return {
        "raw_records": raw_count,
        "processed_records": processed_count,
        "dropped_records": max(raw_count - processed_count, 0),
        "duplicate_records": max(processed_count - distinct_keys, 0),
        "retention_pct": round(100.0 * processed_count / raw_count, 2) if raw_count else 0.0,
        "null_counts": {k: int(v or 0) for k, v in nulls.items()},
        "null_pct": {
            k: round(100.0 * int(v or 0) / processed_count, 2) if processed_count else 0.0
            for k, v in nulls.items()
        },
        "events_by_type": {row["event_type"]: row["count"] for row in event_rows},
        "partitions": sorted(row["partition_date"] for row in partition_rows if row["partition_date"]),
    }

# ─────────────────────────────────────────────
# HELPER FUNCTIONS FOR DATA CLEANING
# ─────────────────────────────────────────────

def _clean_string(value):
    """
    Clean string values:
    - Strip whitespace
    - Return None if empty or None
    - Limit to 1000 characters
    """
    if value is None:
        return None

    if not isinstance(value, str):
        return None

    cleaned = value.strip()

    if not cleaned:
        return None

    return cleaned[:1000]


def _clean_numeric(value):
    """
    Clean numeric values:
    - Convert to float
    - Handle string representations
    - Return None for invalid values
    """
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _validate_record(record):
    """
    Validate a record structure.
    Returns (is_valid: bool, error_message: str or None)
    """
    if not isinstance(record, dict):
        return False, "not_a_dict"

    required_fields = ["occurred_at", "event_type", "product", "customer"]
    missing = [f for f in required_fields if f not in record or record[f] is None]

    if missing:
        return False, f"missing_fields: {missing}"

    product = record.get("product")
    if not isinstance(product, dict) or not product.get("product_id"):
        return False, "missing_product_id"

    customer = record.get("customer")
    if not isinstance(customer, dict):
        return False, "invalid_customer_format"

    return True, None


def _enrich_record(record):
    """
    Enrich a record by:
    - Extracting nested fields
    - Adding partition columns (date, hour)
    - Categorizing by price
    - Carrying the v3 session / order / marketing context through
    """
    occurred_at = record.get("occurred_at", "")

    # Parse datetime and extract date and hour
    try:
        dt = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        partition_date = dt.strftime("%Y-%m-%d")
        partition_hour = dt.strftime("%H")
    except Exception:
        partition_date = "unknown"
        partition_hour = "unknown"

    product = record.get("product") or {}
    customer = record.get("customer") or {}
    order = record.get("order") or {}
    session = record.get("session") or {}
    marketing = record.get("marketing") or {}
    device = record.get("device") or {}

    price = _clean_numeric(product.get("price"))

    if price is None:
        price_category = "unknown"
    elif price < 50:
        price_category = "budget"
    elif price < 200:
        price_category = "mid"
    else:
        price_category = "premium"

    event_type = record.get("event_type")
    net_amount = _clean_numeric(order.get("net_amount"))
    if net_amount is None:
        net_amount = _clean_numeric(order.get("amount"))

    if event_type in ("order_cancelled", "refund_issued"):
        signed_net_amount = -(net_amount or 0.0)
    elif event_type == "order_placed":
        signed_net_amount = net_amount or 0.0
    else:
        signed_net_amount = 0.0

    enriched = {
        "event_type": event_type,
        "product_id": product.get("product_id"),
        "product_name": _clean_string(product.get("name")),
        "product_price": price,
        "customer_id": customer.get("customer_id"),
        "occurred_at": record.get("occurred_at"),
        "partition_date": partition_date,
        "partition_hour": partition_hour,
        "price_category": price_category,
        # ── v3 additions ──
        "idempotency_key": record.get("idempotency_key"),
        "channel": record.get("channel"),
        "device_type": device.get("type"),
        "session_id": session.get("session_id"),
        "customer_segment": customer.get("segment"),
        "customer_country": customer.get("country"),
        "category": _clean_string(product.get("category")),
        "brand": _clean_string(product.get("brand")),
        "order_id": order.get("order_id"),
        "quantity": order.get("quantity"),
        "discount_pct": _clean_numeric(order.get("discount_pct")),
        "gross_amount": _clean_numeric(order.get("gross_amount")),
        "net_amount": net_amount,
        "signed_net_amount": signed_net_amount,
        "currency": order.get("currency"),
        "payment_method": order.get("payment_method"),
        "campaign": marketing.get("campaign"),
        "utm_source": marketing.get("source"),
    }

    return enriched


# ─────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────

def load_config(path: str) -> dict:
    logger.info(f"Chargement de la config depuis: {path}")

    if path.startswith("s3://"):
        parts = path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]

        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))

    with open(path, "r") as f:
        return json.load(f)

# ─────────────────────────────────────────────
# S3 HELPERS
# ─────────────────────────────────────────────

def list_s3_files(bucket: str, prefix: str) -> list:
    # bronze/events peut contenir plusieurs fichiers .json; on les collecte tous avant traitement.
    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])

    return keys


def load_json(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    payload = obj["Body"].read().decode("utf-8").strip()

    if not payload:
        return []

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        pass

    records = []
    for line in payload.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except Exception as e:
            logger.warning(f"Bad JSON line in {key}: {e}")
    return records


def _coerce_records(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return [payload]


def write_json(bucket: str, key: str, data: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json"
    )


def write_parquet(bucket: str, output_prefix: str, rows: list) -> str:
    """Write processed records as a Parquet dataset in S3 (python engine)."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("occurred_at", StringType(), True),
        StructField("partition_date", StringType(), True),
        StructField("partition_hour", StringType(), True),
        StructField("price_category", StringType(), True),
    ])

    spark = SparkSession.builder.getOrCreate()
    projected = [{field.name: row.get(field.name) for field in schema.fields} for row in rows]
    dataframe = spark.createDataFrame(projected, schema=schema)

    output_path = f"s3://{bucket}/{output_prefix.rstrip('/')}/"
    dataframe.write.mode("overwrite").partitionBy("partition_date", "partition_hour").parquet(output_path)
    return output_path


# ─────────────────────────────────────────────
# LOCAL FILE HELPERS
# ─────────────────────────────────────────────

def list_local_files(prefix: str) -> list:
    """List JSON files in a local directory."""
    path = Path(prefix)
    if not path.exists():
        return []
    return sorted([str(f) for f in path.glob("*.json")])


def load_local_json(file_path: str) -> list:
    """Load JSON lines from a local file."""
    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
    return records


def write_local_json(file_path: str, data: dict) -> None:
    """Write JSON to a local file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


# ─────────────────────────────────────────────
# PYTHON ENGINE
# ─────────────────────────────────────────────

def run_job(input_prefix: Optional[str] = None, output_prefix: Optional[str] = None,
            bucket: Optional[str] = None, local_fs: bool = False) -> dict:
    """
    Process e-commerce records with the dependency-free Python engine.

    Produces the ``processed`` layer only — use :func:`run_spark_job` for the
    curated tables. Kept as the local-development and CI path.

    Args:
        input_prefix: Input path (local or S3)
        output_prefix: Output path (local or S3)
        bucket: S3 bucket name (only for S3)
        local_fs: If True, use local filesystem. If False, use S3.

    Returns:
        dict with status, metrics, and output path
    """
    logger.info(f"Traitement depuis {input_prefix} vers {output_prefix}")

    raw_records = []

    # LOAD
    if local_fs:
        files = list_local_files(input_prefix)
        for file_path in files:
            try:
                raw_records.extend(load_local_json(file_path))
            except Exception as e:
                logger.warning(f"Fichier ignoré {file_path}: {e}")
    else:
        files = list_s3_files(bucket, input_prefix)
        logger.info(f"RAW FILES: {files}")
        for f in files:
            try:
                raw_records.extend(_coerce_records(load_json(bucket, f)))
            except Exception as e:
                logger.warning(f"Fichier ignoré {f}: {e}")

    logger.info(f"RAW RECORDS COUNT: {len(raw_records)}")

    if not raw_records:
        logger.warning(f"Aucune donnée trouvée dans {input_prefix}")
        return {
            "status": "success",
            "metrics": {
                "input_records": 0,
                "valid_records": 0,
                "invalid_records": 0,
                "duplicate_records": 0,
                "output_records": 0,
                "quality_pct": 0.0
            },
            "output_path": None
        }

    # PROCESS
    processed = []
    seen = set()
    invalid = 0
    duplicates = 0

    for r in raw_records:
        is_valid, error = _validate_record(r)
        if not is_valid:
            invalid += 1
            logger.warning(f"Invalid record: {error} -> {r}")
            continue

        enriched = _enrich_record(r)

        # Prefer the schema's idempotency key; fall back to the natural key for
        # v2 records that predate it.
        dedup_key = enriched.get("idempotency_key") or (
            f"{enriched['event_type']}-{enriched['product_id']}-{enriched['customer_id']}"
        )
        if dedup_key in seen:
            duplicates += 1
            continue

        seen.add(dedup_key)
        processed.append(enriched)

    # OUTPUT
    if local_fs:
        output_file = Path(output_prefix) / "processed_output.json"
        write_local_json(str(output_file), {
            "input_count": len(raw_records),
            "valid_count": len(raw_records) - invalid,
            "invalid_count": invalid,
            "duplicate_count": duplicates,
            "output_count": len(processed),
            "data": processed
        })
        output_path = str(output_file)
    else:
        output_path = write_parquet(bucket, output_prefix, processed)

    quality_pct = 100.0 * len(processed) / len(raw_records) if raw_records else 0.0

    result = {
        "status": "success",
        "metrics": {
            "input_records": len(raw_records),
            "valid_records": len(raw_records) - invalid,
            "invalid_records": invalid,
            "duplicate_records": duplicates,
            "output_records": len(processed),
            "quality_pct": quality_pct
        },
        "output_path": output_path
    }

    logger.info(f"Résultat: {json.dumps(result, indent=2, default=str)}")
    return result


# ─────────────────────────────────────────────
# SPARK ENGINE
# ─────────────────────────────────────────────

def run_spark_job(config: dict, spark=None) -> dict:
    """Run the full Spark pipeline: processed + curated + quality report.

    Config keys consumed here (beyond the paths):

    ==========================  ==============================================
    ``PROCESS_DATE``            restrict to one ``YYYY-MM-DD`` partition
    ``WRITE_MODE``              Parquet write mode (``overwrite``)
    ``COALESCE``                cap output files per dataset (4)
    ``QUALITY``                 threshold overrides, see :mod:`common.quality`
    ``FAIL_ON_QUALITY``         raise when the quality verdict is ``fail``
    ==========================  ==============================================
    """
    paths = build_paths(config)
    metrics = JobMetrics.from_config(config, stage="glue_processing")
    log_io(config)
    started = datetime.now(timezone.utc)

    spark = spark or SparkSession.builder.appName(config.get("JOB_NAME", "ecommerce-processing")).getOrCreate()

    logger.info("Reading bronze events from %s", paths["bronze_events"])
    if not _has_objects(paths["bronze_events"]):
        return _nothing_to_process(config, paths, metrics, started, "the prefix holds no object")

    try:
        raw = read_raw(spark, paths["bronze_events"])
    except Exception as exc:  # noqa: BLE001 - only the missing-path case is absorbed
        if not _is_missing_path(exc):
            raise
        return _nothing_to_process(config, paths, metrics, started, str(exc))

    # Counted before anything else. A job that reads objects and writes nothing
    # is the hardest kind to diagnose after the fact: it reports success, and
    # the only trace of what happened is the number of rows at each step.
    raw_count = raw.count()
    logger.info("Read %d row(s) from %s", raw_count, paths["bronze_events"])
    if raw_count == 0:
        return _read_nothing(config, paths, metrics, started, 0)

    # Rows, but not one of them carries an event type: the objects are not
    # NDJSON. A pretty-printed JSON file gives Spark one row per *line* of
    # formatting, every field NULL — a non-zero count that means nothing.
    # Without this, the job would blame deduplication for a format problem.
    readable = raw.filter(F.col("event_type").isNotNull()).count()
    if readable == 0:
        return _read_nothing(config, paths, metrics, started, raw_count)
    if readable < raw_count:
        logger.warning(
            "%d of %d row(s) had no event_type and will be dropped — check what is "
            "under %s", raw_count - readable, raw_count, paths["bronze_events"],
        )

    process_date = config.get("PROCESS_DATE")
    processed = to_processed(raw)
    if process_date:
        logger.info("Restricting to partition_date=%s", process_date)
        processed = processed.filter(F.col("partition_date") == process_date)

    # Every curated table scans this DataFrame; caching pays for itself from the
    # second consumer onwards.
    processed = processed.cache()

    processed_count = processed.count()
    logger.info(
        "%d row(s) survived cleaning and deduplication (from %d read)",
        processed_count, raw_count,
    )
    if processed_count == 0:
        return _kept_nothing(config, paths, metrics, started, raw_count, process_date)

    write_mode = config.get("WRITE_MODE", "overwrite")
    coalesce = int(config.get("COALESCE", 4))

    # Coalesced on the way out: with small hourly batches the default
    # parallelism produces hundreds of tiny files, the classic way to make an
    # Athena table slow and expensive.
    (processed.coalesce(coalesce) if coalesce > 0 else processed) \
        .write.mode(write_mode) \
        .partitionBy("partition_date", "partition_hour") \
        .parquet(paths["silver_events"])

    outputs = {"processed": paths["silver_events"]}
    row_counts = {"processed": processed.count()}

    # ── quality report ──
    summary = quality_summary(raw, processed)
    thresholds = config.get("QUALITY") or {}
    verdict = _assess(summary, thresholds)
    report = {
        "job": config.get("JOB_NAME", "ecommerce-processing"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "process_date": process_date,
        "verdict": verdict["verdict"],
        "breaches": verdict["breaches"],
        "summary": summary,
        "row_counts": row_counts,
        "outputs": outputs,
    }

    report_key = (
        f"{zone_prefix(config, 'quality')}"
        f"dt={process_date or started.strftime('%Y-%m-%d')}/"
        f"report-{started.strftime('%Y%m%dT%H%M%S')}.json"
    )
    try:
        write_json(paths["bucket"], report_key, report)
        logger.info("Quality report written to s3://%s/%s", paths["bucket"], report_key)
    except Exception as exc:  # noqa: BLE001 - the data is already written; the report is not worth failing on
        logger.error("Failed to write quality report: %s", exc)

    # ── metrics ──
    metrics.count("RawRecords", summary["raw_records"])
    metrics.count("ProcessedRecords", summary["processed_records"])
    metrics.count("DroppedRecords", summary["dropped_records"])
    metrics.count("DuplicateRecords", summary["duplicate_records"])
    metrics.gauge("RetentionPct", summary["retention_pct"], unit="Percent")
    metrics.gauge("JobDurationSeconds", (datetime.now(timezone.utc) - started).total_seconds(), unit="Seconds")
    for name, count in row_counts.items():
        metrics.count("CuratedRows", count, Dataset=name)
    for event_type, count in summary["events_by_type"].items():
        metrics.count("EventsByType", count, EventType=event_type)
    metrics.flush()

    processed.unpersist()

    result = {
        "status": "fail" if verdict["verdict"] == "fail" else "success",
        "engine": "spark",
        "metrics": {
            "input_records": summary["raw_records"],
            "output_records": summary["processed_records"],
            "invalid_records": summary["dropped_records"],
            "duplicate_records": summary["duplicate_records"],
            "quality_pct": summary["retention_pct"],
        },
        "quality": report,
        "outputs": outputs,
        "output_path": paths["silver_events"],
    }

    if verdict["verdict"] == "fail" and as_bool(config.get("FAIL_ON_QUALITY")):
        raise RuntimeError(f"Quality gate failed: {verdict['breaches']}")

    return result


def _assess(summary: dict, thresholds: dict) -> dict:
    """Apply the batch-level quality gates to the Spark summary."""
    min_retention = float(thresholds.get("min_pass_pct", 95.0))
    warn_retention = float(thresholds.get("warn_pass_pct", 99.0))
    max_duplicate_pct = float(thresholds.get("max_duplicate_pct", 5.0))
    min_records = int(thresholds.get("min_records", 0))

    processed_count = summary["processed_records"]
    duplicate_pct = 100.0 * summary["duplicate_records"] / processed_count if processed_count else 0.0

    breaches = []
    if summary["raw_records"] < min_records:
        breaches.append("min_records")
    if summary["raw_records"] and summary["retention_pct"] < min_retention:
        breaches.append("min_pass_pct")
    if duplicate_pct > max_duplicate_pct:
        breaches.append("max_duplicate_pct")

    for column, pct in summary.get("null_pct", {}).items():
        limit = (thresholds.get("max_null_pct") or {}).get(column)
        if limit is not None and pct > float(limit):
            breaches.append(f"max_null_pct:{column}")

    if breaches:
        verdict = "fail"
    elif summary["raw_records"] and summary["retention_pct"] < warn_retention:
        verdict = "warn"
    else:
        verdict = "pass"

    return {"verdict": verdict, "breaches": breaches}


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

def main(argv=None) -> dict:
    argv = argv if argv is not None else sys.argv
    args = getResolvedOptions(argv, ["JOB_NAME", "CONFIG_PATH"])

    config_path = args["CONFIG_PATH"]
    config = load_config(config_path)
    # The Glue argument wins: it is the job's real identity, and one shared
    # config file must not make all four jobs report the same name.
    if args.get("JOB_NAME"):
        config["JOB_NAME"] = args["JOB_NAME"]

    engine = config.get("ENGINE", "spark")
    bucket = config.get("OUTPUT_BUCKET")
    input_prefix = dataset_prefix(config, "bronze/events")
    output_prefix = dataset_prefix(config, "silver/events")

    logger.info(f"Engine        : {engine}")
    logger.info(f"Bucket        : {bucket}")
    logger.info(f"Input prefix  : {input_prefix}")
    logger.info(f"Output prefix : {output_prefix}")

    if engine == "python":
        return run_job(
            input_prefix=input_prefix,
            output_prefix=output_prefix,
            bucket=bucket,
            local_fs=False,
        )

    return run_spark_job(config)


if __name__ == "__main__":
    print(json.dumps(main(), indent=2, default=str))


# ─────────────────────────────────────────────
# INPUT / OUTPUT CONTRACT
# ─────────────────────────────────────────────

def describe_io(config: dict) -> dict:
    """Where this job reads and writes, and in what format.

    The format matters as much as the path here: bronze must be NDJSON. A
    pretty-printed JSON or a CSV under the same prefix reads as rows with every
    field NULL — no error, no data.
    """
    paths = build_paths(config)
    return {
        "job": "glue_bronze_to_silver",
        "reads": [
            {"what": "bronze events", "format": "NDJSON, one event per line",
             "where": paths["bronze_events"]},
        ],
        "writes": [
            {"what": "silver events", "format": "Parquet, partitioned by date/hour",
             "where": paths["silver_events"]},
            {"what": "quality report", "format": "JSON, one per run",
             "where": f"s3://{paths['bucket']}/{zone_prefix(config, 'quality')}"},
        ],
    }


def log_io(config: dict) -> None:
    """Print the contract at start-up, before any work.

    A job that reports success without writing anything is the hardest failure
    to diagnose, and the first question is always the same: which prefix did it
    actually read? Answer it in the first three lines of the log rather than
    after an afternoon of guessing.
    """
    # Never raises. This is diagnostics printed before any work — a job killed
    # by its own logging is the worst possible trade.
    try:
        contract = describe_io(config)
        logger.info("--- %s : input/output contract ---", contract["job"])
        for side in ("reads", "writes"):
            for item in contract[side]:
                logger.info(
                    "  %-6s %-26s %-12s %s",
                    side.upper(), item.get("what"), f"[{item.get('format')}]", item.get("where"),
                )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not stop the job
        logger.warning("Could not describe the input/output contract: %s", exc)
