"""Glue job 1 — **landing → bronze**: the partner file drops (source 2).

    s3://<lake>/landing/partners/*.csv|*.ndjson
            │
            ▼
      Glue Job ──┬──> s3://<lake>/bronze/events/dt=…/hour=…/   (NDJSON, v3)
                 ├──> s3://<lake>/quarantine/landing/dt=…/     (+ failing rules)
                 └──> s3://<lake>/landing/_processed/           (the files, moved)

Partners export **columns**, not events: a CSV row carries ``product_id`` at the
top level while everything downstream looks for ``product.product_id``. This job
lifts each row into the v3 record — building the nested blocks, doing the basket
arithmetic once, and hashing the same idempotency key the streaming path
produces — so a dropped file and a queued event are the same record by the time
either reaches silver.

Files already in the v3 shape (an exporter that speaks our schema) are read with
the schema directly and skip the lift.

Why a job and not a Lambda
--------------------------
A partner drop is a *batch*: it can be a hundred rows or ten million, and the
second shape does not fit in a function with a 15-minute ceiling. The cost is
latency — a file is picked up on the next run rather than within seconds — and
a minimum of one billed minute per run. For a steady trickle of small files, a
Lambda is the cheaper design; for files, this is.

Re-running is safe twice over
-----------------------------
Ingested files are **moved** to ``landing/_processed/`` after a successful
write, so the next run does not see them again. And even if a file were replayed
by hand, every record carries the same ``idempotency_key``, which the silver job
deduplicates on.
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
# Guarded: this module is imported by the unit tests on machines with no
# PySpark. Every Spark object below is built inside a function.
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
# THE v3 RECORD
# ─────────────────────────────────────────────

#: Where in the drop zone this job looks, relative to the landing zone.
DEFAULT_INGEST_SUBPATH = "partners/"

#: Where ingested files are moved once bronze has been written.
DEFAULT_ARCHIVE_SUBPATH = "_processed/"

#: Flat column → the v3 field it feeds, in order of preference. A partner names
#: things its own way; the mapping lives here rather than in four places.
PRODUCT_ALIASES = {
    "product_id": ("product_id", "sku", "id"),
    "sku": ("sku", "product_id"),
    "name": ("product_name", "name", "title"),
    "category": ("category", "product_category"),
    "brand": ("brand",),
    "price": ("price", "product_price", "unit_price"),
}

EVENT_ALIASES = {
    "occurred_at": ("occurred_at", "event_time", "timestamp"),
    "event_type": ("event_type", "event", "action"),
    "channel": ("channel",),
    "session_id": ("session_id", "session"),
    "sequence": ("sequence", "event_sequence"),
    "device_type": ("device_type", "device"),
    "device_os": ("device_os", "os"),
    "country": ("country",),
    "city": ("city",),
    "customer_id": ("customer_id", "customer"),
    "segment": ("segment", "customer_segment"),
    "order_id": ("order_id",),
    "quantity": ("quantity", "qty"),
    "unit_price": ("unit_price", "price"),
    "discount_pct": ("discount_pct", "discount"),
    "currency": ("currency",),
    "payment_method": ("payment_method", "payment"),
    "campaign": ("campaign", "utm_campaign"),
    "utm_source": ("utm_source", "source"),
    "utm_medium": ("utm_medium", "medium"),
}

#: The seven fields that make up an event's business identity, in the order the
#: shared hash uses them. Changing this order changes every key ever produced.
IDENTITY_FIELDS = (
    "event_type", "occurred_at", "session.session_id", "session.sequence",
    "product.product_id", "customer.customer_id", "order.order_id",
)

#: Bronze admits almost everything — it is the record of what arrived. Only a
#: row that cannot be identified at all is turned away, because it can never be
#: joined, deduplicated or replayed.
BRONZE_CHECKS: List[Dict[str, str]] = [
    {"name": "occurred_at_parsed", "expr": "occurred_at IS NOT NULL",
     "description": "An unparseable timestamp lands the row in the wrong partition."},
    {"name": "event_type_present", "expr": "event_type IS NOT NULL AND length(event_type) > 0",
     "description": "Without it the row belongs to no funnel stage."},
    {"name": "product_id_present", "expr": "product.product_id IS NOT NULL",
     "description": "An event with no product cannot be attributed to anything."},
    {"name": "price_in_range", "expr": "product.price IS NULL OR (product.price >= 0 AND product.price <= 100000)",
     "description": "A price outside this range is a parsing accident, not a sale."},
    {"name": "quantity_in_range", "expr": "order.quantity IS NULL OR (order.quantity >= 1 AND order.quantity <= 100)",
     "description": "Plausible basket sizes; above this it is usually a test order."},
]


# This script is standalone — what a Glue job's Script path points at is all it runs.

SCHEMA_VERSION = "3.0"

def flat_columns() -> List[str]:
    """Every flat column name the lift knows how to read."""
    names = []
    for aliases in list(PRODUCT_ALIASES.values()) + list(EVENT_ALIASES.values()):
        names.extend(aliases)
    return sorted(set(names))


def v3_schema(spark: "SparkSession") -> "StructType":
    """The v3 record shape — *derived* from the lift rather than restated.

    ``lift_flat`` already spells out every field and every type; declaring the
    same structure a second time as a literal would be two definitions of one
    contract, and the second one would rot. Lifting an empty frame that has all
    the columns the lift knows about yields exactly the schema the exporter is
    expected to produce, so the JSON drops are read with it.
    """
    empty = spark.createDataFrame([], ", ".join(f"`{name}` string" for name in flat_columns()))
    return lift_flat(empty).schema


#: Timestamps we accept. Anything else becomes NULL rather than an exception.
ISO_TIMESTAMP_RE = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"


def load_config(path: str, s3: Any = None) -> dict:
    """Read a job's JSON config, from S3 or from disk."""
    logger.info("Loading config from %s", path)
    if path.startswith("s3://"):
        bucket, _, key = path[len("s3://"):].partition("/")
        s3 = s3 or boto3.client("s3")
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))

    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


# ─────────────────────────────────────────────
# LIFTING A FLAT ROW
# ─────────────────────────────────────────────

def _pick(columns: List[str], aliases: Tuple[str, ...]):
    """First alias present in the file, as a column; NULL when none of them is."""
    present = [alias for alias in aliases if alias in columns]
    if not present:
        return F.lit(None).cast("string")
    if len(present) == 1:
        return F.col(f"`{present[0]}`")
    return F.coalesce(*[F.col(f"`{name}`") for name in present])


def lift_flat(df: "DataFrame", channel_default: str = "web") -> "DataFrame":
    """Turn a flat partner export into v3 records.

    Mirrors ``common.ecommerce_schema.normalize_record`` — the builder the
    streaming path uses — so both paths produce the same record for the same
    business event, down to the basket arithmetic and the identity hash.
    """
    columns = df.columns
    product = {field: _pick(columns, aliases) for field, aliases in PRODUCT_ALIASES.items()}
    event = {field: _pick(columns, aliases) for field, aliases in EVENT_ALIASES.items()}

    # Timestamps are re-emitted in UTC ISO-8601, the exact form the streaming
    # path hashes. A row whose timestamp will not parse keeps a NULL and is
    # turned away by the checks rather than landing in a wrong partition.
    # Screened by regex before parsing: Spark runs ANSI mode from 4.0, where a
    # bare to_timestamp on a malformed string *raises* — one unparseable date in
    # a partner export would abort the job instead of quarantining one row.
    normalized = F.regexp_replace(event["occurred_at"], "Z$", "+00:00")
    occurred_ts = F.to_timestamp(F.when(normalized.rlike(ISO_TIMESTAMP_RE), normalized))
    occurred_at = F.date_format(occurred_ts, "yyyy-MM-dd'T'HH:mm:ssXXX")

    unit_price = F.coalesce(event["unit_price"].cast("double"), product["price"].cast("double"))
    quantity = F.coalesce(event["quantity"].cast("int"), F.lit(1))
    discount_pct = F.coalesce(event["discount_pct"].cast("double"), F.lit(0.0))

    gross = F.round(unit_price * quantity, 2)
    discount_amount = F.round(gross * discount_pct / F.lit(100.0), 2)
    net = F.round(gross - F.coalesce(discount_amount, F.lit(0.0)), 2)

    product_id = product["product_id"].cast("string")
    event_type = F.coalesce(event["event_type"].cast("string"), F.lit("unknown"))

    return df.select(
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        F.concat_ws("-", event_type, product_id, occurred_at).alias("event_id"),
        F.lit(None).cast("string").alias("idempotency_key"),   # filled by add_identity
        F.lit(None).cast("string").alias("ingested_at"),
        occurred_at.alias("occurred_at"),
        F.coalesce(event["channel"].cast("string"), F.lit(channel_default)).alias("channel"),
        event_type.alias("event_type"),
        F.struct(
            event["session_id"].cast("string").alias("session_id"),
            event["sequence"].cast("int").alias("sequence"),
        ).alias("session"),
        F.struct(
            F.coalesce(event["device_type"].cast("string"), F.lit("unknown")).alias("type"),
            event["device_os"].cast("string").alias("os"),
            F.lit(None).cast("string").alias("user_agent"),
        ).alias("device"),
        F.struct(
            event["country"].cast("string").alias("country"),
            event["city"].cast("string").alias("city"),
        ).alias("geo"),
        F.struct(
            product_id.alias("product_id"),
            product["sku"].cast("string").alias("sku"),
            product["name"].cast("string").alias("name"),
            product["category"].cast("string").alias("category"),
            product["brand"].cast("string").alias("brand"),
            product["price"].cast("double").alias("price"),
        ).alias("product"),
        F.struct(
            event["customer_id"].cast("string").alias("customer_id"),
            event["segment"].cast("string").alias("segment"),
            event["country"].cast("string").alias("country"),
            F.lit(False).alias("is_returning"),
        ).alias("customer"),
        F.struct(
            event["order_id"].cast("string").alias("order_id"),
            quantity.alias("quantity"),
            F.round(unit_price, 2).alias("unit_price"),
            F.round(discount_pct, 2).alias("discount_pct"),
            gross.alias("gross_amount"),
            discount_amount.alias("discount_amount"),
            net.alias("net_amount"),
            # v2 alias — kept so existing consumers of `order.amount` keep working.
            net.alias("amount"),
            F.coalesce(event["currency"].cast("string"), F.lit("EUR")).alias("currency"),
            F.coalesce(event["payment_method"].cast("string"), F.lit("unknown")).alias("payment_method"),
        ).alias("order"),
        F.struct(
            event["campaign"].cast("string").alias("campaign"),
            event["utm_source"].cast("string").alias("source"),
            event["utm_medium"].cast("string").alias("medium"),
        ).alias("marketing"),
        F.struct(
            F.lit(None).cast("string").alias("processed_at"),
            F.lit(None).cast("string").alias("source"),
            F.lit(None).cast("string").alias("message_id"),
            F.lit(None).cast("string").alias("source_object"),
        ).alias("_meta"),
    )


def add_identity(df: "DataFrame", source_object: str = "landing") -> "DataFrame":
    """Stamp the idempotency key and the ingest metadata.

    The key is ``sha1`` over the same seven fields, joined by ``|``, that
    :func:`common.ecommerce_schema.idempotency_key` hashes — a NULL contributes
    an empty string, which is why every part is coalesced rather than left to
    ``concat_ws``, whose habit is to skip NULLs entirely and silently shift the
    fields along.
    """
    parts = [F.coalesce(F.col(field).cast("string"), F.lit("")) for field in IDENTITY_FIELDS]
    stamped_at = datetime.now(timezone.utc).isoformat()

    return (
        df
        .withColumn("idempotency_key", F.sha1(F.concat_ws("|", *parts)))
        .withColumn("ingested_at", F.lit(stamped_at))
        .withColumn("_meta", F.struct(
            F.lit(stamped_at).alias("processed_at"),
            F.lit("landing").alias("source"),
            F.lit(None).cast("string").alias("message_id"),
            F.lit(source_object).alias("source_object"),
        ))
    )


# ─────────────────────────────────────────────
# READING THE DROP ZONE
# ─────────────────────────────────────────────

def list_drops(bucket: str, prefix: str, s3=None) -> List[Dict[str, str]]:
    """The files waiting in the drop zone, classified by how to read them.

    Logs what it saw, not only what it kept. "Nothing to ingest" and "eleven
    files, none with an extension I read" look identical from bronze, and the
    second one is a mistake somebody needs to see.
    """
    s3 = s3 or boto3.client("s3")
    drops = []
    ignored: List[str] = []
    empty = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if obj.get("Size", 0) == 0:
                empty += 1
                continue
            lowered = key.lower()
            if lowered.endswith(".csv"):
                drops.append({"key": key, "format": "csv"})
            elif lowered.endswith((".json", ".ndjson", ".jsonl")):
                drops.append({"key": key, "format": "json"})
            else:
                ignored.append(key)

    logger.info(
        "Drop zone s3://%s/%s — %d to ingest, %d ignored (extension), %d empty",
        bucket, prefix, len(drops), len(ignored), empty,
    )
    for key in ignored[:20]:
        logger.warning("Ignoring %s: extension is not .csv/.json/.ndjson/.jsonl", key)

    return drops


def sample_keys(bucket: str, prefix: str, s3=None, limit: int = 10) -> List[str]:
    """A handful of the keys under ``prefix`` — for diagnostics, not for work.

    Never raises: this runs on a path the job is already reporting a problem
    about, and losing the real message to a listing error would be perverse.
    """
    s3 = s3 or boto3.client("s3")
    keys: List[str] = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])
                if len(keys) >= limit:
                    return keys
    except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the real error
        logger.debug("Could not list %s for diagnostics: %s", prefix, exc)
    return keys


def _read_csv(spark: "SparkSession", paths: List[str]) -> "DataFrame":
    """Seam for the tests — every cell read as text, cast on the way out."""
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(paths)
    )


def _read_json(spark: "SparkSession", paths: List[str]) -> "DataFrame":
    """Seam for the tests — already-v3 records, read with the schema."""
    return (
        spark.read
        .schema(v3_schema(spark))
        .option("mode", "PERMISSIVE")
        .json(paths)
    )


def read_drops(spark: "SparkSession", bucket: str, drops: List[Dict[str, str]],
               channel_default: str = "web") -> Optional["DataFrame"]:
    """Read every waiting file and return one DataFrame of v3 records."""
    frames = []

    csv_paths = [f"s3://{bucket}/{d['key']}" for d in drops if d["format"] == "csv"]
    if csv_paths:
        logger.info("Reading %d CSV drop(s)", len(csv_paths))
        frames.append(lift_flat(_read_csv(spark, csv_paths), channel_default))

    json_paths = [f"s3://{bucket}/{d['key']}" for d in drops if d["format"] == "json"]
    if json_paths:
        logger.info("Reading %d JSON drop(s)", len(json_paths))
        frames.append(_read_json(spark, json_paths))

    if not frames:
        return None

    united = frames[0]
    for frame in frames[1:]:
        united = united.unionByName(frame, allowMissingColumns=True)
    return united


# ─────────────────────────────────────────────
# WRITING
# ─────────────────────────────────────────────

def split_on_checks(df: "DataFrame", checks: List[Dict[str, str]]) -> Tuple["DataFrame", "DataFrame"]:
    """Return ``(accepted, rejected)``; the rejected carry the rules they broke.

    Evaluated in one pass: a boolean per check, then a single filter, rather
    than one scan of the data per rule.
    """
    flags = F.filter(
        F.array(*[F.when(~F.expr(check["expr"]), F.lit(check["name"])) for check in checks]),
        lambda name: name.isNotNull(),
    )
    tagged = df.withColumn("failed_checks", flags)

    accepted = tagged.filter(F.size("failed_checks") == 0).drop("failed_checks")
    rejected = tagged.filter(F.size("failed_checks") > 0)
    return accepted, rejected


def with_partitions(df: "DataFrame") -> "DataFrame":
    """Partition on *event* time, so a late file lands in the hour it belongs to."""
    normalized = F.regexp_replace(F.col("occurred_at"), "Z$", "+00:00")
    occurred_ts = F.to_timestamp(F.when(normalized.rlike(ISO_TIMESTAMP_RE), normalized))
    return (
        df
        .withColumn("dt", F.coalesce(F.date_format(occurred_ts, "yyyy-MM-dd"), F.lit("unknown")))
        .withColumn("hour", F.coalesce(F.date_format(occurred_ts, "HH"), F.lit("unknown")))
    )


def _write_ndjson(df: "DataFrame", path: str, partition_by: List[str], coalesce: int) -> None:
    """Seam for the tests — bronze is NDJSON, one JSON object per line."""
    writer = df.coalesce(coalesce) if coalesce and coalesce > 0 else df
    writer.write.mode("append").partitionBy(*partition_by).json(path)


def archive(bucket: str, drops: List[Dict[str, str]], destination_prefix: str, s3=None) -> int:
    """Move ingested files out of the drop zone. Never raises.

    Moving is what makes the next run cheap: without it the job re-reads every
    file it has ever been given. A failure here is logged, not fatal — the data
    is already in bronze, and a re-read is absorbed by the dedupe in silver.
    """
    s3 = s3 or boto3.client("s3")
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    moved = 0

    for drop in drops:
        key = drop["key"]
        target = f"{destination_prefix}dt={stamp}/{key.rsplit('/', 1)[-1]}"
        try:
            s3.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": key})
            s3.delete_object(Bucket=bucket, Key=key)
            moved += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Could not archive %s: %s", key, exc)

    logger.info("Archived %d/%d file(s) to %s", moved, len(drops), destination_prefix)
    return moved


# ─────────────────────────────────────────────
# CONFIG + ENTRYPOINT
# ─────────────────────────────────────────────

def ingest_prefix(config: dict) -> str:
    """Where the partners drop their files."""
    return f"{zone_prefix(config, 'landing')}{_norm(config.get('LANDING_INGEST_SUBPATH', DEFAULT_INGEST_SUBPATH))}"


def archive_prefix(config: dict) -> str:
    """Where they go once bronze has them."""
    return f"{zone_prefix(config, 'landing')}{_norm(config.get('LANDING_ARCHIVE_SUBPATH', DEFAULT_ARCHIVE_SUBPATH))}"


# ─────────────────────────────────────────────
# INPUT / OUTPUT CONTRACT
# ─────────────────────────────────────────────

def describe_io(config: dict) -> dict:
    """Where this job reads and writes, and in what format.

    Declared rather than implied. ``tests/test_io_contracts.py`` pins every
    entry against the path the code actually uses, and checks that what this
    job writes is what the next one reads.
    """
    bucket = config["OUTPUT_BUCKET"]
    paths = build_paths(config)
    return {
        "job": "glue_landing_ingest",
        "reads": [
            {"what": "partner file drops", "format": "CSV / JSON / NDJSON",
             "where": f"s3://{bucket}/{ingest_prefix(config)}"},
        ],
        "writes": [
            {"what": "bronze events", "format": "NDJSON, one event per line",
             "where": paths["bronze_events"]},
            {"what": "rejected records", "format": "NDJSON + failing rules",
             "where": f"{paths['quarantine'].rstrip('/')}/landing/"},
            {"what": "ingested files (moved)", "format": "as received",
             "where": f"s3://{bucket}/{archive_prefix(config)}"},
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


def run(config: Dict[str, Any], spark: Any = None, s3: Any = None) -> Dict[str, Any]:
    started = datetime.now(timezone.utc)
    bucket = config["OUTPUT_BUCKET"]
    paths = build_paths(config)
    metrics = JobMetrics.from_config(config, stage="glue_landing_ingest")
    log_io(config)

    s3 = s3 or boto3.client("s3")
    drops = list_drops(bucket, ingest_prefix(config), s3=s3)

    if not drops:
        # A run that ingests nothing and reports success is indistinguishable
        # from one that worked — which is how an empty bronze zone goes
        # unnoticed. Say where it looked, and let the config make it fatal.
        location = f"s3://{bucket}/{ingest_prefix(config)}"
        message = (
            f"No file to ingest in {location} — nothing was written to "
            f"{paths['bronze_events']}. Upload .csv/.json/.ndjson/.jsonl files there, "
            f"and check OUTPUT_BUCKET and LANDING_INGEST_SUBPATH in the job config. "
            f"Already-ingested files are moved to s3://{bucket}/{archive_prefix(config)}."
        )
        metrics.count("DropsIngested", 0)
        metrics.flush()

        # Almost always the files are in the bucket, just not in the sub-prefix
        # the job reads. Show what *is* under landing/ so the log answers the
        # next question instead of prompting it.
        nearby = sample_keys(bucket, zone_prefix(config, "landing"), s3=s3)
        if nearby:
            message += (
                "\nFound under s3://{0}/{1} instead: {2}".format(
                    bucket, zone_prefix(config, "landing"), ", ".join(nearby)
                )
            )

        if as_bool(config.get("FAIL_ON_EMPTY_DROP_ZONE")):
            raise ValueError(message)

        logger.warning(message)
        return {
            "status": "success",
            "files": 0, "records": 0, "rejected": 0, "archived": 0,
            "reason": "empty drop zone",
            "drop_zone": location,
        }

    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName(config.get("JOB_NAME", "landing-ingest")).getOrCreate()

    records = read_drops(spark, bucket, drops, config.get("CHANNEL", "web"))
    records = add_identity(records, source_object=f"s3://{bucket}/{ingest_prefix(config)}")
    records = records.cache()

    accepted, rejected = split_on_checks(records, BRONZE_CHECKS)
    coalesce = int(config.get("COALESCE", 4))

    accepted_count = accepted.count()
    rejected_count = rejected.count()

    if accepted_count:
        _write_ndjson(with_partitions(accepted), paths["bronze_events"], ["dt", "hour"], coalesce)
        logger.info("Landed %d record(s) in %s", accepted_count, paths["bronze_events"])

    quarantine_path = f"{paths['quarantine'].rstrip('/')}/landing/"
    if rejected_count:
        _write_ndjson(with_partitions(rejected), quarantine_path, ["dt"], coalesce)
        logger.warning("Quarantined %d record(s) to %s", rejected_count, quarantine_path)

    # Only archive once bronze actually has the data.
    archived = 0
    if accepted_count and as_bool(config.get("ARCHIVE_PROCESSED"), True):
        archived = archive(bucket, drops, archive_prefix(config), s3=s3)

    records.unpersist()

    metrics.count("DropsIngested", len(drops))
    metrics.count("RecordsLanded", accepted_count)
    metrics.count("RecordsQuarantined", rejected_count)
    metrics.count("DropsArchived", archived)
    metrics.gauge("JobDurationSeconds", (datetime.now(timezone.utc) - started).total_seconds(), unit="Seconds")
    metrics.flush()

    return {
        "status": "success",
        "files": len(drops),
        "records": accepted_count,
        "rejected": rejected_count,
        "archived": archived,
        "bronze_path": paths["bronze_events"],
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
