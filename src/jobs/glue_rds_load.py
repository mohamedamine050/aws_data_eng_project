"""Glue job 5 — **the lake → the PostgreSQL warehouse**.

    S3 silver/events ──┐
    S3 gold/*        ──┴──> Spark JDBC ──> PostgreSQL (RDS)

Loads a *set* of targets — the event fact table plus every gold table — in one
pass, sharing a single JDBC connection profile. Which targets run is config,
not code:

    "RDS_TABLES": [
      {"dataset": "silver/events", "table": "analytics.fact_events"},
      {"dataset": "gold/orders",   "table": "analytics.fact_orders", "mode": "overwrite"}
    ]

Dataset names accept both the medallion form and the pre-medallion aliases
(``processed``, ``curated/orders``). With no ``RDS_TABLES`` block the job falls
back to the original single-table behaviour (``PROCESSED_S3_PATH`` →
``RDS_TABLE``), so existing schedules are unaffected.

Self-contained on purpose: the JDBC and credential handling lives in this file,
so the Glue job's *Script path* is the whole story.

Target tables must already exist — create them with ``sql/warehouse/001_schema.sql``.
``overwrite`` TRUNCATEs rather than dropping, so their column types, indexes and
grants survive every load. Re-running a partition into the append-only
``fact_events`` goes through the staging upsert in ``sql/warehouse/003_upsert.sql``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - environment may not have pyspark installed
    SparkSession = None


try:
    from awsglue.utils import getResolvedOptions
except Exception:
    getResolvedOptions = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# LAKE LAYOUT
# ─────────────────────────────────────────────
# A copy of common/lakehouse.py, on purpose: this script is standalone, and
# common/ belongs to the Lambdas. tests/test_job_self_containment.py pins every
# copy against the original, so a divergence fails the build rather than
# silently writing a day of data into the wrong prefix.

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
            enabled=config.get("METRICS_ENABLED", env_default),
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


#: Secret keys accepted for each setting, in order of preference. RDS rotates
#: secrets in more than one shape depending on how they were created.
SECRET_ALIASES = {
    "host": ["host", "hostname"],
    "port": ["port"],
    "database": ["dbname", "database", "db"],
    "username": ["username", "user"],
    "password": ["password"],
}

DEFAULT_PORT = 5432
DEFAULT_DRIVER = "org.postgresql.Driver"


#: Minimum columns the single-table (legacy) load expects to find.
REQUIRED_COLUMNS = [
    "event_type",
    "product_id",
    "product_name",
    "product_price",
    "customer_id",
    "occurred_at",
    "partition_date",
    "partition_hour",
    "price_category",
]

#: Default warehouse layout: every dataset the lake produces, and the table it
#: lands in. Override per-target via ``RDS_TABLES`` in the config.
DEFAULT_TARGETS = [
    {"dataset": "silver/events", "table": "fact_events", "optional": False},
    {"dataset": "gold/sessions", "table": "fact_sessions"},
    {"dataset": "gold/funnel_daily", "table": "agg_funnel_daily"},
    {"dataset": "gold/orders", "table": "fact_orders"},
    {"dataset": "gold/customer_rfm", "table": "dim_customer_rfm"},
    {"dataset": "gold/product_daily", "table": "agg_product_daily"},
    {"dataset": "gold/anomalies", "table": "fact_anomalies"},
]

#: Datasets the load must not silently skip. Everything else is optional: a
#: gold table this run did not rebuild is a warning, not a failed load.
MANDATORY_DATASETS = ("processed", "silver/events")


def _load_text(path: str) -> str:
    if path.startswith("s3://"):
        bucket_key = path.replace("s3://", "", 1)
        bucket, key = bucket_key.split("/", 1)
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    return Path(path).read_text(encoding="utf-8")


def load_config(path: str) -> dict:
    logger.info("Loading config from %s", path)
    return json.loads(_load_text(path))


def _load_secret(secret_arn: str) -> dict:
    secrets = boto3.client("secretsmanager")
    response = secrets.get_secret_value(SecretId=secret_arn)
    secret_string = response.get("SecretString")
    if not secret_string:
        raise ValueError(f"Secret {secret_arn} has no SecretString")
    return json.loads(secret_string)


def _build_processed_path(config: dict) -> str:
    """The event fact table — ``silver/events``, or wherever the config puts it."""
    return dataset_path(config, "silver/events")


def _dataset_path(config: dict, dataset: str) -> str:
    """Resolve a dataset name to an S3 path.

    Understands the medallion names (``silver/events``, ``gold/orders``) and the
    pre-medallion aliases (``processed``, ``curated/orders``) that older configs
    still use.
    """
    if dataset.startswith("s3://"):
        return dataset if dataset.endswith("/") else dataset + "/"
    if dataset in ("processed", "silver"):
        return _build_processed_path(config)
    if dataset.startswith("curated/"):
        return gold_path(config, dataset.split("/", 1)[1])
    if dataset.split("/", 1)[0] in ZONES:
        return dataset_path(config, dataset)

    return s3_path(config["OUTPUT_BUCKET"], dataset)


def resolve_targets(config: dict) -> List[Dict[str, Any]]:
    """Build the list of ``{dataset, path, table, mode, required_columns}`` to load.

    Precedence: an explicit ``RDS_TABLES`` list wins; otherwise ``RDS_LOAD_ALL``
    loads the default warehouse layout; otherwise the legacy single table.
    """
    default_mode = config.get("RDS_WRITE_MODE", "append")

    if config.get("RDS_TABLES"):
        specs = config["RDS_TABLES"]
    elif config.get("RDS_LOAD_ALL"):
        specs = DEFAULT_TARGETS
    else:
        specs = [{
            "dataset": "processed",
            "table": config.get("RDS_TABLE"),
            "required_columns": REQUIRED_COLUMNS,
        }]

    targets = []
    for spec in specs:
        dataset = spec.get("dataset", "processed")
        table = spec.get("table")
        if not table:
            raise ValueError(f"Target for dataset '{dataset}' has no table name")
        targets.append({
            "dataset": dataset,
            "path": spec.get("path") or _dataset_path(config, dataset),
            "table": table,
            "mode": spec.get("mode", default_mode),
            "required_columns": spec.get("required_columns"),
            "optional": bool(spec.get("optional", dataset not in MANDATORY_DATASETS)),
        })
    return targets


def _from_secret(secret: dict, setting: str):
    for alias in SECRET_ALIASES[setting]:
        if secret.get(alias) not in (None, ""):
            return secret[alias]
    return None


def _resolve_rds_settings(config: dict) -> dict:
    """Build the warehouse connection profile from the config, the secret, or both.

    A per-target table list makes ``RDS_TABLE`` optional — there is nothing to
    name when every target names its own table.
    """
    config = config or {}

    secret = {}
    if config.get("RDS_SECRET_ARN"):
        secret = _load_secret(config["RDS_SECRET_ARN"]) or {}

    def pick(setting: str, key: str, default=None):
        value = config.get(f"RDS_{key}")
        if value in (None, ""):
            value = _from_secret(secret, setting)
        return default if value in (None, "") else value

    settings = {
        "host": pick("host", "HOST"),
        "port": str(pick("port", "PORT", DEFAULT_PORT)),
        "database": pick("database", "DATABASE"),
        "username": pick("username", "USERNAME"),
        "password": pick("password", "PASSWORD"),
        "table": config.get("RDS_TABLE"),
        "schema": config.get("RDS_SCHEMA"),
        "driver": config.get("RDS_JDBC_DRIVER", DEFAULT_DRIVER),
        "sslmode": config.get("RDS_SSLMODE", "require"),
        "write_mode": config.get("RDS_WRITE_MODE", "append"),
        # Batched inserts turn one round-trip per row into one per batch — the
        # difference between minutes and hours on a few million rows.
        "batchsize": int(config.get("RDS_BATCH_SIZE", 10000)),
        "num_partitions": int(config.get("RDS_NUM_PARTITIONS", 8)),
        # `truncate` keeps the table (and its grants/indexes) on an overwrite
        # instead of letting Spark DROP and recreate it with guessed types.
        "truncate": bool(config.get("RDS_TRUNCATE", True)),
    }

    required = {
        "HOST": "host", "PORT": "port", "DATABASE": "database",
        "USERNAME": "username", "PASSWORD": "password",
    }
    if not (config.get("RDS_TABLES") or config.get("RDS_LOAD_ALL")):
        required["TABLE"] = "table"

    missing = [f"RDS_{name}" for name, field in required.items() if settings[field] in (None, "")]
    if missing:
        raise ValueError(f"Missing RDS settings: {missing}")

    return settings


def _build_jdbc_url(settings: dict) -> str:
    return (
        f"jdbc:postgresql://{settings['host']}:{settings['port']}/{settings['database']}"
        f"?sslmode={settings.get('sslmode', 'require')}"
    )


def _qualified(settings: dict, table: str) -> str:
    """``schema.table`` when a schema is configured and the name is unqualified."""
    schema = settings.get("schema")
    if schema and "." not in table:
        return f"{schema}.{table}"
    return table


def _read_processed_dataset(spark: "SparkSession", processed_path: str):
    logger.info("Reading processed Parquet from %s", processed_path)
    dataframe = spark.read.parquet(processed_path)
    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Processed dataset is missing columns: {missing}")
    return dataframe.select(*REQUIRED_COLUMNS)


def _read_dataset(spark: "SparkSession", path: str, required_columns: Optional[List[str]] = None):
    """Read any Parquet dataset, optionally projecting a required column list."""
    logger.info("Reading Parquet from %s", path)
    dataframe = spark.read.parquet(path)

    if required_columns:
        missing = [column for column in required_columns if column not in dataframe.columns]
        if missing:
            raise ValueError(f"Dataset {path} is missing columns: {missing}")
        return dataframe.select(*required_columns)

    return dataframe


def _write_to_rds(dataframe, settings: dict, table: Optional[str] = None, mode: Optional[str] = None) -> None:
    jdbc_url = _build_jdbc_url(settings)
    table = table or settings["table"]
    mode = mode or settings["write_mode"]
    logger.info("Writing %s rows into %s (mode=%s)", dataframe.count(), table, mode)

    writer = (
        dataframe.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", _qualified(settings, table))
        .option("user", settings["username"])
        .option("password", settings["password"])
        .option("driver", settings["driver"])
        .option("batchsize", settings.get("batchsize", 10000))
        .option("numPartitions", settings.get("num_partitions", 8))
    )
    if mode == "overwrite" and settings.get("truncate", True):
        writer = writer.option("truncate", "true")

    writer.mode(mode).save()


def load_targets(spark: "SparkSession", targets: List[Dict[str, Any]], settings: dict) -> List[Dict[str, Any]]:
    """Load every target, reporting per-table outcomes.

    A missing *optional* dataset (a curated table the processing job did not
    produce this run) is skipped with a warning; a failure on the mandatory
    ``processed`` target propagates.
    """
    results = []
    for target in targets:
        entry = {"dataset": target["dataset"], "table": target["table"], "path": target["path"]}
        try:
            dataframe = _read_dataset(spark, target["path"], target.get("required_columns"))
            row_count = dataframe.count()
            if row_count == 0:
                logger.warning("No rows at %s — skipping %s", target["path"], target["table"])
                results.append({**entry, "status": "skipped", "reason": "empty", "rows_loaded": 0})
                continue

            _write_to_rds(dataframe, settings, table=target["table"], mode=target["mode"])
            results.append({**entry, "status": "loaded", "rows_loaded": row_count, "mode": target["mode"]})
        except Exception as exc:  # noqa: BLE001
            if not target.get("optional"):
                raise
            logger.warning("Skipping optional target %s: %s", target["table"], exc)
            results.append({**entry, "status": "skipped", "reason": str(exc), "rows_loaded": 0})
    return results


def _parse_args() -> dict:
    if getResolvedOptions and len(os.sys.argv) > 1 and "JOB_NAME" in os.sys.argv:
        resolved = getResolvedOptions(os.sys.argv, ["JOB_NAME", "CONFIG_PATH"])
        return {"config": resolved["CONFIG_PATH"], "mode": "glue"}

    parser = argparse.ArgumentParser(description="Load processed Glue data into PostgreSQL RDS")
    parser.add_argument("--config", required=True, help="Path to the Glue config JSON file or s3:// path")
    args = parser.parse_args()
    return {"config": args.config, "mode": "local"}


def main() -> None:
    args = _parse_args()
    config = load_config(args["config"])

    rds_settings = _resolve_rds_settings(config)
    targets = resolve_targets(config)
    logger.info("Loading %d target(s): %s", len(targets), [t["table"] for t in targets])

    started = datetime.now(timezone.utc)
    spark = SparkSession.builder.appName(config.get("JOB_NAME", "ecommerce-rds-load")).getOrCreate()

    results = load_targets(spark, targets, rds_settings)
    total_rows = sum(item["rows_loaded"] for item in results)

    try:
        metrics = JobMetrics.from_config(config, stage="glue_rds_load")
        metrics.count("RowsLoaded", total_rows)
        metrics.count("TablesLoaded", sum(1 for item in results if item["status"] == "loaded"))
        metrics.count("TablesSkipped", sum(1 for item in results if item["status"] == "skipped"))
        metrics.gauge("JobDurationSeconds", (datetime.now(timezone.utc) - started).total_seconds(), unit="Seconds")
        for item in results:
            metrics.count("RowsLoadedByTable", item["rows_loaded"], Table=item["table"])
        metrics.flush()
    except Exception as exc:  # noqa: BLE001 - metrics must never fail a load
        logger.warning("Metrics emission skipped: %s", exc)

    print(
        json.dumps(
            {
                "status": "success",
                "mode": args["mode"],
                "targets": results,
                "rows_loaded": total_rows,
                "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
