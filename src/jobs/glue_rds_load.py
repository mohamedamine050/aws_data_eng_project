from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None

try:
    from awsglue.utils import getResolvedOptions
except Exception:
    getResolvedOptions = None


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# LAKE LAYOUT
# ============================================================

ZONES = (
    "landing",
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "quality",
)

DEFAULT_ZONE_PREFIXES = {
    "landing": "landing/",
    "bronze": "bronze/",
    "silver": "silver/",
    "gold": "gold/",
    "quarantine": "quarantine/",
    "quality": "quality/",
}

LEGACY_ZONE_KEYS = {
    "gold": "CURATED_PREFIX",
}

LEGACY_DATASET_KEYS = {
    "bronze/events": ("RAW_PREFIX", "RAW_S3_PATH"),
    "silver/events": ("PROCESSED_PREFIX", "PROCESSED_S3_PATH"),
    "quarantine/events": ("REJECTED_PREFIX", None),
}

GOLD_DATASETS = {
    "sessions": ["partition_date"],
    "funnel_daily": ["partition_date"],
    "orders": ["partition_date"],
    "customer_rfm": None,
    "product_daily": ["partition_date"],
    "anomalies": ["partition_date"],
}


# ============================================================
# PATH HELPERS
# ============================================================

def _norm(prefix: str) -> str:
    prefix = str(prefix).strip().strip("/")
    return f"{prefix}/" if prefix else ""


def zone_prefix(config: dict, zone: str) -> str:
    if zone not in DEFAULT_ZONE_PREFIXES:
        raise ValueError(
            f"Unknown zone '{zone}'"
        )

    for key in (
        f"{zone.upper()}_PREFIX",
        LEGACY_ZONE_KEYS.get(zone),
    ):
        if key and config.get(key) is not None:
            return _norm(config[key])

    return DEFAULT_ZONE_PREFIXES[zone]


def _split(dataset: str) -> Tuple[str, str]:
    zone, _, name = dataset.partition("/")
    return zone, name.strip("/")


def dataset_prefix(config: dict, dataset: str) -> str:
    zone, name = _split(dataset)

    if not name:
        return zone_prefix(config, zone)

    derived = (
        f"{zone.upper()}_"
        f"{name.upper().replace('/', '_')}_PREFIX"
    )

    legacy = (
        LEGACY_DATASET_KEYS.get(
            f"{zone}/{name}"
        )
        or (None, None)
    )[0]

    for key in (derived, legacy):
        if key and config.get(key) is not None:
            return _norm(config[key])

    return f"{zone_prefix(config, zone)}{name}/"


def s3_path(bucket: str, prefix: str) -> str:
    return f"s3://{bucket}/{_norm(prefix)}"


def zone_path(config: dict, zone: str) -> str:
    return s3_path(
        config["OUTPUT_BUCKET"],
        zone_prefix(config, zone),
    )


def dataset_path(config: dict, dataset: str) -> str:

    if dataset.startswith("s3://"):
        return (
            dataset
            if dataset.endswith("/")
            else dataset + "/"
        )

    zone, name = _split(dataset)

    derived = (
        f"{zone.upper()}_"
        f"{name.upper().replace('/', '_')}_S3_PATH"
        if name
        else f"{zone.upper()}_S3_PATH"
    )

    legacy = (
        LEGACY_DATASET_KEYS.get(dataset)
        or (None, None)
    )[1]

    for key in (derived, legacy):
        if key and config.get(key):
            value = str(config[key])
            return (
                value
                if value.endswith("/")
                else value + "/"
            )

    return s3_path(
        config["OUTPUT_BUCKET"],
        dataset_prefix(config, dataset),
    )


def gold_path(config: dict, name: str) -> str:
    return dataset_path(
        config,
        f"gold/{name}",
    )


def _dataset_path(
    config: dict,
    dataset: str,
) -> str:

    if dataset.startswith("s3://"):
        return (
            dataset
            if dataset.endswith("/")
            else dataset + "/"
        )

    if dataset in ("processed", "silver"):
        return dataset_path(
            config,
            "silver/events",
        )

    if dataset.startswith("curated/"):
        return gold_path(
            config,
            dataset.split("/", 1)[1],
        )

    zone = dataset.split("/", 1)[0]

    if zone in ZONES:
        return dataset_path(
            config,
            dataset,
        )

    return s3_path(
        config["OUTPUT_BUCKET"],
        dataset,
    )


# ============================================================
# CONFIG
# ============================================================

def _load_text(path: str) -> str:

    if path.startswith("s3://"):

        bucket_key = path.replace(
            "s3://",
            "",
            1,
        )

        bucket, key = bucket_key.split(
            "/",
            1,
        )

        s3 = boto3.client("s3")

        obj = s3.get_object(
            Bucket=bucket,
            Key=key,
        )

        return obj["Body"].read().decode(
            "utf-8"
        )

    return Path(path).read_text(
        encoding="utf-8"
    )


def load_config(path: str) -> dict:
    logger.info(
        "Loading config from %s",
        path,
    )

    return json.loads(
        _load_text(path)
    )


# ============================================================
# SECRETS MANAGER
# ============================================================

SECRET_ALIASES = {
    "host": ["host", "hostname"],
    "port": ["port"],
    "database": [
        "dbname",
        "database",
        "db",
    ],
    "username": [
        "username",
        "user",
    ],
    "password": ["password"],
}

DEFAULT_PORT = 5432
DEFAULT_DRIVER = "org.postgresql.Driver"


def _load_secret(secret_arn: str) -> dict:

    client = boto3.client(
        "secretsmanager"
    )

    response = client.get_secret_value(
        SecretId=secret_arn
    )

    secret_string = response.get(
        "SecretString"
    )

    if not secret_string:
        raise ValueError(
            f"Secret {secret_arn} has no SecretString"
        )

    return json.loads(secret_string)


def _from_secret(
    secret: dict,
    setting: str,
):

    for alias in SECRET_ALIASES[setting]:
        if secret.get(alias) not in (
            None,
            "",
        ):
            return secret[alias]

    return None


def _resolve_rds_settings(
    config: dict,
) -> dict:

    secret = {}

    if config.get("RDS_SECRET_ARN"):
        secret = _load_secret(
            config["RDS_SECRET_ARN"]
        )

    def pick(
        setting: str,
        key: str,
        default=None,
    ):

        value = config.get(
            f"RDS_{key}"
        )

        if value in (None, ""):
            value = _from_secret(
                secret,
                setting,
            )

        return (
            default
            if value in (None, "")
            else value
        )

    settings = {
        "host": pick(
            "host",
            "HOST",
        ),

        "port": str(
            pick(
                "port",
                "PORT",
                DEFAULT_PORT,
            )
        ),

        "database": pick(
            "database",
            "DATABASE",
        ),

        "username": pick(
            "username",
            "USERNAME",
        ),

        "password": pick(
            "password",
            "PASSWORD",
        ),

        # IMPORTANT
        # Default schema = analytics
        "schema": config.get(
            "RDS_SCHEMA",
            "analytics",
        ),

        "driver": config.get(
            "RDS_JDBC_DRIVER",
            DEFAULT_DRIVER,
        ),

        "sslmode": config.get(
            "RDS_SSLMODE",
            "require",
        ),

        "write_mode": config.get(
            "RDS_WRITE_MODE",
            "append",
        ),

        "batchsize": int(
            config.get(
                "RDS_BATCH_SIZE",
                10000,
            )
        ),

        "num_partitions": int(
            config.get(
                "RDS_NUM_PARTITIONS",
                8,
            )
        ),

        "truncate": (
            str(
                config.get(
                    "RDS_TRUNCATE",
                    "true",
                )
            ).lower()
            not in (
                "false",
                "0",
                "no",
            )
        ),
    }

    required = [
        "host",
        "database",
        "username",
        "password",
    ]

    missing = [
        key
        for key in required
        if not settings.get(key)
    ]

    if missing:
        raise ValueError(
            f"Missing RDS settings: {missing}"
        )

    return settings


# ============================================================
# JDBC
# ============================================================

def _build_jdbc_url(
    settings: dict,
) -> str:

    return (
        f"jdbc:postgresql://"
        f"{settings['host']}:"
        f"{settings['port']}/"
        f"{settings['database']}"
        f"?sslmode="
        f"{settings.get('sslmode', 'require')}"
    )


def _qualified(
    settings: dict,
    table: str,
) -> str:

    schema = settings.get(
        "schema"
    )

    if schema and "." not in table:
        return f"{schema}.{table}"

    return table


# ============================================================
# CREATE POSTGRESQL SCHEMA
# ============================================================

def _validate_identifier(
    identifier: str,
) -> None:

    if not identifier:
        raise ValueError(
            "PostgreSQL identifier cannot be empty"
        )

    allowed = (
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789_"
    )

    if not all(
        char in allowed
        for char in identifier
    ):
        raise ValueError(
            f"Invalid PostgreSQL identifier: "
            f"{identifier}"
        )


def _ensure_schema(
    spark: "SparkSession",
    settings: dict,
) -> None:

    schema = settings.get(
        "schema"
    )

    if not schema:
        logger.warning(
            "RDS_SCHEMA is empty. "
            "Schema creation skipped."
        )
        return

    _validate_identifier(
        schema
    )

    jdbc_url = _build_jdbc_url(
        settings
    )

    connection = None
    statement = None

    try:

        logger.info(
            "Creating/checking PostgreSQL schema '%s'",
            schema,
        )

        DriverManager = (
            spark.sparkContext
            ._gateway
            .jvm
            .java.sql
            .DriverManager
        )

        connection = (
            DriverManager.getConnection(
                jdbc_url,
                settings["username"],
                settings["password"],
            )
        )

        connection.setAutoCommit(
            True
        )

        statement = (
            connection.createStatement()
        )

        sql = (
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
        )

        logger.info(
            "Executing PostgreSQL: %s",
            sql,
        )

        statement.executeUpdate(
            sql
        )

        logger.info(
            "Schema '%s' is ready.",
            schema,
        )

    except Exception as exc:

        logger.error(
            "Unable to create PostgreSQL schema '%s': %s",
            schema,
            exc,
        )

        raise

    finally:

        if statement is not None:
            try:
                statement.close()
            except Exception:
                pass

        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


# ============================================================
# TARGETS
# ============================================================

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


DEFAULT_TARGETS = [
    {
        "dataset": "silver/events",
        "table": "fact_events",
        "mode": "append",
        "optional": False,
    },
    {
        "dataset": "gold/sessions",
        "table": "fact_sessions",
        "mode": "overwrite",
    },
    {
        "dataset": "gold/funnel_daily",
        "table": "agg_funnel_daily",
        "mode": "overwrite",
    },
    {
        "dataset": "gold/orders",
        "table": "fact_orders",
        "mode": "overwrite",
    },
    {
        "dataset": "gold/customer_rfm",
        "table": "dim_customer_rfm",
        "mode": "overwrite",
    },
    {
        "dataset": "gold/product_daily",
        "table": "agg_product_daily",
        "mode": "overwrite",
    },
    {
        "dataset": "gold/anomalies",
        "table": "fact_anomalies",
        "mode": "overwrite",
    },
]


MANDATORY_DATASETS = (
    "processed",
    "silver/events",
)


def resolve_targets(
    config: dict,
) -> List[Dict[str, Any]]:

    default_mode = config.get(
        "RDS_WRITE_MODE",
        "append",
    )

    if config.get("RDS_TABLES"):

        specs = config["RDS_TABLES"]

    elif config.get("RDS_TABLE"):

        specs = [
            {
                "dataset": "processed",
                "table": config["RDS_TABLE"],
                "required_columns":
                    REQUIRED_COLUMNS,
                "optional": False,
            }
        ]

    else:

        logger.info(
            "No RDS_TABLES/RDS_TABLE found. "
            "Using default warehouse layout."
        )

        specs = DEFAULT_TARGETS

    targets = []

    for spec in specs:

        dataset = spec.get(
            "dataset",
            "processed",
        )

        table = spec.get(
            "table"
        )

        if not table:
            raise ValueError(
                f"Target for {dataset} "
                f"has no table name"
            )

        targets.append(
            {
                "dataset": dataset,

                "path": (
                    spec.get("path")
                    or _dataset_path(
                        config,
                        dataset,
                    )
                ),

                "table": table,

                "mode": spec.get(
                    "mode",
                    default_mode,
                ),

                "required_columns":
                    spec.get(
                        "required_columns"
                    ),

                "optional": bool(
                    spec.get(
                        "optional",
                        dataset
                        not in MANDATORY_DATASETS,
                    )
                ),
            }
        )

    return targets


# ============================================================
# READ PARQUET
# ============================================================

def _read_dataset(
    spark: "SparkSession",
    path: str,
    required_columns: Optional[
        List[str]
    ] = None,
):

    logger.info(
        "Reading Parquet from %s",
        path,
    )

    dataframe = spark.read.parquet(
        path
    )

    if required_columns:

        missing = [
            column
            for column in required_columns
            if column not in dataframe.columns
        ]

        if missing:
            raise ValueError(
                f"Dataset {path} is missing "
                f"columns: {missing}"
            )

        dataframe = dataframe.select(
            *required_columns
        )

    return dataframe


# ============================================================
# WRITE JDBC
# ============================================================

def _write_to_rds(
    dataframe,
    settings: dict,
    table: str,
    mode: str,
) -> None:

    jdbc_url = _build_jdbc_url(
        settings
    )

    qualified_table = _qualified(
        settings,
        table,
    )

    row_count = dataframe.count()

    logger.info(
        "Writing %d rows to %s",
        row_count,
        qualified_table,
    )

    logger.info(
        "Mode: %s",
        mode,
    )

    writer = (
        dataframe.write
        .format("jdbc")
        .option(
            "url",
            jdbc_url,
        )
        .option(
            "dbtable",
            qualified_table,
        )
        .option(
            "user",
            settings["username"],
        )
        .option(
            "password",
            settings["password"],
        )
        .option(
            "driver",
            settings["driver"],
        )
        .option(
            "batchsize",
            str(
                settings.get(
                    "batchsize",
                    10000,
                )
            ),
        )
        .option(
            "numPartitions",
            str(
                settings.get(
                    "num_partitions",
                    8,
                )
            ),
        )
    )

    if (
        mode == "overwrite"
        and settings.get(
            "truncate",
            True,
        )
    ):
        writer = writer.option(
            "truncate",
            "true",
        )

    writer.mode(
        mode
    ).save()

    logger.info(
        "SUCCESS: %d rows -> %s",
        row_count,
        qualified_table,
    )


# ============================================================
# LOAD TARGETS
# ============================================================

def load_targets(
    spark: "SparkSession",
    targets: List[Dict[str, Any]],
    settings: dict,
) -> List[Dict[str, Any]]:

    results = []

    for target in targets:

        entry = {
            "dataset":
                target["dataset"],

            "table":
                _qualified(
                    settings,
                    target["table"],
                ),

            "path":
                target["path"],
        }

        try:

            dataframe = _read_dataset(
                spark,
                target["path"],
                target.get(
                    "required_columns"
                ),
            )

            row_count = dataframe.count()

            if row_count == 0:

                logger.warning(
                    "Dataset %s is empty. "
                    "Skipping %s.",
                    target["path"],
                    target["table"],
                )

                results.append({
                    **entry,
                    "status": "skipped",
                    "reason": "empty",
                    "rows_loaded": 0,
                })

                continue

            _write_to_rds(
                dataframe,
                settings,
                target["table"],
                target["mode"],
            )

            results.append({
                **entry,
                "status": "loaded",
                "rows_loaded": row_count,
                "mode": target["mode"],
            })

        except Exception as exc:

            if not target.get(
                "optional",
                False,
            ):
                raise

            logger.warning(
                "Optional target %s skipped: %s",
                target["table"],
                exc,
            )

            results.append({
                **entry,
                "status": "skipped",
                "reason": str(exc),
                "rows_loaded": 0,
            })

    return results


# ============================================================
# GLUE ARGUMENTS
# ============================================================

def _job_name_in(
    argv: List[str],
) -> bool:

    return any(
        arg.lstrip("-").split(
            "=",
            1,
        )[0] == "JOB_NAME"
        for arg in argv
    )


def _parse_args() -> dict:

    if (
        getResolvedOptions
        and _job_name_in(
            sys.argv[1:]
        )
    ):

        resolved = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "CONFIG_PATH",
            ],
        )

        return {
            "config":
                resolved["CONFIG_PATH"],

            "mode":
                "glue",

            "job_name":
                resolved.get(
                    "JOB_NAME"
                ),
        }

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        required=True,
    )

    args = parser.parse_args()

    return {
        "config": args.config,
        "mode": "local",
        "job_name": None,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    # --------------------------------------------------------
    # 1. Arguments
    # --------------------------------------------------------

    args = _parse_args()

    # --------------------------------------------------------
    # 2. Config
    # --------------------------------------------------------

    config = load_config(
        args["config"]
    )

    if args.get("job_name"):
        config["JOB_NAME"] = (
            args["job_name"]
        )

    config.setdefault(
        "CONFIG_PATH",
        args["config"],
    )

    # --------------------------------------------------------
    # 3. RDS settings
    # --------------------------------------------------------

    settings = _resolve_rds_settings(
        config
    )

    logger.info(
        "============================================"
    )

    logger.info(
        "RDS CONFIGURATION"
    )

    logger.info(
        "Host     : %s",
        settings["host"],
    )

    logger.info(
        "Port     : %s",
        settings["port"],
    )

    logger.info(
        "Database : %s",
        settings["database"],
    )

    logger.info(
        "Schema   : %s",
        settings["schema"],
    )

    logger.info(
        "============================================"
    )

    # --------------------------------------------------------
    # 4. Targets
    # --------------------------------------------------------

    targets = resolve_targets(
        config
    )

    logger.info(
        "Targets:"
    )

    for target in targets:
        logger.info(
            "  %s -> %s [%s]",
            target["path"],
            _qualified(
                settings,
                target["table"],
            ),
            target["mode"],
        )

    # --------------------------------------------------------
    # 5. Spark
    # --------------------------------------------------------

    started = datetime.now(
        timezone.utc
    )

    spark = (
        SparkSession.builder
        .appName(
            config.get(
                "JOB_NAME",
                "glue-rds-load",
            )
        )
        .getOrCreate()
    )

    try:

        # ====================================================
        # IMPORTANT FIX
        # ====================================================
        #
        # BEFORE Spark JDBC .save():
        #
        # CREATE SCHEMA IF NOT EXISTS analytics
        #
        # ====================================================

        _ensure_schema(
            spark,
            settings,
        )

        # ----------------------------------------------------
        # 6. Load all datasets
        # ----------------------------------------------------

        results = load_targets(
            spark,
            targets,
            settings,
        )

        total_rows = sum(
            item["rows_loaded"]
            for item in results
        )

        duration = (
            datetime.now(
                timezone.utc
            )
            - started
        ).total_seconds()

        # ----------------------------------------------------
        # 7. Result
        # ----------------------------------------------------

        result = {
            "status": "success",
            "job": config.get(
                "JOB_NAME"
            ),
            "schema": settings[
                "schema"
            ],
            "targets": results,
            "rows_loaded": total_rows,
            "duration_seconds": round(
                duration,
                2,
            ),
        }

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

    finally:

        spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
