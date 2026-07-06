"""Glue job that loads the processed Parquet dataset into PostgreSQL RDS.

The job reads the processed Athena-ready dataset from S3, then writes it to
an RDS PostgreSQL table using Spark JDBC. The target table must already exist
or be created by your infrastructure code (Terraform / SQL).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

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
    if config.get("PROCESSED_S3_PATH"):
        return config["PROCESSED_S3_PATH"]

    output_bucket = config["OUTPUT_BUCKET"]
    processed_prefix = config.get("PROCESSED_PREFIX", "processed/")
    return f"s3://{output_bucket}/{processed_prefix.rstrip('/')}/"


def _resolve_rds_settings(config: dict) -> dict:
    secret_data = {}
    if config.get("RDS_SECRET_ARN"):
        secret_data = _load_secret(config["RDS_SECRET_ARN"])

    host = config.get("RDS_HOST") or secret_data.get("host") or secret_data.get("hostname")
    port = config.get("RDS_PORT") or secret_data.get("port") or 5432
    database = config.get("RDS_DATABASE") or secret_data.get("dbname") or secret_data.get("database")
    username = config.get("RDS_USERNAME") or secret_data.get("username") or secret_data.get("user")
    password = config.get("RDS_PASSWORD") or secret_data.get("password")
    table = config["RDS_TABLE"]

    missing = [
        name
        for name, value in [
            ("RDS_HOST", host),
            ("RDS_PORT", port),
            ("RDS_DATABASE", database),
            ("RDS_USERNAME", username),
            ("RDS_PASSWORD", password),
            ("RDS_TABLE", table),
        ]
        if value in (None, "")
    ]
    if missing:
        raise ValueError(f"Missing RDS settings: {missing}")

    return {
        "host": host,
        "port": str(port),
        "database": database,
        "username": username,
        "password": password,
        "table": table,
        "driver": config.get("RDS_JDBC_DRIVER", "org.postgresql.Driver"),
        "sslmode": config.get("RDS_SSLMODE", "require"),
        "write_mode": config.get("RDS_WRITE_MODE", "append"),
    }


def _build_jdbc_url(settings: dict) -> str:
    return (
        f"jdbc:postgresql://{settings['host']}:{settings['port']}/{settings['database']}"
        f"?sslmode={settings['sslmode']}"
    )


def _read_processed_dataset(spark: SparkSession, processed_path: str):
    logger.info("Reading processed Parquet from %s", processed_path)
    dataframe = spark.read.parquet(processed_path)
    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Processed dataset is missing columns: {missing}")
    return dataframe.select(*REQUIRED_COLUMNS)


def _write_to_rds(dataframe, settings: dict) -> None:
    jdbc_url = _build_jdbc_url(settings)
    logger.info("Writing %s rows into %s", dataframe.count(), settings["table"])
    (
        dataframe.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", settings["table"])
        .option("user", settings["username"])
        .option("password", settings["password"])
        .option("driver", settings["driver"])
        .mode(settings["write_mode"])
        .save()
    )


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

    processed_path = _build_processed_path(config)
    rds_settings = _resolve_rds_settings(config)

    spark = SparkSession.builder.getOrCreate()
    dataframe = _read_processed_dataset(spark, processed_path)

    row_count = dataframe.count()
    if row_count == 0:
        logger.warning("No processed rows found at %s", processed_path)
        return

    _write_to_rds(dataframe, rds_settings)

    print(
        json.dumps(
            {
                "status": "success",
                "mode": args["mode"],
                "source_path": processed_path,
                "target_table": rds_settings["table"],
                "rows_loaded": row_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()