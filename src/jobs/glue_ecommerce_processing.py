"""
E-commerce data processing job with quality checks, cleaning, and transformations.

Architecture:
  CONFIG (S3) -> Glue Job -> S3 RAW -> Processing -> S3 PROCESSED

Features:
  - Config-driven pipeline
  - Data validation & cleaning
  - Deduplication
  - S3 read/write

IMPORTANT:
  Ne pas embarquer boto3/botocore dans --extra-py-files (dependencies.zip).
  Glue fournit déjà une version complète et fonctionnelle de boto3/botocore.
  Si dependencies.zip contient boto3/botocore, cela écrase la version native
  et provoque des erreurs du type: DataNotFoundError: Unable to load data for: endpoints
"""

import sys
import json
import logging
import boto3
from pathlib import Path
from datetime import datetime
from awsglue.utils import getResolvedOptions

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
    """
    occurred_at = record.get("occurred_at", "")
    
    # Parse datetime and extract date and hour
    try:
        dt = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        partition_date = dt.strftime("%Y-%m-%d")
        partition_hour = dt.strftime("%H")
    except:
        partition_date = "unknown"
        partition_hour = "unknown"
    
    price = _clean_numeric(record.get("product", {}).get("price"))
    
    if price is None:
        price_category = "unknown"
    elif price < 50:
        price_category = "budget"
    elif price < 200:
        price_category = "mid"
    else:
        price_category = "premium"
    
    enriched = {
        "event_type": record.get("event_type"),
        "product_id": record.get("product", {}).get("product_id"),
        "product_name": _clean_string(record.get("product", {}).get("name")),
        "product_price": price,
        "customer_id": record.get("customer", {}).get("customer_id"),
        "occurred_at": record.get("occurred_at"),
        "partition_date": partition_date,
        "partition_hour": partition_hour,
        "price_category": price_category,
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
    # raw/ peut contenir plusieurs fichiers .json; on les collecte tous avant traitement.
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
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )


def write_parquet(bucket: str, output_prefix: str, rows: list[dict]) -> str:
    """Write processed records as a Parquet dataset in S3."""
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
    dataframe = spark.createDataFrame(rows, schema=schema)

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
                except:
                    pass
    return records


def write_local_json(file_path: str, data: dict) -> None:
    """Write JSON to a local file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run_job(input_prefix: str = None, output_prefix: str = None, bucket: str = None, local_fs: bool = False) -> dict:
    """
    Process e-commerce records.
    
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
        
        dedup_key = f"{enriched['event_type']}-{enriched['product_id']}-{enriched['customer_id']}"
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
    
    logger.info(f"Résultat: {json.dumps(result, indent=2)}")
    return result

# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_PATH"])

    config_path = args["CONFIG_PATH"]
    config = load_config(config_path)

    bucket = config.get("OUTPUT_BUCKET")
    input_prefix = config.get("RAW_PREFIX", "raw/")
    output_prefix = config.get("PROCESSED_PREFIX", "processed/")

    logger.info(f"Bucket        : {bucket}")
    logger.info(f"Input prefix  : {input_prefix}")
    logger.info(f"Output prefix : {output_prefix}")

    result = run_job(
        input_prefix=input_prefix,
        output_prefix=output_prefix,
        bucket=bucket,
        local_fs=False,
    )

    print(json.dumps(result, indent=2))