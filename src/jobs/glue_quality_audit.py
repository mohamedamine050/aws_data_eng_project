"""Glue job 3 — **the quality gate**: audit silver, quarantine what fails.

    s3://<lake>/silver/events/ ──> Glue Job ──┬──> quality/dt=…/audit-*.json
                                              ├──> quarantine/audit/dt=…/
                                              └──> CloudWatch + (optional) job failure

The Lambda already rejects records that break a rule at ingest time. This job
answers the question the Lambda cannot, because it only ever sees one message:
*is the dataset as a whole trustworthy today?* Duplicate rates, null rates,
revenue that does not add up, a partition that is suspiciously small — all of
them are properties of a batch, not of a row.

Checks are data, not code
-------------------------
Every check is ``{name, expr, severity, description}`` where ``expr`` is a
Spark SQL predicate that is **true when the row is good**. Adding a rule is one
entry in a list — in the config, if you do not want to redeploy:

    "QUALITY_CHECKS": [
      {"name": "eur_only", "expr": "currency = 'EUR'", "severity": "warn"}
    ]

An ``error`` check failing sends the offending rows to ``quarantine/audit/``
with the names of the checks they broke, so a bad day can be diagnosed and
replayed. A ``warn`` check only moves the score.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

from common import lakehouse
from common.metrics import MetricsEmitter

try:
    from awsglue.utils import getResolvedOptions
except Exception:  # pragma: no cover - allows local runs without the Glue libs
    getResolvedOptions = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# One file is one Glue job, complete: the config loader below is this script's own.

def load_config(path: str, s3: Any = None) -> dict:
    """Read a job's JSON config, from S3 or from disk."""
    LOGGER.info("Loading config from %s", path)
    if path.startswith("s3://"):
        bucket, _, key = path[len("s3://"):].partition("/")
        s3 = s3 or boto3.client("s3")
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))

    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


#: The baseline audit. ``expr`` is true for a *valid* row; NULL-safe on purpose,
#: because ``NULL = 'x'`` is NULL and would silently pass.
DEFAULT_CHECKS: List[Dict[str, str]] = [
    {"name": "idempotency_key_present", "severity": "error",
     "expr": "idempotency_key IS NOT NULL AND length(idempotency_key) > 0",
     "description": "Without it the pipeline cannot deduplicate."},
    {"name": "event_type_known", "severity": "error",
     "expr": "event_type IN ('product_viewed','add_to_cart','checkout_started','order_placed',"
             "'order_cancelled','payment_failed','refund_issued','search_performed','product_removed')",
     "description": "An unknown event type means the producer changed under us."},
    {"name": "occurred_at_parsed", "severity": "error",
     "expr": "occurred_ts IS NOT NULL",
     "description": "An unparseable timestamp lands the row in the wrong partition."},
    {"name": "not_in_the_future", "severity": "error",
     "expr": "occurred_ts <= current_timestamp() + interval 1 hour",
     "description": "A future event is a clock problem, not a sale."},
    {"name": "product_id_present", "severity": "error",
     "expr": "product_id IS NOT NULL",
     "description": "An event with no product cannot be attributed to anything."},
    {"name": "price_non_negative", "severity": "error",
     "expr": "product_price IS NULL OR product_price >= 0",
     "description": "A negative price is a parsing accident."},
    {"name": "order_has_id", "severity": "error",
     "expr": "event_type NOT IN ('order_placed','order_cancelled','refund_issued') OR order_id IS NOT NULL",
     "description": "Revenue that cannot be traced to an order cannot be reconciled."},
    {"name": "amount_adds_up", "severity": "error",
     "expr": "net_amount IS NULL OR gross_amount IS NULL OR discount_amount IS NULL "
             "OR abs(gross_amount - discount_amount - net_amount) <= 0.01",
     "description": "Basket economics must be internally consistent."},
    {"name": "quantity_sane", "severity": "warn",
     "expr": "quantity IS NULL OR (quantity > 0 AND quantity <= 100)",
     "description": "Plausible basket sizes; above this it is usually a test order."},
    {"name": "customer_id_present", "severity": "warn",
     "expr": "customer_id IS NOT NULL",
     "description": "Anonymous browsing is legitimate, but too much of it breaks RFM."},
    {"name": "channel_known", "severity": "warn",
     "expr": "channel IN ('web','mobile_app','mobile_web','marketplace','store','api')",
     "description": "A new channel needs a decision, not a silent pass."},
    {"name": "currency_known", "severity": "warn",
     "expr": "currency IS NULL OR currency IN ('EUR','USD','GBP','CHF','CAD')",
     "description": "An unexpected currency makes every revenue sum wrong."},
]

#: Default gates applied to the audit result.
DEFAULT_THRESHOLDS = {
    "min_pass_pct": 99.0,
    "warn_pass_pct": 99.9,
    "max_duplicate_pct": 1.0,
    "min_records": 1,
}


# -------------------------------------------------
# PURE LOGIC (unit-testable without Spark)
# -------------------------------------------------

def resolve_checks(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """The baseline checks plus the config's, with ``QUALITY_CHECKS_DISABLED`` removed.

    Config checks override a baseline check of the same name, which is how you
    relax one rule without copying the other eleven.
    """
    disabled = set(config.get("QUALITY_CHECKS_DISABLED") or [])
    merged: Dict[str, Dict[str, str]] = {check["name"]: dict(check) for check in DEFAULT_CHECKS}

    for check in config.get("QUALITY_CHECKS") or []:
        if not check.get("name") or not check.get("expr"):
            raise ValueError(f"A quality check needs a 'name' and an 'expr': {check}")
        merged[check["name"]] = {"severity": "error", "description": "", **check}

    return [check for name, check in merged.items() if name not in disabled]


def score(results: List[Dict[str, Any]], total: int) -> Dict[str, Any]:
    """Turn per-check failure counts into the batch's headline numbers."""
    errors = [r for r in results if r["severity"] == "error"]
    warnings = [r for r in results if r["severity"] == "warn"]
    failing_rows = max((r["failed"] for r in errors), default=0)

    return {
        "records": total,
        "checks_run": len(results),
        "checks_failed": sum(1 for r in results if r["failed"] > 0),
        "error_failures": sum(r["failed"] for r in errors),
        "warn_failures": sum(r["failed"] for r in warnings),
        # The worst single error check is the honest floor: one row can break
        # several checks, so summing them would over-count the damage.
        "pass_pct": round(100.0 * (total - failing_rows) / total, 2) if total else 100.0,
    }


def assess(summary: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    """Apply the gates and return ``pass`` / ``warn`` / ``fail`` with the reasons."""
    gates = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    breaches: List[str] = []

    if summary["records"] < int(gates["min_records"]):
        breaches.append("min_records")
    if summary["records"] and summary["pass_pct"] < float(gates["min_pass_pct"]):
        breaches.append("min_pass_pct")
    if summary.get("duplicate_pct", 0.0) > float(gates["max_duplicate_pct"]):
        breaches.append("max_duplicate_pct")

    for column, pct in (summary.get("null_pct") or {}).items():
        limit = (gates.get("max_null_pct") or {}).get(column)
        if limit is not None and pct > float(limit):
            breaches.append(f"max_null_pct:{column}")

    if breaches:
        verdict = "fail"
    elif summary["records"] and summary["pass_pct"] < float(gates["warn_pass_pct"]):
        verdict = "warn"
    else:
        verdict = "pass"

    return {"verdict": verdict, "breaches": breaches}


# -------------------------------------------------
# SPARK AUDIT
# -------------------------------------------------

def run_checks(dataframe: Any, checks: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Count failures for every check in **one** pass over the data.

    One aggregation with N conditional sums, not N filters: the data is scanned
    once whatever the rule count.
    """
    from pyspark.sql import functions as F

    if not checks:
        return []

    aggregations = [
        F.sum(F.when(F.expr(check["expr"]), 0).otherwise(1)).alias(check["name"])
        for check in checks
    ]
    row = dataframe.agg(*aggregations).collect()[0]

    return [
        {
            "name": check["name"],
            "severity": check.get("severity", "error"),
            "description": check.get("description", ""),
            "expr": check["expr"],
            "failed": int(row[check["name"]] or 0),
        }
        for check in checks
    ]


def failing_rows(dataframe: Any, checks: List[Dict[str, str]], severity: str = "error"):
    """The rows that break at least one check, with ``failed_checks`` attached."""
    from pyspark.sql import functions as F

    selected = [check for check in checks if check.get("severity", "error") == severity]
    if not selected:
        return None

    # `filter(array, x -> x is not null)` rather than array_compact: the
    # higher-order form works on Glue 4.0 (Spark 3.3) too.
    flags = F.filter(
        F.array(*[
            F.when(~F.expr(check["expr"]), F.lit(check["name"])) for check in selected
        ]),
        lambda name: name.isNotNull(),
    )
    return (
        dataframe.withColumn("failed_checks", flags)
        .filter(F.size(F.col("failed_checks")) > 0)
        .withColumn("audited_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )


def profile(dataframe: Any, columns: List[str], total: int) -> Dict[str, float]:
    """Null percentage per column — the cheapest early warning there is."""
    from pyspark.sql import functions as F

    present = [name for name in columns if name in dataframe.columns]
    if not present or not total:
        return {}

    row = dataframe.agg(*[
        F.sum(F.col(name).isNull().cast("long")).alias(name) for name in present
    ]).collect()[0]
    return {name: round(100.0 * int(row[name] or 0) / total, 2) for name in present}


def run(config: Dict[str, Any], spark: Any = None, s3: Any = None) -> Dict[str, Any]:
    from pyspark.sql import SparkSession, functions as F

    started = datetime.now(timezone.utc)
    paths = lakehouse.build_paths(config)
    checks = resolve_checks(config)
    metrics = MetricsEmitter.from_config(config, stage="glue_quality_audit")

    spark = spark or SparkSession.builder.appName(config.get("JOB_NAME", "quality-audit")).getOrCreate()

    silver = spark.read.parquet(paths["silver_events"])
    process_date = config.get("PROCESS_DATE")
    if process_date:
        silver = silver.filter(F.col("partition_date") == process_date)
    silver = silver.cache()

    total = silver.count()
    results = run_checks(silver, checks)
    summary = score(results, total)

    distinct_keys = silver.select("idempotency_key").distinct().count() if total else 0
    summary["duplicate_records"] = max(total - distinct_keys, 0)
    summary["duplicate_pct"] = round(100.0 * summary["duplicate_records"] / total, 2) if total else 0.0
    summary["null_pct"] = profile(
        silver,
        config.get("PROFILE_COLUMNS") or ["customer_id", "product_price", "order_id", "session_id", "campaign"],
        total,
    )

    verdict = assess(summary, config.get("QUALITY") or {})

    # ── quarantine ──
    quarantined = 0
    quarantine_path = None
    if config.get("QUARANTINE_ENABLED", True) and summary["error_failures"]:
        bad = failing_rows(silver, checks, severity="error")
        if bad is not None:
            quarantine_path = (
                f"{paths['quarantine'].rstrip('/')}/audit/"
                f"dt={process_date or started.strftime('%Y-%m-%d')}/"
            )
            bad.coalesce(int(config.get("COALESCE", 4))).write.mode(
                config.get("QUARANTINE_WRITE_MODE", "overwrite")
            ).parquet(quarantine_path)
            quarantined = bad.count()
            logger.warning("Quarantined %d rows to %s", quarantined, quarantine_path)

    report = {
        "job": config.get("JOB_NAME", "quality-audit"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "process_date": process_date,
        "source": paths["silver_events"],
        "verdict": verdict["verdict"],
        "breaches": verdict["breaches"],
        "summary": summary,
        "checks": results,
        "quarantine": {"rows": quarantined, "path": quarantine_path},
    }

    report_key = (
        f"{lakehouse.zone_prefix(config, 'quality')}"
        f"dt={process_date or started.strftime('%Y-%m-%d')}/"
        f"audit-{started.strftime('%Y%m%dT%H%M%S')}.json"
    )
    try:
        (s3 or boto3.client("s3")).put_object(
            Bucket=paths["bucket"], Key=report_key,
            Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Audit report written to s3://%s/%s", paths["bucket"], report_key)
    except Exception as exc:  # noqa: BLE001 - the verdict still stands without the file
        logger.error("Failed to write the audit report: %s", exc)

    metrics.count("AuditedRecords", total)
    metrics.count("QuarantinedRecords", quarantined)
    metrics.count("DuplicateRecords", summary["duplicate_records"])
    metrics.gauge("QualityPassPct", summary["pass_pct"], unit="Percent")
    metrics.gauge("JobDurationSeconds", (datetime.now(timezone.utc) - started).total_seconds(), unit="Seconds")
    for item in results:
        metrics.count("CheckFailures", item["failed"], Check=item["name"])
    metrics.flush()

    silver.unpersist()

    if verdict["verdict"] == "fail" and config.get("FAIL_ON_QUALITY", False):
        raise RuntimeError(f"Quality gate failed: {verdict['breaches']}")

    return {
        "status": "fail" if verdict["verdict"] == "fail" else "success",
        "report_key": report_key,
        "report": report,
        "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }





def main(argv=None) -> Dict[str, Any]:
    args = getResolvedOptions(argv if argv is not None else sys.argv, ["JOB_NAME", "CONFIG_PATH"])
    config = load_config(args["CONFIG_PATH"])
    config.setdefault("JOB_NAME", args.get("JOB_NAME"))
    return run(config)


if __name__ == "__main__":
    print(json.dumps(main(), indent=2, default=str))
