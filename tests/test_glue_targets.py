"""Unit tests for the Glue jobs' config-driven wiring.

Covers path resolution, the quality gate and the multi-table RDS target
resolution — everything the jobs decide *before* Spark gets involved, so these
run without a cluster.
"""

import pytest

import jobs.glue_ecommerce_processing as processing
import jobs.glue_rds_load as rds


# ─────────────────────────────────────────────
# PROCESSING — QUALITY GATE
# ─────────────────────────────────────────────

def _summary(**overrides):
    return {
        "raw_records": 100,
        "processed_records": 100,
        "dropped_records": 0,
        "duplicate_records": 0,
        "retention_pct": 100.0,
        "null_pct": {},
        **overrides,
    }


def test_assess_passes_a_clean_batch():
    assert processing._assess(_summary(), {}) == {"verdict": "pass", "breaches": []}


def test_assess_warns_below_the_warn_threshold():
    verdict = processing._assess(_summary(processed_records=97, dropped_records=3, retention_pct=97.0), {})

    assert verdict["verdict"] == "warn"
    assert verdict["breaches"] == []


def test_assess_fails_below_the_minimum_retention():
    verdict = processing._assess(_summary(processed_records=80, dropped_records=20, retention_pct=80.0), {})

    assert verdict["verdict"] == "fail"
    assert "min_pass_pct" in verdict["breaches"]


def test_assess_fails_on_too_many_duplicates():
    verdict = processing._assess(_summary(duplicate_records=10), {})

    assert verdict["verdict"] == "fail"
    assert "max_duplicate_pct" in verdict["breaches"]


def test_assess_fails_on_a_null_rate_breach():
    verdict = processing._assess(
        _summary(null_pct={"customer_id": 40.0}), {"max_null_pct": {"customer_id": 10.0}}
    )

    assert "max_null_pct:customer_id" in verdict["breaches"]


def test_assess_ignores_columns_without_a_configured_limit():
    assert processing._assess(_summary(null_pct={"category": 90.0}), {})["verdict"] == "pass"


def test_assess_honours_threshold_overrides():
    verdict = processing._assess(
        _summary(processed_records=80, dropped_records=20, retention_pct=80.0),
        {"min_pass_pct": 50.0, "warn_pass_pct": 60.0},
    )
    assert verdict["verdict"] == "pass"


def test_assess_of_an_empty_batch_does_not_fail_on_retention():
    verdict = processing._assess(_summary(raw_records=0, processed_records=0, retention_pct=0.0), {})
    assert verdict["verdict"] == "pass"


def test_assess_fails_below_the_minimum_record_count():
    verdict = processing._assess(_summary(raw_records=5), {"min_records": 10})
    assert "min_records" in verdict["breaches"]


# ─────────────────────────────────────────────
# PROCESSING — ENRICHMENT CARRIES v3 CONTEXT
# ─────────────────────────────────────────────

def test_enrich_record_carries_the_v3_blocks():
    enriched = processing._enrich_record({
        "occurred_at": "2026-06-24T15:30:00+00:00",
        "event_type": "order_placed",
        "channel": "mobile_app",
        "idempotency_key": "abc",
        "session": {"session_id": "sess-1"},
        "device": {"type": "mobile"},
        "product": {"product_id": "sku-1", "name": "Mouse", "price": 19.99, "category": "electronics"},
        "customer": {"customer_id": "cust-1", "segment": "vip", "country": "FR"},
        "order": {"order_id": "ord-1", "quantity": 2, "net_amount": 39.98, "currency": "EUR"},
        "marketing": {"campaign": "spring_sale", "source": "google"},
    })

    assert enriched["session_id"] == "sess-1"
    assert enriched["order_id"] == "ord-1"
    assert enriched["quantity"] == 2
    assert enriched["signed_net_amount"] == 39.98
    assert enriched["campaign"] == "spring_sale"
    assert enriched["customer_segment"] == "vip"
    assert enriched["channel"] == "mobile_app"


def test_enrich_record_signs_a_refund_negative():
    enriched = processing._enrich_record({
        "occurred_at": "2026-06-24T15:30:00+00:00",
        "event_type": "refund_issued",
        "product": {"product_id": "sku-1", "price": 10.0},
        "customer": {"customer_id": "cust-1"},
        "order": {"net_amount": 10.0},
    })
    assert enriched["signed_net_amount"] == -10.0


def test_enrich_record_leaves_browsing_events_at_zero_revenue():
    enriched = processing._enrich_record({
        "occurred_at": "2026-06-24T15:30:00+00:00",
        "event_type": "product_viewed",
        "product": {"product_id": "sku-1", "price": 10.0},
        "customer": {"customer_id": "cust-1"},
        "order": {"net_amount": 10.0},
    })
    assert enriched["signed_net_amount"] == 0.0


def test_run_job_deduplicates_on_the_idempotency_key(tmp_path):
    """Two records that differ only in arrival metadata are one business event."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    base = ('{"occurred_at":"2026-06-24T15:30:00Z","event_type":"order_placed","idempotency_key":"same",'
            '"product":{"product_id":"sku-1"},"customer":{"customer_id":"cust-1"},"order":{"order_id":"o-%d"}}')
    (input_dir / "part-0000.json").write_text(
        (base % 1) + "\n" + (base % 2) + "\n", encoding="utf-8"
    )

    result = processing.run_job(
        input_prefix=str(input_dir) + "/", output_prefix=str(tmp_path / "out") + "/", local_fs=True
    )

    assert result["metrics"]["duplicate_records"] == 1
    assert result["metrics"]["output_records"] == 1


# ─────────────────────────────────────────────
# RDS LOAD — TARGET RESOLUTION
# ─────────────────────────────────────────────

def test_resolve_targets_defaults_to_the_legacy_single_table():
    targets = rds.resolve_targets({"OUTPUT_BUCKET": "lake", "RDS_TABLE": "fact_events"})

    assert len(targets) == 1
    assert targets[0]["dataset"] == "processed"
    assert targets[0]["path"] == "s3://lake/silver/events/"
    assert targets[0]["table"] == "fact_events"
    assert targets[0]["required_columns"] == rds.REQUIRED_COLUMNS
    assert targets[0]["optional"] is False


def test_resolve_targets_load_all_covers_the_warehouse():
    targets = rds.resolve_targets({"OUTPUT_BUCKET": "lake", "RDS_LOAD_ALL": True})

    tables = [t["table"] for t in targets]
    assert tables == [spec["table"] for spec in rds.DEFAULT_TARGETS]
    assert {t["dataset"] for t in targets} >= {"silver/events", "gold/orders", "gold/customer_rfm"}


def test_resolve_targets_maps_curated_datasets_to_paths():
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_TABLES": [{"dataset": "curated/sessions", "table": "fact_sessions"}],
    })

    assert targets[0]["path"] == "s3://lake/gold/sessions/"
    assert targets[0]["optional"] is True


def test_resolve_targets_honours_a_curated_prefix_override():
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "CURATED_PREFIX": "gold/",
        "RDS_TABLES": [{"dataset": "curated/orders", "table": "fact_orders"}],
    })

    assert targets[0]["path"] == "s3://lake/gold/orders/"


def test_resolve_targets_accepts_an_explicit_path():
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_TABLES": [{"dataset": "anything", "table": "t", "path": "s3://other/data/"}],
    })

    assert targets[0]["path"] == "s3://other/data/"


def test_resolve_targets_per_target_mode_overrides_the_default():
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_WRITE_MODE": "append",
        "RDS_TABLES": [
            {"dataset": "processed", "table": "fact_events"},
            {"dataset": "curated/customer_rfm", "table": "dim_rfm", "mode": "overwrite"},
        ],
    })

    assert targets[0]["mode"] == "append"
    assert targets[1]["mode"] == "overwrite"


def test_resolve_targets_rejects_a_target_without_a_table():
    with pytest.raises(ValueError, match="no table name"):
        rds.resolve_targets({"OUTPUT_BUCKET": "lake", "RDS_TABLES": [{"dataset": "processed"}]})


def test_rds_table_is_optional_once_rds_tables_is_set():
    settings = rds._resolve_rds_settings({
        "RDS_HOST": "db", "RDS_PORT": 5432, "RDS_DATABASE": "d",
        "RDS_USERNAME": "u", "RDS_PASSWORD": "p",
        "RDS_TABLES": [{"dataset": "processed", "table": "fact_events"}],
    })

    assert settings["table"] is None
    assert settings["batchsize"] == 10000


# ─────────────────────────────────────────────
# RDS LOAD — WRITING
# ─────────────────────────────────────────────

class _Writer:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def option(self, key, value):
        self.calls.append((key, value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def save(self):
        self.calls.append(("save", None))


class _DataFrame:
    def __init__(self, rows=2, columns=None):
        self.write = _Writer()
        self.columns = columns if columns is not None else list(rds.REQUIRED_COLUMNS)
        self._rows = rows
        self.selected = None

    def count(self):
        return self._rows

    def select(self, *args):
        self.selected = args
        return self


class _Spark:
    def __init__(self, frames):
        self._frames = frames

    @property
    def read(self):
        outer = self

        class Read:
            def parquet(self, path):
                frame = outer._frames.get(path)
                if frame is None:
                    raise RuntimeError(f"Path does not exist: {path}")
                return frame

        return Read()


def _settings(**overrides):
    return {
        "username": "u", "password": "p", "driver": "org.postgresql.Driver",
        "write_mode": "append", "table": "fact_events",
        "batchsize": 5000, "num_partitions": 4, "truncate": True,
        **overrides,
    }


def test_write_to_rds_sets_batching_options(monkeypatch):
    monkeypatch.setattr(rds, "_build_jdbc_url", lambda settings: "jdbc:postgresql://h:5432/d")
    frame = _DataFrame()

    rds._write_to_rds(frame, _settings(), table="fact_events")

    assert ("batchsize", 5000) in frame.write.calls
    assert ("numPartitions", 4) in frame.write.calls
    assert ("truncate", "true") not in frame.write.calls   # append mode


def test_write_to_rds_truncates_instead_of_dropping_on_overwrite(monkeypatch):
    """Truncate keeps the table's grants, indexes and column types intact."""
    monkeypatch.setattr(rds, "_build_jdbc_url", lambda settings: "jdbc:postgresql://h:5432/d")
    frame = _DataFrame()

    rds._write_to_rds(frame, _settings(), table="fact_events", mode="overwrite")

    assert ("truncate", "true") in frame.write.calls
    assert ("mode", "overwrite") in frame.write.calls


def test_read_dataset_projects_required_columns():
    frame = _DataFrame()
    spark = _Spark({"s3://lake/processed/": frame})

    result = rds._read_dataset(spark, "s3://lake/processed/", rds.REQUIRED_COLUMNS)

    assert result.selected == tuple(rds.REQUIRED_COLUMNS)


def test_read_dataset_without_required_columns_returns_everything():
    frame = _DataFrame()
    spark = _Spark({"s3://lake/curated/orders/": frame})

    assert rds._read_dataset(spark, "s3://lake/curated/orders/") is frame
    assert frame.selected is None


def test_read_dataset_rejects_a_dataset_missing_columns():
    spark = _Spark({"s3://lake/processed/": _DataFrame(columns=["event_type"])})

    with pytest.raises(ValueError, match="missing columns"):
        rds._read_dataset(spark, "s3://lake/processed/", rds.REQUIRED_COLUMNS)


def test_load_targets_reports_per_table_outcomes(monkeypatch):
    monkeypatch.setattr(rds, "_build_jdbc_url", lambda settings: "jdbc:postgresql://h:5432/d")
    spark = _Spark({
        "s3://lake/silver/events/": _DataFrame(rows=10),
        "s3://lake/gold/orders/": _DataFrame(rows=3, columns=["order_id"]),
    })
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_TABLES": [
            {"dataset": "processed", "table": "fact_events"},
            {"dataset": "curated/orders", "table": "fact_orders"},
        ],
    })

    results = rds.load_targets(spark, targets, _settings())

    assert [r["status"] for r in results] == ["loaded", "loaded"]
    assert [r["rows_loaded"] for r in results] == [10, 3]


def test_load_targets_skips_an_empty_dataset(monkeypatch):
    monkeypatch.setattr(rds, "_build_jdbc_url", lambda settings: "jdbc:postgresql://h:5432/d")
    spark = _Spark({"s3://lake/gold/anomalies/": _DataFrame(rows=0, columns=["reasons"])})
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_TABLES": [{"dataset": "curated/anomalies", "table": "fact_anomalies"}],
    })

    results = rds.load_targets(spark, targets, _settings())

    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "empty"


def test_load_targets_skips_a_missing_optional_dataset(monkeypatch):
    """A curated table this run did not produce must not fail the whole load."""
    monkeypatch.setattr(rds, "_build_jdbc_url", lambda settings: "jdbc:postgresql://h:5432/d")
    spark = _Spark({})
    targets = rds.resolve_targets({
        "OUTPUT_BUCKET": "lake",
        "RDS_TABLES": [{"dataset": "curated/sessions", "table": "fact_sessions"}],
    })

    results = rds.load_targets(spark, targets, _settings())

    assert results[0]["status"] == "skipped"
    assert "does not exist" in results[0]["reason"]


def test_load_targets_propagates_a_failure_on_the_mandatory_target():
    spark = _Spark({})
    targets = rds.resolve_targets({"OUTPUT_BUCKET": "lake", "RDS_TABLE": "fact_events"})

    with pytest.raises(RuntimeError, match="does not exist"):
        rds.load_targets(spark, targets, _settings())
