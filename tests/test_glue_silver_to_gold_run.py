"""What the gold job *orchestrates*, as opposed to what it computes.

The builders themselves are covered against a real SparkSession in
``test_transforms.py``. What is covered here is the part no cluster is needed
for and no cluster makes obvious: which tables a given config actually builds,
where each one is written, which window of silver is read, and what the job
reports when it is done.

Every builder is stubbed, so a mistake in the wiring shows up as a wrong path or
a missing table rather than as a forty-minute job that produced the wrong thing.
"""

import pytest

pytest.importorskip("pyspark", reason="PySpark not installed")

import jobs.glue_silver_to_gold as gold  # noqa: E402


class _Writer:
    def __init__(self, frame):
        self.frame = frame

    def mode(self, value):
        self.frame.write_mode = value
        return self

    def partitionBy(self, *columns):  # noqa: N802 - Spark's spelling
        self.frame.partitions = list(columns)
        return self

    def parquet(self, path):
        self.frame.written_to = path


class _Frame:
    """Enough of a DataFrame for the orchestration to run end to end."""

    def __init__(self, rows=7, name="silver"):
        self.rows = rows
        self.name = name
        self.written_to = None
        self.write_mode = None
        self.partitions = None
        self.coalesced = None
        self.filters = []
        self.unpersisted = False

    @property
    def write(self):
        return _Writer(self)

    def cache(self):
        return self

    def unpersist(self):
        self.unpersisted = True

    def count(self):
        return self.rows

    def coalesce(self, n):
        self.coalesced = n
        return self

    def filter(self, condition):
        self.filters.append(str(condition))
        return self


class _Spark:
    def __init__(self, frame):
        self.frame = frame
        self.read_from = None
        self.read = self

    def parquet(self, path):
        self.read_from = path
        return self.frame


@pytest.fixture
def built(monkeypatch):
    """Stub every builder; remember the frame each one produced."""
    frames = {}

    def builder(name):
        def make(*args, **kwargs):
            frames[name] = _Frame(rows=len(name), name=name)
            return frames[name]
        return make

    for name in ("sessions", "funnel_daily", "orders", "customer_rfm",
                 "product_daily", "anomalies"):
        monkeypatch.setattr(gold, f"build_{name}", builder(name))
    return frames


def _run(config=None, silver=None, **overrides):
    spark = _Spark(silver or _Frame())
    settings = {"OUTPUT_BUCKET": "lake", "METRICS_ENABLED": False, **(config or {}), **overrides}
    return gold.run(settings, spark=spark), spark


# ─────────────────────────────────────────────
# WHAT GETS BUILT
# ─────────────────────────────────────────────

def test_a_default_run_writes_all_six_tables(built):
    result, _ = _run()

    assert set(result["outputs"]) == {
        "sessions", "funnel_daily", "orders", "customer_rfm", "product_daily", "anomalies",
    }
    assert result["status"] == "success"


def test_a_subset_builds_only_what_was_asked(built):
    """Rebuilding one table after a fix must not recompute the other five."""
    result, _ = _run({"GOLD_DATASETS": ["orders", "anomalies"]})

    assert set(result["outputs"]) == {"orders", "anomalies"}
    assert "sessions" not in built


def test_an_empty_list_writes_nothing(built):
    result, _ = _run({"GOLD_DATASETS": []})

    assert result["outputs"] == {}
    assert result["row_counts"] == {}


def test_rfm_is_built_from_the_orders_table_not_from_silver_again(built):
    """Orders is an expensive aggregation; RFM reuses it rather than redoing it."""
    _run({"GOLD_DATASETS": ["customer_rfm"]})

    assert "orders" in built, "orders should be built as RFM's input even when not published"


# ─────────────────────────────────────────────
# WHERE IT WRITES
# ─────────────────────────────────────────────

def test_each_table_lands_in_its_own_gold_prefix(built):
    result, _ = _run()

    assert result["outputs"]["orders"] == "s3://lake/gold/orders/"
    assert result["outputs"]["customer_rfm"] == "s3://lake/gold/customer_rfm/"


def test_a_relocated_gold_zone_is_honoured(built):
    result, _ = _run({"GOLD_PREFIX": "marts/"})

    assert result["outputs"]["sessions"] == "s3://lake/marts/sessions/"


def test_daily_tables_are_partitioned_and_the_customer_table_is_not(built):
    _run({"GOLD_DATASETS": ["orders", "customer_rfm"]})

    assert built["orders"].partitions == ["partition_date"]
    assert built["customer_rfm"].partitions is None


def test_the_write_mode_and_file_count_come_from_the_config(built):
    _run({"GOLD_DATASETS": ["sessions"], "WRITE_MODE": "append", "COALESCE": 2})

    assert built["sessions"].write_mode == "append"
    assert built["sessions"].coalesced == 2


# ─────────────────────────────────────────────
# WHICH WINDOW OF SILVER IT READS
# ─────────────────────────────────────────────

def test_the_whole_table_is_read_when_no_date_is_given(built):
    silver = _Frame()
    _run(silver=silver)

    assert silver.filters == []


def test_a_process_date_alone_restricts_to_that_day(built, spark):
    # `spark` is requested only to give F.col/F.lit an active context; the job
    # still runs against the stub session below.
    silver = _Frame()
    _run({"PROCESS_DATE": "2026-06-24"}, silver=silver)

    assert len(silver.filters) == 1
    assert "partition_date" in silver.filters[0]


def test_a_lookback_widens_the_window_past_the_processed_day(built, spark):
    """Sessions and RFM are window functions: recomputing one day alone would
    truncate every window that started earlier."""
    silver = _Frame()
    _run({"PROCESS_DATE": "2026-06-24", "GOLD_LOOKBACK_DAYS": 7}, silver=silver)

    assert len(silver.filters) == 1
    assert "date_sub" in silver.filters[0]


def test_silver_is_read_from_the_configured_path(built):
    _, spark = _run()

    assert spark.read_from == "s3://lake/silver/events/"


# ─────────────────────────────────────────────
# WHAT IT REPORTS
# ─────────────────────────────────────────────

def test_the_result_carries_a_row_count_per_table(built):
    result, _ = _run({"GOLD_DATASETS": ["sessions", "orders"]})

    assert result["row_counts"] == {"sessions": len("sessions"), "orders": len("orders")}


def test_the_cached_frames_are_released(built):
    """A cached DataFrame left behind holds executor memory for the rest of the run."""
    silver = _Frame()
    _run({"GOLD_DATASETS": ["customer_rfm", "product_daily"]}, silver=silver)

    assert silver.unpersisted
    assert built["customer_rfm"].unpersisted
    assert built["product_daily"].unpersisted


def test_the_run_is_timed(built):
    result, _ = _run()

    assert result["duration_seconds"] >= 0
    assert result["process_date"] is None
