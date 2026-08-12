"""Unit tests for Glue job 1 — the partner drops (source 2) into bronze.

The file listing and the S3 moves are stubbed; the lift itself runs on a real
local SparkSession against the sample export committed in ``data/``, so what is
tested is the file a partner would actually send.
"""

import hashlib
from pathlib import Path

import pytest

import jobs.glue_landing_ingest as landing

SAMPLE_CSV = Path(__file__).resolve().parent.parent / "data/landing/partner_events.csv"

#: Declared rather than inferred: a CSV column is text, and a column that is
#: entirely NULL in a two-row fixture has no type Spark could guess.
_FLAT_SCHEMA = "occurred_at string, event_type string, product_id string"


class _S3:
    def __init__(self, objects=None):
        self.objects = objects or []
        self.copied = []
        self.deleted = []
        self.fail_on = None

    def get_paginator(self, _name):
        outer = self

        class _Paginator:
            def paginate(self, Bucket, Prefix):  # noqa: N803 - boto3's names
                yield {"Contents": [o for o in outer.objects if o["Key"].startswith(Prefix)]}

        return _Paginator()

    def copy_object(self, Bucket, Key, CopySource):  # noqa: N803
        if self.fail_on and self.fail_on in CopySource["Key"]:
            raise RuntimeError("access denied")
        self.copied.append((CopySource["Key"], Key))

    def delete_object(self, Bucket, Key):  # noqa: N803
        self.deleted.append(Key)


# ─────────────────────────────────────────────
# WHERE IT LOOKS
# ─────────────────────────────────────────────

def test_the_drop_zone_and_the_archive_sit_under_landing():
    config = {"OUTPUT_BUCKET": "lake"}

    assert landing.ingest_prefix(config) == "landing/partners/"
    assert landing.archive_prefix(config) == "landing/_processed/"


def test_both_can_be_moved_by_config():
    config = {"OUTPUT_BUCKET": "lake", "LANDING_PREFIX": "drops/",
              "LANDING_INGEST_SUBPATH": "inbox/", "LANDING_ARCHIVE_SUBPATH": "done/"}

    assert landing.ingest_prefix(config) == "drops/inbox/"
    assert landing.archive_prefix(config) == "drops/done/"


# ─────────────────────────────────────────────
# WHAT IT PICKS UP
# ─────────────────────────────────────────────

def test_files_are_classified_by_extension():
    s3 = _S3([
        {"Key": "landing/partners/a.csv", "Size": 10},
        {"Key": "landing/partners/b.ndjson", "Size": 10},
        {"Key": "landing/partners/c.jsonl", "Size": 10},
        {"Key": "landing/partners/d.json", "Size": 10},
    ])

    drops = landing.list_drops("lake", "landing/partners/", s3=s3)

    assert [d["format"] for d in drops] == ["csv", "json", "json", "json"]


def test_folders_empty_objects_and_unknown_formats_are_ignored():
    """The console's "create folder" leaves a zero-byte object behind."""
    s3 = _S3([
        {"Key": "landing/partners/", "Size": 0},
        {"Key": "landing/partners/empty.csv", "Size": 0},
        {"Key": "landing/partners/notes.txt", "Size": 40},
        {"Key": "landing/partners/real.csv", "Size": 120},
    ])

    assert [d["key"] for d in landing.list_drops("lake", "landing/partners/", s3=s3)] == [
        "landing/partners/real.csv"
    ]


# ─────────────────────────────────────────────
# ARCHIVING
# ─────────────────────────────────────────────

def test_an_ingested_file_is_moved_out_of_the_drop_zone():
    """Without the move, the next run re-reads every file ever received."""
    s3 = _S3()

    moved = landing.archive("lake", [{"key": "landing/partners/batch.csv"}], "landing/_processed/", s3=s3)

    assert moved == 1
    assert s3.copied[0][0] == "landing/partners/batch.csv"
    assert s3.copied[0][1].startswith("landing/_processed/dt=")
    assert s3.deleted == ["landing/partners/batch.csv"]


def test_a_failed_move_is_logged_not_raised():
    """The data is already in bronze; a re-read is absorbed by the silver dedupe."""
    s3 = _S3()
    s3.fail_on = "batch.csv"

    assert landing.archive("lake", [{"key": "landing/partners/batch.csv"}], "landing/_processed/", s3=s3) == 0
    assert s3.deleted == []


def test_an_empty_drop_zone_costs_no_cluster(monkeypatch):
    """No files, no SparkSession — the job returns before asking for one."""
    monkeypatch.setattr(landing, "list_drops", lambda *a, **kw: [])

    result = landing.run({"OUTPUT_BUCKET": "lake", "METRICS_ENABLED": False}, spark=None, s3=_S3())

    assert result == {"status": "success", "files": 0, "records": 0, "rejected": 0, "archived": 0}


# ─────────────────────────────────────────────
# THE LIFT (Spark)
# ─────────────────────────────────────────────

@pytest.fixture
def sample(spark):
    """The committed partner export, read exactly as the job reads it."""
    raw = spark.read.option("header", "true").csv(str(SAMPLE_CSV).replace("\\", "/"))
    return landing.add_identity(landing.lift_flat(raw), source_object="test")


def test_a_flat_row_becomes_a_nested_record(sample):
    row = sample.filter("event_type = 'order_placed'").collect()[0]

    assert row["product"]["product_id"].startswith("sku-")
    assert row["customer"]["customer_id"].startswith("cust-")
    assert row["schema_version"] == "3.0"


def test_the_basket_arithmetic_is_done_once_at_the_edge(sample):
    """gross - discount = net, so no consumer downstream has to recompute it."""
    for row in sample.filter("order.gross_amount IS NOT NULL").limit(20).collect():
        order = row["order"]
        expected = round(order["gross_amount"] - order["discount_amount"], 2)
        assert order["net_amount"] == pytest.approx(expected, abs=0.01)
        assert order["amount"] == order["net_amount"]


def test_the_identity_hash_matches_the_streaming_path(sample):
    """The one thing that must not drift: an event delivered as a file and the
    same event delivered through SQS have to deduplicate against each other."""
    row = sample.filter("event_type = 'order_placed'").collect()[0]

    parts = [
        row["event_type"] or "",
        row["occurred_at"] or "",
        row["session"]["session_id"] or "",
        "" if row["session"]["sequence"] is None else str(row["session"]["sequence"]),
        row["product"]["product_id"] or "",
        row["customer"]["customer_id"] or "",
        row["order"]["order_id"] or "",
    ]
    expected = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()  # noqa: S324

    assert row["idempotency_key"] == expected


def test_every_record_carries_a_key(sample):
    assert sample.filter("idempotency_key IS NULL").count() == 0


def test_the_sample_export_lands_whole(sample):
    """The committed sample is the fixture for the whole pipeline: if a row of
    it stops being ingestible, every downstream test is running on less data
    than it thinks."""
    accepted, rejected = landing.split_on_checks(sample, landing.BRONZE_CHECKS)

    assert accepted.count() == 89
    assert rejected.count() == 0


def test_partitions_come_from_event_time(sample):
    """Not arrival time — a file delivered late still lands in its own hour."""
    row = landing.with_partitions(sample).collect()[0]

    assert row["dt"] == row["occurred_at"][:10]
    assert row["hour"] == row["occurred_at"][11:13]


# ─────────────────────────────────────────────
# THE CHECKS
# ─────────────────────────────────────────────

def test_a_row_with_no_product_is_turned_away(spark):
    """Bronze admits almost everything, but not a row nothing can be joined to."""
    raw = spark.createDataFrame(
        [("2026-06-24T12:00:00+00:00", "product_viewed", None)],
        _FLAT_SCHEMA,
    )
    lifted = landing.add_identity(landing.lift_flat(raw))

    accepted, rejected = landing.split_on_checks(lifted, landing.BRONZE_CHECKS)

    assert accepted.count() == 0
    assert rejected.collect()[0]["failed_checks"] == ["product_id_present"]


def test_an_unparseable_timestamp_is_turned_away(spark):
    """It would otherwise land in the `unknown` partition and never be found."""
    raw = spark.createDataFrame([("not a date", "product_viewed", "sku-1")], _FLAT_SCHEMA)
    lifted = landing.add_identity(landing.lift_flat(raw))

    _, rejected = landing.split_on_checks(lifted, landing.BRONZE_CHECKS)

    assert "occurred_at_parsed" in rejected.collect()[0]["failed_checks"]


def test_a_rejected_row_names_every_rule_it_broke(spark):
    raw = spark.createDataFrame([("not a date", None, None)], _FLAT_SCHEMA)
    lifted = landing.add_identity(landing.lift_flat(raw))

    _, rejected = landing.split_on_checks(lifted, landing.BRONZE_CHECKS)
    failed = rejected.collect()[0]["failed_checks"]

    assert {"occurred_at_parsed", "product_id_present"} <= set(failed)


def test_every_check_explains_itself():
    for check in landing.BRONZE_CHECKS:
        assert check["description"], check["name"]
