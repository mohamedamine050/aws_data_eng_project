"""What the audit job *does with* its verdict.

The two tests that pass ``PROCESS_DATE`` request the ``spark`` fixture: filtering
builds an ``F.col`` expression, which needs an active context even though no
data ever reaches a cluster here.

The checks and the scoring are covered against a real SparkSession in
``test_glue_quality_audit.py``. What is covered here is everything that happens
around them: what gets quarantined, where the report is written, and — the point
of the whole job — whether a bad partition actually stops the pipeline or is
merely noted.
"""

import json

import pytest

pytest.importorskip("pyspark", reason="PySpark not installed")

import jobs.glue_quality_audit as audit  # noqa: E402


class _Writer:
    def __init__(self, frame):
        self.frame = frame

    def mode(self, value):
        self.frame.write_mode = value
        return self

    def parquet(self, path):
        self.frame.written_to = path


class _Frame:
    def __init__(self, rows=100, distinct=100):
        self.rows = rows
        self.distinct = distinct
        self.written_to = None
        self.write_mode = None
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

    def coalesce(self, _n):
        return self

    def filter(self, _condition):
        return self

    def select(self, *_columns):
        return _Distinct(self.distinct)


class _Distinct:
    def __init__(self, rows):
        self.rows = rows

    def distinct(self):
        return self

    def count(self):
        return self.rows


class _Spark:
    def __init__(self, frame):
        self.frame = frame
        self.read = self

    def parquet(self, _path):
        return self.frame


class _S3:
    def __init__(self, explode=False):
        self.puts = []
        self.explode = explode

    def put_object(self, **kwargs):
        if self.explode:
            raise RuntimeError("access denied")
        self.puts.append(kwargs)
        return {}


@pytest.fixture
def clean(monkeypatch):
    """A batch where every check passes."""
    monkeypatch.setattr(audit, "run_checks", lambda df, checks: [
        {"name": c["name"], "severity": c.get("severity", "error"),
         "description": c.get("description", ""), "expr": c["expr"], "failed": 0}
        for c in checks
    ])
    monkeypatch.setattr(audit, "profile", lambda df, columns, total: {})
    monkeypatch.setattr(audit, "failing_rows", lambda df, checks, severity="error": _Frame(rows=0))


@pytest.fixture
def dirty(monkeypatch):
    """A batch where one error-severity check fails on a tenth of the rows."""
    def results(df, checks):
        return [
            {"name": c["name"], "severity": c.get("severity", "error"),
             "description": c.get("description", ""), "expr": c["expr"],
             "failed": 10 if c["name"] == "product_id_present" else 0}
            for c in checks
        ]

    bad = _Frame(rows=10)
    monkeypatch.setattr(audit, "run_checks", results)
    monkeypatch.setattr(audit, "profile", lambda df, columns, total: {"customer_id": 4.0})
    monkeypatch.setattr(audit, "failing_rows", lambda df, checks, severity="error": bad)
    return bad


def _run(config=None, silver=None, s3=None):
    s3 = s3 or _S3()
    settings = {"OUTPUT_BUCKET": "lake", "METRICS_ENABLED": False, **(config or {})}
    result = audit.run(settings, spark=_Spark(silver or _Frame()), s3=s3)
    return result, s3


# ─────────────────────────────────────────────
# THE VERDICT
# ─────────────────────────────────────────────

def test_a_clean_batch_passes_and_quarantines_nothing(clean):
    result, _ = _run()

    assert result["status"] == "success"
    assert result["report"]["verdict"] == "pass"
    assert result["report"]["quarantine"] == {"rows": 0, "path": None}


def test_a_breach_is_reported_but_does_not_stop_the_job_by_default(dirty):
    """Reporting is the default so a threshold can be tuned before it is enforced."""
    result, _ = _run()

    assert result["report"]["verdict"] == "fail"
    assert result["status"] == "fail"


def test_fail_on_quality_turns_the_report_into_a_gate(dirty):
    """This is what stops gold from being rebuilt on a bad partition."""
    with pytest.raises(RuntimeError, match="Quality gate failed"):
        _run({"FAIL_ON_QUALITY": True})


def test_a_clean_batch_never_raises_even_with_the_gate_on(clean):
    result, _ = _run({"FAIL_ON_QUALITY": True})

    assert result["status"] == "success"


# ─────────────────────────────────────────────
# DUPLICATES
# ─────────────────────────────────────────────

def test_duplicates_are_counted_against_the_idempotency_key(clean):
    result, _ = _run(silver=_Frame(rows=100, distinct=90))

    assert result["report"]["summary"]["duplicate_records"] == 10
    assert result["report"]["summary"]["duplicate_pct"] == 10.0


def test_an_empty_partition_does_not_divide_by_zero(clean):
    result, _ = _run(silver=_Frame(rows=0, distinct=0))

    assert result["report"]["summary"]["duplicate_pct"] == 0.0


# ─────────────────────────────────────────────
# QUARANTINE
# ─────────────────────────────────────────────

def test_failing_rows_are_quarantined_under_the_audited_day(dirty, spark):
    result, _ = _run({"PROCESS_DATE": "2026-06-24"})

    assert result["report"]["quarantine"]["rows"] == 10
    assert result["report"]["quarantine"]["path"] == "s3://lake/quarantine/audit/dt=2026-06-24/"
    assert dirty.written_to == "s3://lake/quarantine/audit/dt=2026-06-24/"


def test_the_quarantine_can_be_switched_off(dirty):
    result, _ = _run({"QUARANTINE_ENABLED": False})

    assert result["report"]["quarantine"]["rows"] == 0
    assert dirty.written_to is None


# ─────────────────────────────────────────────
# THE REPORT
# ─────────────────────────────────────────────

def test_the_report_lands_in_the_quality_zone_under_the_audited_day(clean, spark):
    result, s3 = _run({"PROCESS_DATE": "2026-06-24"})

    assert s3.puts[0]["Bucket"] == "lake"
    assert s3.puts[0]["Key"].startswith("quality/dt=2026-06-24/audit-")
    assert result["report_key"] == s3.puts[0]["Key"]


def test_the_report_names_every_check_it_ran(clean):
    _, s3 = _run()
    written = json.loads(s3.puts[0]["Body"].decode("utf-8"))

    assert {c["name"] for c in written["checks"]} == {c["name"] for c in audit.DEFAULT_CHECKS}
    assert written["source"] == "s3://lake/silver/events/"


def test_a_config_check_is_audited_alongside_the_baseline(clean):
    _, s3 = _run({"QUALITY_CHECKS": [
        {"name": "eur_only", "expr": "currency = 'EUR'", "severity": "warn"}
    ]})
    written = json.loads(s3.puts[0]["Body"].decode("utf-8"))

    assert "eur_only" in {c["name"] for c in written["checks"]}


def test_losing_the_report_does_not_lose_the_verdict(dirty):
    """The report is a convenience; the verdict is what the pipeline acts on."""
    result, _ = _run(s3=_S3(explode=True))

    assert result["report"]["verdict"] == "fail"
    assert result["status"] == "fail"


def test_the_cached_frame_is_released(clean):
    silver = _Frame()
    _run(silver=silver)

    assert silver.unpersisted
