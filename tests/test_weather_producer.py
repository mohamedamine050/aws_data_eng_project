"""Unit tests for the weather_producer Lambda handler."""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

# Let boto3.client(...) be created at import time without real credentials,
# and make `common` importable when the handler is loaded.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))


@pytest.fixture
def producer():
    """Load the weather_producer handler under a unique module name.

    (Both Lambdas have a `handler.py`, so we load by file path to avoid a clash.)
    """
    spec = importlib.util.spec_from_file_location(
        "producer_handler", _SRC / "lambdas/weather_producer/handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── ARGS & CONFIG ────────────────────────────────────────────

def test_get_args_from_event(producer):
    assert producer.get_args({"CONFIG_PATH": "s3://b/k.json"}) == {"CONFIG_PATH": "s3://b/k.json"}


def test_get_args_env_fallback(producer, monkeypatch):
    monkeypatch.setenv("CONFIG_PATH", "config/local.json")
    assert producer.get_args({})["CONFIG_PATH"] == "config/local.json"


def test_get_args_missing_raises(producer, monkeypatch):
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    with pytest.raises(RuntimeError):
        producer.get_args({})


def test_load_config_local_file(producer, tmp_path):
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"KINESIS_STREAM_NAME": "s"}), encoding="utf-8")
    assert producer.load_config(str(cfg))["KINESIS_STREAM_NAME"] == "s"


# ── LOCATIONS ────────────────────────────────────────────────

def test_slugify(producer):
    assert producer._slugify("New York", "US") == "new-york-us"
    assert producer._slugify("Tunis", None) == "tunis"


def test_resolve_locations_defaults(producer):
    locs = producer.resolve_locations(None)
    assert len(locs) == len(producer.DEFAULT_LOCATIONS)
    assert all("location_id" in loc for loc in locs)


def test_resolve_locations_custom(producer):
    locs = producer.resolve_locations(
        [{"city": "Tunis", "country": "TN", "latitude": 36.8, "longitude": 10.1}])
    assert locs == [{
        "city": "Tunis", "country": "TN",
        "latitude": 36.8, "longitude": 10.1, "location_id": "tunis-tn",
    }]


def test_resolve_locations_skips_malformed(producer):
    locs = producer.resolve_locations([
        {"city": "Good", "latitude": 1.0, "longitude": 2.0},
        {"city": "Bad", "latitude": "oops", "longitude": 2.0},  # bad coord
        {"latitude": 1.0, "longitude": 2.0},                    # missing city
    ])
    assert [loc["city"] for loc in locs] == ["Good"]


# ── EXTRACT ──────────────────────────────────────────────────

def test_open_meteo_url_free(producer):
    url = producer._open_meteo_url({"latitude": 1, "longitude": 2})
    assert url.startswith(producer.OPEN_METEO_FREE_URL)
    assert "apikey" not in url


def test_open_meteo_url_with_key(producer):
    url = producer._open_meteo_url({"latitude": 1}, api_key="SECRET")
    assert url.startswith(producer.OPEN_METEO_CUSTOMER_URL)
    assert "apikey=SECRET" in url


class _FakeResp:
    def __init__(self, payload):
        self._data = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_fetch_weather_ok(producer, monkeypatch):
    payload = {"current": {
        "time": "2026-06-24T12:00", "temperature_2m": 20.0, "weather_code": 0,
        "relative_humidity_2m": 50,
    }}
    monkeypatch.setattr(producer.urllib.request, "urlopen",
                        lambda *a, **k: _FakeResp(payload))
    loc = {"city": "Tunis", "country": "TN", "latitude": 36.8,
           "longitude": 10.1, "location_id": "tunis-tn"}
    rec = producer.fetch_weather(loc, "metric", 10)
    assert rec["measurement"]["temperature"] == 20.0
    assert rec["condition"]["main"] == "Clear"


def test_fetch_weather_no_current_returns_none(producer, monkeypatch):
    monkeypatch.setattr(producer.urllib.request, "urlopen",
                        lambda *a, **k: _FakeResp({"reason": "bad"}))
    loc = {"city": "X", "latitude": 0, "longitude": 0, "location_id": "x"}
    assert producer.fetch_weather(loc, "metric", 10) is None


# ── LOAD (Kinesis) ───────────────────────────────────────────

def _record(loc_id="tunis-tn"):
    return {"location": {"location_id": loc_id}, "measurement": {"temperature": 1}}


def test_put_records_success(producer, monkeypatch):
    captured = {}

    def fake_put(StreamName, Records):
        captured["stream"] = StreamName
        captured["n"] = len(Records)
        return {"FailedRecordCount": 0, "Records": [{} for _ in Records]}

    monkeypatch.setattr(producer._KINESIS, "put_records", fake_put)
    failed = producer.put_records("my-stream", [_record(), _record("london-gb")])
    assert failed == 0
    assert captured == {"stream": "my-stream", "n": 2}


def test_put_records_empty(producer):
    assert producer.put_records("s", []) == 0


def test_put_records_total_failure(producer, monkeypatch):
    def boom(StreamName, Records):
        raise producer.BotoCoreError()

    monkeypatch.setattr(producer._KINESIS, "put_records", boom)
    assert producer.put_records("s", [_record()]) == 1


# ── HANDLER ──────────────────────────────────────────────────

def test_lambda_handler_end_to_end(producer, monkeypatch, tmp_path):
    cfg = tmp_path / "prod.json"
    cfg.write_text(json.dumps({
        "KINESIS_STREAM_NAME": "demo",
        "LOCATIONS": [{"city": "Tunis", "country": "TN", "latitude": 36.8, "longitude": 10.1}],
    }), encoding="utf-8")

    payload = {"current": {"time": "2026-06-24T12:00", "temperature_2m": 30.0, "weather_code": 0}}
    monkeypatch.setattr(producer.urllib.request, "urlopen", lambda *a, **k: _FakeResp(payload))

    sent = []
    monkeypatch.setattr(producer._KINESIS, "put_records",
                        lambda StreamName, Records: (sent.extend(Records),
                                                     {"FailedRecordCount": 0, "Records": [{} for _ in Records]})[1])

    result = producer.lambda_handler({"CONFIG_PATH": str(cfg)}, None)
    assert result == {"locations_requested": 1, "fetched": 1, "sent": 1, "failed": 0}
    assert len(sent) == 1
