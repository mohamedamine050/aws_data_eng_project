"""Unit tests for common.weather_schema."""

import sys
from pathlib import Path

# Make `common` importable (src/ on the path).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from common.weather_schema import (  # noqa: E402
    SCHEMA_VERSION,
    describe_weather_code,
    normalize_record,
)


def test_describe_known_code():
    assert describe_weather_code(0) == ("Clear", "clear sky")
    assert describe_weather_code(3) == ("Clouds", "overcast")
    assert describe_weather_code(95)[0] == "Thunderstorm"


def test_describe_unknown_code():
    main, desc = describe_weather_code(123)
    assert main == "Unknown"
    assert "123" in desc


def test_describe_none_code():
    assert describe_weather_code(None) == (None, None)


def _sample_current():
    return {
        "time": "2026-06-24T12:00",
        "temperature_2m": 18.2,
        "apparent_temperature": 17.9,
        "relative_humidity_2m": 64,
        "surface_pressure": 1012.0,
        "wind_speed_10m": 3.6,
        "wind_direction_10m": 210,
        "cloud_cover": 40,
        "weather_code": 3,
    }


def _sample_location():
    return {
        "city": "London", "country": "GB",
        "latitude": 51.5072, "longitude": -0.1276,
        "location_id": "london-gb",
    }


def test_normalize_record_shape():
    rec = normalize_record(_sample_location(), _sample_current(), "metric")

    assert rec["schema_version"] == SCHEMA_VERSION
    assert rec["units"] == "metric"
    assert rec["event_id"] == "london-gb-2026-06-24T12:00"
    # observed_at is normalized to a UTC ISO timestamp.
    assert rec["observed_at"].startswith("2026-06-24T12:00:00")

    assert rec["location"]["city"] == "London"
    assert rec["location"]["location_id"] == "london-gb"

    m = rec["measurement"]
    assert m["temperature"] == 18.2
    assert m["humidity"] == 64
    assert m["wind_speed"] == 3.6
    # Fields the current endpoint doesn't provide stay null.
    assert m["temp_min"] is None
    assert m["visibility"] is None

    assert rec["condition"] == {"main": "Clouds", "description": "overcast", "code": 3}


def test_normalize_record_falls_back_to_now_without_time():
    current = _sample_current()
    del current["time"]
    rec = normalize_record(_sample_location(), current, "metric")
    # Still produces an ISO timestamp (current time).
    assert "T" in rec["observed_at"]
