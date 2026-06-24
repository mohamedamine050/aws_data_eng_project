"""Weather event schema.

Stdlib-only (no third-party imports) so the scheduled producer Lambda
(`src/lambdas/weather_producer/handler.py`) can import it without dragging extra
dependencies into its deployment package.

Centralizing `normalize_record` here keeps the record schema sent to Kinesis in
one place, so the downstream stream-processor Lambda always sees a single shape.

Data source: Open-Meteo (https://open-meteo.com) — a free weather API that needs
no API key. It is queried by latitude/longitude and returns numeric WMO weather
codes, which we translate to human-readable conditions below.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

SCHEMA_VERSION = "2.0"

# WMO weather interpretation codes used by Open-Meteo, mapped to a coarse
# "main" category + a human-readable description.
# Reference: https://open-meteo.com/en/docs (WMO Weather interpretation codes)
WMO_WEATHER_CODES: Dict[int, Tuple[str, str]] = {
    0: ("Clear", "clear sky"),
    1: ("Clouds", "mainly clear"),
    2: ("Clouds", "partly cloudy"),
    3: ("Clouds", "overcast"),
    45: ("Fog", "fog"),
    48: ("Fog", "depositing rime fog"),
    51: ("Drizzle", "light drizzle"),
    53: ("Drizzle", "moderate drizzle"),
    55: ("Drizzle", "dense drizzle"),
    56: ("Drizzle", "light freezing drizzle"),
    57: ("Drizzle", "dense freezing drizzle"),
    61: ("Rain", "slight rain"),
    63: ("Rain", "moderate rain"),
    65: ("Rain", "heavy rain"),
    66: ("Rain", "light freezing rain"),
    67: ("Rain", "heavy freezing rain"),
    71: ("Snow", "slight snow fall"),
    73: ("Snow", "moderate snow fall"),
    75: ("Snow", "heavy snow fall"),
    77: ("Snow", "snow grains"),
    80: ("Rain", "slight rain showers"),
    81: ("Rain", "moderate rain showers"),
    82: ("Rain", "violent rain showers"),
    85: ("Snow", "slight snow showers"),
    86: ("Snow", "heavy snow showers"),
    95: ("Thunderstorm", "thunderstorm"),
    96: ("Thunderstorm", "thunderstorm with slight hail"),
    99: ("Thunderstorm", "thunderstorm with heavy hail"),
}


def describe_weather_code(code: Optional[int]) -> Tuple[Optional[str], Optional[str]]:
    """Return (main, description) for a WMO weather code, or (None, None)."""
    if code is None:
        return None, None
    return WMO_WEATHER_CODES.get(int(code), ("Unknown", f"weather code {code}"))


def _to_utc_iso(time_str: Optional[str]) -> str:
    """Parse an Open-Meteo 'current.time' value (UTC) into an ISO-8601 string."""
    if time_str:
        try:
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def normalize_record(location: Dict[str, Any], current: Dict[str, Any], units: str) -> Dict[str, Any]:
    """Map an Open-Meteo 'current' block to our stable event schema.

    `location` is the configured place: {city, country, latitude, longitude,
    location_id}. `current` is the `current` object from the Open-Meteo
    response. `units` is the unit system the request was made with.
    """
    code = current.get("weather_code")
    cond_main, cond_desc = describe_weather_code(code)

    observed_at = _to_utc_iso(current.get("time"))
    location_id = location.get("location_id") or location.get("city")

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{location_id}-{current.get('time') or int(time.time())}",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "observed_at": observed_at,
        "units": units,
        "location": {
            "city": location.get("city"),
            "country": location.get("country"),
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
            "location_id": location_id,
        },
        "measurement": {
            "temperature": current.get("temperature_2m"),
            "feels_like": current.get("apparent_temperature"),
            "temp_min": None,  # not provided by the current-weather endpoint
            "temp_max": None,
            "pressure": current.get("surface_pressure"),
            "humidity": current.get("relative_humidity_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "wind_deg": current.get("wind_direction_10m"),
            "cloudiness": current.get("cloud_cover"),
            "visibility": current.get("visibility"),
        },
        "condition": {
            "main": cond_main,
            "description": cond_desc,
            "code": code,
        },
    }
