"""Scheduled weather producer Lambda (EventBridge -> Lambda -> Kinesis).

A *single-batch* producer, triggered on a schedule by Amazon EventBridge
(e.g. once per hour):

    EventBridge (rate/cron) ──> [this Lambda] ──> Kinesis Data Stream

On each invocation it fetches current weather for the configured locations from
the Open-Meteo API and pushes one record per location to Kinesis. There is no
loop: EventBridge controls the cadence, the function does one batch and returns.

Data source: **Open-Meteo** (https://open-meteo.com) — free, **no API key
required**. It is queried by latitude/longitude, so locations are configured as
city + coordinates (see the LOCATIONS env var below).

For a fast, dependency-light cold start this handler uses only the standard
library (`urllib`) for HTTP and `boto3` (already in the runtime) for Kinesis.
The record schema lives in `common.weather_schema` so the event shape is defined
in one place.

Packaging: zip this file together with the `common/` package, e.g.

    src/lambdas/weather_producer/handler.py  ->  handler.py
    src/common/weather_schema.py             ->  common/weather_schema.py

Handler entrypoint: ``handler.lambda_handler``

Configuration
-------------
Driven by a JSON config file, pointed to by the CONFIG_PATH environment variable
(local path or s3://bucket/key). Config keys:

    KINESIS_STREAM_NAME   (required)  Target Kinesis stream.
    OPEN_METEO_API_KEY    (optional)  Only for a paid Open-Meteo plan; switches to
                                      the customer endpoint + apikey param. Free
                                      tier needs no key.
    LOCATIONS             (optional)  List of {city, latitude, longitude[, country]}.
                                      Defaults to a small built-in set.
    UNITS                 (optional)  metric (default) | imperial.
    HTTP_TIMEOUT          (optional)  Per-request timeout seconds. Default 10.

Env vars: CONFIG_PATH (required), LOG_LEVEL (optional).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common.weather_schema import normalize_record

LOGGER = logging.getLogger()
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# ─────────────────────────────────────────────
# CONFIG / CONSTANTS
# ─────────────────────────────────────────────

# Free endpoint (no key). The paid/commercial plan uses a different host and an
# `apikey` query param; see _open_meteo_url().
OPEN_METEO_FREE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_CUSTOMER_URL = "https://customer-api.open-meteo.com/v1/forecast"

# Current-weather variables requested from Open-Meteo (well-supported set).
CURRENT_VARS = ",".join([
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "weather_code",
])

# Unit-system mapping -> Open-Meteo query units.
UNIT_PARAMS = {
    "metric": {"temperature_unit": "celsius", "wind_speed_unit": "ms"},
    "imperial": {"temperature_unit": "fahrenheit", "wind_speed_unit": "mph"},
}

DEFAULT_LOCATIONS = [
    {"city": "London", "country": "GB", "latitude": 51.5072, "longitude": -0.1276},
    {"city": "Paris", "country": "FR", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Tokyo", "country": "JP", "latitude": 35.6762, "longitude": 139.6503},
    {"city": "New York", "country": "US", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Tunis", "country": "TN", "latitude": 36.8065, "longitude": 10.1815},
]

# Created at module load so they are reused across warm invocations.
_KINESIS = boto3.client("kinesis")


# ─────────────────────────────────────────────
# ARGS & CONFIG
# ─────────────────────────────────────────────

def get_args(event: Dict[str, Any]) -> Dict[str, str]:
    """Resolve runtime args. CONFIG_PATH is passed as an argument via the
    invocation event (e.g. an EventBridge constant input
    {"CONFIG_PATH": "s3://..."}), with a fallback to the CONFIG_PATH env var.
    It points to a JSON config (local file or s3://...)."""
    config_path = (event or {}).get("CONFIG_PATH") or os.environ.get("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH not provided (event argument or environment).")
    return {"CONFIG_PATH": config_path}


def load_config(path: str) -> Dict[str, Any]:
    """Load the JSON config from S3 (s3://bucket/key) or a local file."""
    LOGGER.info("Loading config from %s", path)
    if path.startswith("s3://"):
        parsed = urllib.parse.urlparse(path)
        obj = boto3.client("s3").get_object(
            Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return json.loads(obj["Body"].read().decode("utf-8"))
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ─────────────────────────────────────────────
# LOCATIONS
# ─────────────────────────────────────────────

def _slugify(city: str, country: Optional[str]) -> str:
    base = city.strip().lower().replace(" ", "-")
    return f"{base}-{country.strip().lower()}" if country else base


def resolve_locations(config_locations: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Build the list of locations from config, or fall back to the defaults.

    Each config entry is an object: {city, latitude, longitude[, country]}.
    A deterministic location_id is derived for use as the Kinesis partition key.
    """
    source = config_locations if config_locations else DEFAULT_LOCATIONS
    locations: List[Dict[str, Any]] = []
    for entry in source:
        try:
            locations.append({
                "city": entry["city"],
                "country": entry.get("country"),
                "latitude": float(entry["latitude"]),
                "longitude": float(entry["longitude"]),
                "location_id": _slugify(entry["city"], entry.get("country")),
            })
        except (KeyError, TypeError, ValueError) as exc:
            LOGGER.warning("Skipping malformed location %r: %s", entry, exc)
    return locations


# ─────────────────────────────────────────────
# EXTRACT (Open-Meteo)
# ─────────────────────────────────────────────

def _open_meteo_url(params: Dict[str, Any], api_key: str = "") -> str:
    """Build the request URL.

    Free tier by default (no key). If api_key is provided (paid plan), use the
    customer host and append the apikey parameter.
    """
    base = OPEN_METEO_FREE_URL
    if api_key:
        base = OPEN_METEO_CUSTOMER_URL
        params = {**params, "apikey": api_key}
    return f"{base}?{urllib.parse.urlencode(params)}"


def fetch_weather(location: Dict[str, Any], units: str, timeout: int,
                  api_key: str = "") -> Optional[Dict[str, Any]]:
    """Fetch + normalize current weather for one location using Open-Meteo."""
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": CURRENT_VARS,
        "timezone": "UTC",
    }
    params.update(UNIT_PARAMS.get(units, UNIT_PARAMS["metric"]))
    url = _open_meteo_url(params, api_key)
    req = urllib.request.Request(url, headers={"User-Agent": "rt-weather-producer/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        LOGGER.warning("Weather fetch failed for %s: HTTP %s %s",
                       location["city"], exc.code, exc.reason)
        return None
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        LOGGER.warning("Weather fetch failed for %s: %s", location["city"], exc)
        return None

    current = payload.get("current")
    if not current:
        LOGGER.warning("No 'current' block for %s: %s", location["city"], payload.get("reason", ""))
        return None
    return normalize_record(location, current, units)


# ─────────────────────────────────────────────
# LOAD (Kinesis)
# ─────────────────────────────────────────────

def put_records(stream_name: str, records: List[Dict[str, Any]]) -> int:
    """Send records to Kinesis with PutRecords. Returns the failed count."""
    if not records:
        return 0

    entries = [
        {
            "Data": (json.dumps(rec) + "\n").encode("utf-8"),
            "PartitionKey": str(rec["location"]["location_id"]),
        }
        for rec in records
    ]
    try:
        resp = _KINESIS.put_records(StreamName=stream_name, Records=entries)
    except (BotoCoreError, ClientError) as exc:
        LOGGER.error("PutRecords failed entirely: %s", exc)
        return len(records)

    failed = resp.get("FailedRecordCount", 0)
    if failed:
        for result in resp["Records"]:
            if "ErrorCode" in result:
                LOGGER.warning("Record failed: %s - %s", result["ErrorCode"], result.get("ErrorMessage"))
    return failed


# ─────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    args = get_args(event)
    config = load_config(args["CONFIG_PATH"])

    stream_name = config.get("KINESIS_STREAM_NAME")
    if not stream_name:
        raise RuntimeError("KINESIS_STREAM_NAME is missing from the config.")

    # Optional: only needed for a paid Open-Meteo plan. Empty -> free tier.
    api_key = str(config.get("OPEN_METEO_API_KEY", "")).strip()

    units = config.get("UNITS", "metric")
    timeout = int(config.get("HTTP_TIMEOUT", 10))
    locations = resolve_locations(config.get("LOCATIONS"))

    records: List[Dict[str, Any]] = []
    for loc in locations:
        rec = fetch_weather(loc, units, timeout, api_key)
        if rec is not None:
            records.append(rec)

    failed = put_records(stream_name, records)
    sent = len(records) - failed
    result = {
        "locations_requested": len(locations),
        "fetched": len(records),
        "sent": sent,
        "failed": failed,
    }
    LOGGER.info("Producer run: %s", result)
    return result
