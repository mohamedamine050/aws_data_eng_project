# Real-Time Weather Ingestion — Data Engineering Scripts

The **data engineering code** for a serverless, cost-optimized real-time weather
ingestion pipeline on AWS. This repo contains the **scripts only** — the
processing logic for each stage — not the infrastructure (no
Terraform/CloudFormation).

```
EventBridge ──> weather_producer Lambda ──> Kinesis Stream ──> stream_processor Lambda ──> S3 (raw)
```

| Stage | Script | What it does |
|-------|--------|--------------|
| Produce | [src/lambdas/weather_producer/handler.py](src/lambdas/weather_producer/handler.py) | Scheduled Lambda (EventBridge); fetches current weather from Open-Meteo (no API key) and pushes JSON records to Kinesis |
| Process | [src/lambdas/stream_processor/handler.py](src/lambdas/stream_processor/handler.py) | Kinesis-triggered Lambda; validates/enriches records and lands NDJSON in S3 `raw/`, partitioned by date/hour |

## Layout

```
.
├── src/
│   ├── common/weather_schema.py             # record schema (stdlib only), used by the producer
│   └── lambdas/
│       ├── weather_producer/handler.py      # Weather API -> Kinesis (scheduled Lambda)
│       └── stream_processor/handler.py      # Kinesis -> S3 raw
├── tests/                                   # pytest unit tests
├── requirements.txt                         # boto3 (local testing only)
├── requirements-dev.txt                     # + pytest
├── .env.example                             # CONFIG_PATH env-var template
└── README.md
```

## 1. Producer — Weather API → Kinesis (scheduled Lambda)

The producer runs as a **scheduled Lambda**, triggered by EventBridge:

```
EventBridge (rate/cron)  ──>  weather_producer Lambda  ──>  Kinesis
```

`src/lambdas/weather_producer/handler.py` is a **single-batch** producer
(entrypoint `handler.lambda_handler`). On each invocation it fetches current
weather for the configured locations and pushes one record per location to
Kinesis. It uses only the standard library (`urllib`) + `boto3`, so it needs
**no extra dependencies or layer** — EventBridge controls the cadence, the
function does one fetch-and-push per call. It batches all locations into a single
`PutRecords` call, using each location id as the partition key (ordered per
location).

Data source: **[Open-Meteo](https://open-meteo.com)** — free, **no API key
required**. It is queried by latitude/longitude, so locations are configured as
`city,lat,lon[,country]` (see `LOCATIONS` below).

**Package** it together with the `common` package:

```bash
cd src
mkdir -p ../build/weather_producer
cp lambdas/weather_producer/handler.py ../build/weather_producer/handler.py
cp -r common ../build/weather_producer/common
cd ../build/weather_producer && zip -r ../weather_producer.zip . && cd -
```

Resulting layout inside the zip:

```
handler.py
common/weather_schema.py
```

**Configuration.** `CONFIG_PATH` is passed as an **argument** in the invocation
event — set the EventBridge rule's constant input to
`{"CONFIG_PATH": "s3://bucket/config/weather_producer.json"}` (falls back to a
`CONFIG_PATH` env var if absent). It points to a JSON config file (on S3, or a
local path for testing). Example:

```json
{
  "KINESIS_STREAM_NAME": "rt-weather-analytics-dev-stream",
  "OPEN_METEO_API_KEY": "",
  "UNITS": "metric",
  "HTTP_TIMEOUT": 10,
  "LOCATIONS": [
    { "city": "Tunis", "country": "TN", "latitude": 36.8065, "longitude": 10.1815 }
  ]
}
```

| Config key | Required | Notes |
|-----------|----------|-------|
| `KINESIS_STREAM_NAME` | yes | target stream |
| `OPEN_METEO_API_KEY` | no | only for a paid Open-Meteo plan (customer endpoint + `apikey`); free tier needs no key |
| `LOCATIONS` | no | list of `{city, latitude, longitude[, country]}`, default London/Paris/Tokyo/New York/Tunis |
| `UNITS` | no | `metric` (default) / `imperial` |
| `HTTP_TIMEOUT` | no | per-request seconds, default 10 |

**EventBridge schedule** — trigger every hour. Either a classic rule:

```bash
aws events put-rule --name weather-producer-hourly \
  --schedule-expression "rate(1 hour)"
aws lambda add-permission --function-name weather-producer \
  --statement-id eventbridge-invoke --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:<region>:<account>:rule/weather-producer-hourly
aws events put-targets --rule weather-producer-hourly \
  --targets "Id"="1","Arn"="arn:aws:lambda:<region>:<account>:function:weather-producer"
```

or an EventBridge **Scheduler** schedule (`cron(0 * * * ? *)` for top of every hour).
The handler returns a small summary (`{locations_requested, fetched, sent, failed}`)
that shows up in CloudWatch Logs.

> IAM: the Lambda's execution role needs `kinesis:PutRecords` on the stream,
> plus the basic Lambda logging permissions. No outbound secrets needed —
> Open-Meteo requires no API key.

## 2. Stream processor — Kinesis → S3 raw

`src/lambdas/stream_processor/handler.py` is the handler (`handler.handler`).
Pure standard library + `boto3` (already in the Lambda runtime), so the
deployment package is just this file.

- Configure the Kinesis **event source mapping** with
  `FunctionResponseTypes = ["ReportBatchItemFailures"]` so only failed records
  are retried (the handler returns `batchItemFailures`).
- Poison/invalid records are logged and dropped (never block the shard).
- **Configuration** via `CONFIG_PATH` → JSON config file on S3, e.g.
  `{"OUTPUT_BUCKET": "my-data-lake-bucket", "RAW_PREFIX": "raw/"}` —
  `OUTPUT_BUCKET` (required), `RAW_PREFIX` (optional, default `raw/`).
  `get_args(event)` reads `CONFIG_PATH` from the event argument, but since the
  Kinesis trigger carries records (not config), set `CONFIG_PATH` as an env var
  here. `LOG_LEVEL` optional.

Output objects: `s3://<bucket>/raw/dt=YYYY-MM-DD/hour=HH/<shard>-<ts>.json`
(newline-delimited JSON, partitioned by event date/hour).

## Record schema (producer → Kinesis → stream processor → S3)

```json
{
  "schema_version": "2.0",
  "event_id": "london-gb-2026-06-24T12:00",
  "ingested_at": "2026-06-24T12:00:05+00:00",
  "observed_at": "2026-06-24T12:00:00+00:00",
  "units": "metric",
  "location": { "city": "London", "country": "GB",
                "latitude": 51.5072, "longitude": -0.1276, "location_id": "london-gb" },
  "measurement": { "temperature": 18.2, "feels_like": 17.9, "temp_min": null,
                   "temp_max": null, "pressure": 1012, "humidity": 64,
                   "wind_speed": 3.6, "wind_deg": 210, "cloudiness": 40,
                   "visibility": null },
  "condition": { "main": "Clouds", "description": "overcast", "code": 3 }
}
```

The producer normalizes the Open-Meteo response into this stable schema (in
[src/common/weather_schema.py](src/common/weather_schema.py)), translating WMO
`weather_code` values into `condition.main`/`description`. Keeping a stable
schema means the stream processor doesn't break if the upstream API shape
changes. `temp_min`/`temp_max`/`visibility` are `null` because the
current-weather endpoint doesn't provide them.

## Tests

Unit tests (pytest) cover the schema mapping and both Lambda handlers — no AWS or
network access (boto3 clients and the weather API are mocked/stubbed):

```bash
pip install -r requirements-dev.txt
pytest
```

`tests/` contains `test_weather_schema.py`, `test_weather_producer.py` and
`test_stream_processor.py` (config loading, location parsing, URL building,
fetch/normalize, Kinesis puts, decode/validate, partitioning, and the
end-to-end handler paths including dropped-record and S3-failure cases).

## Notes

- These scripts assume the AWS resources (Kinesis stream, S3 bucket, the two
  Lambdas, EventBridge rule) already exist — provision them with your IaC tool of
  choice and wire the names/env vars in.
- Cost-optimization choices baked into the code: batched Kinesis puts, NDJSON
  output, date/hour partitioning of the raw zone, and partial-batch Lambda
  responses to avoid reprocessing whole batches.
