# Real-Time E-Commerce Event Ingestion — Data Engineering Scripts

The **data engineering code** for a serverless, cost-optimized real-time
E-commerce event ingestion pipeline on AWS. This repo contains the **scripts
only** — the processing logic for each stage — not the infrastructure (no
Terraform/CloudFormation).

```
EventBridge ──> ecommerce_producer Lambda ──> SQS queue ──> stream_processor Lambda ──> S3 (raw)
```

| Stage | Component | What it does |
|-------|-----------|--------------|
| Produce | Producer Lambda | Scheduled Lambda (EventBridge); generates e-commerce events for configured products and sends JSON messages to SQS |
| Process | Stream processor Lambda | SQS-triggered Lambda; validates/enriches records and lands NDJSON in S3 `raw/`, partitioned by date/hour |

## Layout

The project is organized around a small set of components:
- a shared schema module for event normalization,
- a producer Lambda for generating events,
- a stream processor Lambda for landing them in S3,
- a small test suite and configuration template.

## 1. Producer — E-commerce events → SQS (scheduled Lambda)

The producer runs as a **scheduled Lambda**, triggered by EventBridge:

```
EventBridge (rate/cron)  ──>  ecommerce_producer Lambda  ──>  SQS
```

The producer Lambda is a **single-batch** component (entrypoint
`handler.lambda_handler`). On each invocation it generates e-commerce events
for the configured products and sends one message per record to SQS. It uses
only the standard library + `boto3`, so it needs **no extra dependencies or
layer** — EventBridge controls the cadence, the function does one fetch-and-send
per call. It sends all events via `SendMessageBatch` (batches of up to 10).

Data source: a product or event feed (for example an e-commerce API or catalog
service). The payload is normalized into a stable schema before being sent to SQS.

**Package** the producer component together with its shared schema module into a
single deployment artifact.

**Configuration.** `CONFIG_PATH` is passed as an **argument** in the invocation
event — set the EventBridge rule's constant input to
`{"CONFIG_PATH": "s3://bucket/config/ecommerce_producer.json"}` (falls back to a
`CONFIG_PATH` env var if absent). It points to a JSON config file (on S3, or a
local path for testing). Example:

```json
{
  "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/ecommerce-queue",
  "CHANNEL": "web",
  "PRODUCTS": [
    { "product_id": "sku-1001", "sku": "SKU-1001", "name": "Wireless Mouse", "price": 49.99 }
  ]
}
```

| Config key | Required | Notes |
|-----------|----------|-------|
| `QUEUE_URL` | yes | target SQS queue URL |
| `CHANNEL` | no | channel name for the generated event, default `web` |
| `PRODUCTS` | no | list of product objects, default sample SKUs |

**EventBridge schedule** — trigger every hour. Either a classic rule:

```bash
aws events put-rule --name ecommerce-producer-hourly \\
  --schedule-expression "rate(1 hour)"
aws lambda add-permission --function-name ecommerce-producer \\
  --statement-id eventbridge-invoke --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:<region>:<account>:rule/ecommerce-producer-hourly
aws events put-targets --rule ecommerce-producer-hourly \\
  --targets "Id"="1","Arn"="arn:aws:lambda:<region>:<account>:function:ecommerce-producer"
```

or an EventBridge **Scheduler** schedule (`cron(0 * * * ? *)` for top of every hour).
The handler returns a small summary (`{locations_requested, fetched, sent, failed}`)
that shows up in CloudWatch Logs.

> IAM: the Lambda's execution role needs `sqs:SendMessage` on the queue,
> plus the basic Lambda logging permissions.

## 2. Stream processor — SQS → S3 raw

The stream processor Lambda is the handler (`handler.handler`). Pure standard
library + `boto3` (already in the Lambda runtime), so the deployment package is
just the handler file.

- Configure the SQS **event source mapping** with
  `FunctionResponseTypes = ["ReportBatchItemFailures"]` so only failed messages
  are retried (the handler returns `batchItemFailures` by `messageId`).
- Poison/invalid messages are logged and dropped (never block the queue).
- **Configuration** via `CONFIG_PATH` → JSON config file on S3, e.g.
  `{"OUTPUT_BUCKET": "my-data-lake-bucket", "RAW_PREFIX": "raw/"}` —
  `OUTPUT_BUCKET` (required), `RAW_PREFIX` (optional, default `raw/`).
  `get_args(event)` reads `CONFIG_PATH` from the event argument, but since the
  SQS trigger carries messages (not config), set `CONFIG_PATH` as an env var
  here. `LOG_LEVEL` optional.

Output objects: `s3://<bucket>/raw/dt=YYYY-MM-DD/hour=HH/<ts>-<uuid>.json`
(newline-delimited JSON, partitioned by event date/hour).

## Record schema (producer → SQS → stream processor → S3)

```json
{
  "schema_version": "2.0",
  "event_id": "product_viewed-sku-1001-2026-06-24T12:00",
  "ingested_at": "2026-06-24T12:00:05+00:00",
  "occurred_at": "2026-06-24T12:00:00+00:00",
  "channel": "web",
  "event_type": "product_viewed",
  "product": { "product_id": "sku-1001", "sku": "SKU-1001", "name": "Wireless Mouse", "category": "electronics", "price": 49.99 },
  "customer": { "customer_id": "cust-demo", "segment": "new" },
  "order": { "amount": 49.99, "currency": "EUR" }
}
```

The producer normalizes upstream e-commerce payloads into this stable schema.
Keeping a stable schema means the stream processor doesn't break if the upstream
API shape changes. This makes the pipeline ready for product events, order
updates, or customer activity streams.

## Tests

Unit tests (pytest) cover the schema mapping and both Lambda handlers — no AWS or
network access (boto3 clients are mocked/stubbed):

```bash
pip install -r requirements-dev.txt
pytest
```

The test suite covers config loading, product parsing, event generation, SQS
sends, decode/validate, partitioning, and the end-to-end handler paths
including dropped-record and S3-failure cases.

## Notes

- These scripts assume the AWS resources (SQS queue, S3 bucket, the two
  Lambdas, EventBridge rule) already exist — provision them with your IaC tool of
  choice and wire the names/env vars in.
- Cost-optimization choices baked into the code: batched SQS sends, NDJSON
  output, date/hour partitioning of the raw zone, and partial-batch Lambda
  responses to avoid reprocessing whole batches.
