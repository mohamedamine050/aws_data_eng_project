# Sample data — enough to run the pipeline end to end

Four files, generated once and committed so a first run has something to chew
on. Every row was passed through the project's own normalisers and quality
rules before being written: **89 events, 0 rejected**.

| File | Feeds | Read by |
|------|-------|---------|
| `catalog/products.csv` | the product catalog | `ecommerce_producer` λ — `PRODUCTS_S3_CSV` |
| `catalog/customers.csv` | the customer pool | `ecommerce_producer` λ — `CUSTOMERS_S3_CSV` |
| `landing/partner_events.csv` | source ② — a partner's flat export | **Glue job 1**, `glue_landing_ingest` |
| `landing/partner_events.ndjson` | source ② — an exporter that already speaks v3 | the same job, read with the schema |

## Try it locally

```bash
# source ② — the lift and the checks, on a local SparkSession
pytest tests/test_glue_landing_ingest.py -q
```

```bash
# source ① — point the producer at the catalog and let it generate sessions
export PRODUCTS_LOCAL=data/catalog/products.csv
export CUSTOMERS_LOCAL=data/catalog/customers.csv
```

## Uploading them

```bash
aws s3 cp data/catalog/  s3://$OUTPUT_BUCKET/catalog/  --recursive
aws s3 cp data/landing/partner_events.csv s3://$OUTPUT_BUCKET/landing/partners/
```

The second command is the whole of source ②: the `ObjectCreated` notification on
`landing/partners/` goes through EventBridge and starts the Glue job, which
expands the file into events, writes them to `bronze/events/`, and moves the
file to `landing/_processed/` so the next run does not read it twice.

## About the flat CSV

A partner exports **columns**, not nested objects — `product_id` sits at the top
level while everything downstream looks for `product.product_id`. `lift_flat` in
the landing job builds the nested blocks as Spark columns, does the basket
arithmetic once, and hashes the same seven identity fields
`common/ecommerce_schema.py` hashes. A dropped file and a queued event are
therefore the same record — same shape, same `idempotency_key` — by the time
either reaches silver. A test pins that hash against the Python implementation.

A row with no product id, or a timestamp that will not parse, is not given a
placeholder: it goes to `quarantine/landing/` carrying the names of the rules it
broke. Inventing an `unknown` product would turn a junk row into a
plausible-looking event.

## Shape of the partner export

```
occurred_at,event_type,channel,session_id,device_type,country,city,
product_id,product_name,category,brand,price,
customer_id,segment,order_id,quantity,discount_pct,
currency,payment_method,campaign,utm_source,utm_medium
```

40 browsing sessions over one day, following the funnel
`product_viewed → add_to_cart → checkout_started → order_placed` with the
attrition of a real shop: most sessions never buy, 7 orders come out of 89
events. `order_id`, `payment_method` and `discount_pct` are empty on the stages
where they have no meaning — which is also what makes the file a useful test of
the null handling downstream.
