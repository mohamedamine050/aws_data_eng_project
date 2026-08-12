-- =============================================================================
-- THE WAREHOUSE — what glue_rds_load writes into, and what BI reads from.
--
--   s3://<lake>/silver/events  --> analytics.fact_events
--   s3://<lake>/gold/*         --> analytics.fact_* / dim_* / agg_*
--
-- Every table here is derived from the event stream. The pipeline reads no
-- operational database, so there is no conformed customer or product
-- dimension — only what behaviour reveals about them.
--
-- The loader writes with Spark JDBC in `append` or `overwrite` mode. Overwrite
-- TRUNCATEs rather than dropping, so these definitions — and their indexes and
-- grants — survive every load. That is why the tables are created here and not
-- inferred by Spark.
--
--   psql "$RDS_URL" -f sql/warehouse/001_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;


-- -----------------------------------------------------------------------------
-- FACT — one row per event. The grain of everything else.
-- Mirrors transforms.PROCESSED_COLUMNS, in order.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_events (
    idempotency_key   text,
    event_id          text,
    event_type        text,
    occurred_ts       timestamptz,
    occurred_at       text,
    ingested_at       text,
    channel           text,
    device_type       text,
    device_os         text,
    session_id        text,
    event_sequence    integer,
    product_id        text,
    sku               text,
    product_name      text,
    category          text,
    brand             text,
    product_price     numeric(12,2),
    customer_id       text,
    customer_segment  text,
    customer_country  text,
    is_returning      boolean,
    order_id          text,
    quantity          integer,
    unit_price        numeric(12,2),
    discount_pct      numeric(5,2),
    gross_amount      numeric(12,2),
    discount_amount   numeric(12,2),
    net_amount        numeric(12,2),
    -- Cancellations and refunds are stored negative, so net revenue is a plain
    -- SUM anywhere downstream instead of a CASE nobody remembers to write.
    signed_net_amount numeric(12,2),
    currency          char(3),
    payment_method    text,
    campaign          text,
    utm_source        text,
    utm_medium        text,
    price_category    text,
    is_revenue_event  boolean,
    is_conversion     boolean,
    day_of_week       text,
    is_weekend        boolean,
    partition_date    text,
    partition_hour    text
);

-- fact_events is appended to, so a replayed window would double-count. This
-- unique index turns that silent corruption into a loud failure.
--   Consequence: a direct `append` of overlapping data ABORTS, because Spark's
--   JDBC writer has no ON CONFLICT. Replays go through the staging upsert in
--   003_upsert.sql, which is the supported path for re-running a partition.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_events_key
    ON analytics.fact_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fact_events_date     ON analytics.fact_events (partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_events_type     ON analytics.fact_events (event_type);
CREATE INDEX IF NOT EXISTS idx_fact_events_customer ON analytics.fact_events (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_product  ON analytics.fact_events (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_session  ON analytics.fact_events (session_id);


-- -----------------------------------------------------------------------------
-- FACT — one row per browsing session
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_sessions (
    session_id        text PRIMARY KEY,
    customer_id       text,
    channel           text,
    device_type       text,
    country           text,
    campaign          text,
    utm_source        text,
    session_start     timestamptz,
    session_end       timestamptz,
    events            bigint,
    distinct_products bigint,
    views             bigint,
    cart_adds         bigint,
    cart_removals     bigint,
    checkouts         bigint,
    payment_failures  bigint,
    orders            bigint,
    revenue           numeric(14,2),
    duration_seconds  bigint,
    converted         boolean,
    bounced           boolean,
    partition_date    text
);

CREATE INDEX IF NOT EXISTS idx_fact_sessions_date     ON analytics.fact_sessions (partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_sessions_customer ON analytics.fact_sessions (customer_id);


-- -----------------------------------------------------------------------------
-- FACT — one row per order
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id         text PRIMARY KEY,
    customer_id      text,
    session_id       text,
    channel          text,
    device_type      text,
    country          text,
    currency         char(3),
    payment_method   text,
    campaign         text,
    ordered_at       timestamptz,
    line_items       bigint,
    units            bigint,
    gross_amount     numeric(14,2),
    discount_amount  numeric(14,2),
    net_amount       numeric(14,2),
    cancelled        boolean,
    refunded         boolean,
    reversed_amount  numeric(14,2),
    status           text,
    realized_revenue numeric(14,2),
    avg_item_value   numeric(14,2),
    partition_date   text
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date     ON analytics.fact_orders (partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON analytics.fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status   ON analytics.fact_orders (status);


-- -----------------------------------------------------------------------------
-- FACT — flagged events awaiting a human
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_anomalies (
    partition_date  text,
    occurred_ts     timestamptz,
    event_type      text,
    idempotency_key text,
    session_id      text,
    customer_id     text,
    product_id      text,
    order_id        text,
    quantity        integer,
    discount_pct    numeric(5,2),
    net_amount      numeric(12,2),
    channel         text,
    reasons         text[],
    severity        text
);

CREATE INDEX IF NOT EXISTS idx_fact_anomalies_date     ON analytics.fact_anomalies (partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_anomalies_severity ON analytics.fact_anomalies (severity);


-- -----------------------------------------------------------------------------
-- DIM — one row per customer, scored from their own behaviour
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_customer_rfm (
    customer_id       text PRIMARY KEY,
    sessions          bigint,
    events            bigint,
    declared_segment  text,
    country           text,
    orders            bigint,
    monetary          numeric(14,2),
    first_order_at    timestamptz,
    last_order_at     timestamptz,
    units             bigint,
    avg_order_value   numeric(14,2),
    refunded_orders   bigint,
    recency_days      integer,
    refund_rate_pct   numeric(6,2),
    r_score           integer,
    f_score           integer,
    m_score           integer,
    rfm_score         integer,
    rfm_segment       text,
    is_buyer          boolean
);


-- -----------------------------------------------------------------------------
-- AGG — daily funnel by channel
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_funnel_daily (
    partition_date        text,
    channel               text,
    sessions              bigint,
    customers             bigint,
    viewed                bigint,
    carted                bigint,
    checked_out           bigint,
    ordered               bigint,
    revenue               numeric(14,2),
    view_to_cart_pct      numeric(6,2),
    cart_to_checkout_pct  numeric(6,2),
    checkout_to_order_pct numeric(6,2),
    overall_conversion_pct numeric(6,2),
    revenue_per_session   numeric(12,2),
    PRIMARY KEY (partition_date, channel)
);


-- -----------------------------------------------------------------------------
-- AGG — daily product performance
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_product_daily (
    partition_date    text,
    product_id        text,
    product_name      text,
    category          text,
    brand             text,
    price_category    text,
    avg_price         numeric(12,2),
    views             bigint,
    cart_adds         bigint,
    orders            bigint,
    units_sold        bigint,
    revenue           numeric(14,2),
    customers         bigint,
    view_to_order_pct numeric(6,2),
    revenue_rank      integer,
    PRIMARY KEY (partition_date, product_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_product_daily_category ON analytics.agg_product_daily (category);


-- -----------------------------------------------------------------------------
-- Roles. The loader writes; everyone else reads. Two roles, because a BI tool
-- with UPDATE rights on a warehouse is an incident waiting for a Monday.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        CREATE ROLE pipeline_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_reader') THEN
        CREATE ROLE analytics_reader NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA analytics TO pipeline_writer, analytics_reader;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics TO pipeline_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO pipeline_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO analytics_reader;
