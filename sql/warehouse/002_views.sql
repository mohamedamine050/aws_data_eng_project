-- =============================================================================
-- Serving views — the shapes BI tools should point at.
--
-- A view here is a decision recorded once: which denominator, which sign
-- convention, which status counts as revenue. Every dashboard that reuses one
-- of these agrees with every other dashboard by construction.
--
--   psql "$RDS_URL" -f sql/warehouse/002_views.sql
-- =============================================================================

SET search_path TO analytics, public;

-- ── Daily revenue, the single agreed definition ──────────────────────────────
-- signed_net_amount is already negative for cancellations and refunds, so this
-- is net of reversals without a CASE anyone can forget.
CREATE OR REPLACE VIEW analytics.v_revenue_daily AS
SELECT
    partition_date::date                                       AS day,
    channel,
    count(*) FILTER (WHERE event_type = 'order_placed')         AS orders,
    count(DISTINCT customer_id)                                 AS customers,
    round(sum(signed_net_amount), 2)                            AS net_revenue,
    round(sum(signed_net_amount) FILTER (WHERE event_type = 'order_placed'), 2) AS gross_revenue,
    round(-sum(signed_net_amount) FILTER (WHERE event_type IN ('order_cancelled', 'refund_issued')), 2) AS reversals
FROM analytics.fact_events
WHERE partition_date <> 'unknown'
GROUP BY 1, 2;


-- ── Conversion funnel, in sessions ───────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.v_funnel AS
SELECT
    partition_date::date AS day,
    channel,
    sessions,
    viewed,
    carted,
    checked_out,
    ordered,
    view_to_cart_pct,
    cart_to_checkout_pct,
    checkout_to_order_pct,
    overall_conversion_pct,
    revenue_per_session
FROM analytics.agg_funnel_daily
WHERE partition_date <> 'unknown';


-- ── Customer 360, as far as the event stream can see ─────────────────────────
-- Everything here is derived from behaviour. There is no operational customer
-- record in this pipeline, so a customer who has never generated an event does
-- not exist downstream — which is the honest consequence of an event-only
-- architecture, not an oversight.
CREATE OR REPLACE VIEW analytics.v_customer_360 AS
SELECT
    r.customer_id,
    r.country,
    r.declared_segment,
    r.rfm_segment,
    r.recency_days,
    r.orders,
    r.monetary            AS lifetime_revenue,
    r.avg_order_value,
    r.refund_rate_pct,
    r.is_buyer,
    r.sessions,
    s.last_session_at,
    round(s.revenue_per_session, 2) AS revenue_per_session
FROM analytics.dim_customer_rfm r
LEFT JOIN (
    SELECT
        customer_id,
        max(session_end)  AS last_session_at,
        avg(revenue)      AS revenue_per_session
    FROM analytics.fact_sessions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
) s ON s.customer_id = r.customer_id;


-- ── Product performance over the whole history ───────────────────────────────
-- Revenue and conversion only: unit cost lives in the shop's own systems, which
-- this pipeline does not read, so margin is deliberately absent rather than
-- guessed.
CREATE OR REPLACE VIEW analytics.v_product_performance AS
SELECT
    product_id,
    max(product_name)                 AS product_name,
    max(category)                     AS category,
    max(brand)                        AS brand,
    round(avg(avg_price), 2)          AS avg_price,
    sum(views)                        AS lifetime_views,
    sum(cart_adds)                    AS lifetime_cart_adds,
    sum(units_sold)                   AS lifetime_units_sold,
    round(sum(revenue), 2)            AS lifetime_revenue,
    count(DISTINCT partition_date)    AS active_days,
    CASE WHEN sum(views) > 0
         THEN round(100.0 * sum(units_sold) / sum(views), 2) END AS view_to_unit_pct
FROM analytics.agg_product_daily
WHERE partition_date <> 'unknown'
GROUP BY product_id;


-- ── Anomalies still open, worst first ────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.v_anomalies_recent AS
SELECT
    partition_date::date AS day,
    severity,
    reasons,
    count(*)                        AS events,
    round(sum(net_amount), 2)       AS amount_at_risk,
    count(DISTINCT customer_id)     AS customers
FROM analytics.fact_anomalies
WHERE partition_date <> 'unknown'
  AND partition_date::date >= current_date - interval '30 days'
GROUP BY 1, 2, 3
ORDER BY (severity = 'high') DESC, amount_at_risk DESC NULLS LAST;
