-- =============================================================================
-- Example analyses — the questions this warehouse was built to answer.
-- Copy/paste material, not part of any deployment.
-- =============================================================================

SET search_path TO analytics, public;

-- ── 1. Where is revenue going, by channel, this month? ───────────────────────
SELECT day, channel, orders, net_revenue, reversals
FROM analytics.v_revenue_daily
WHERE day >= date_trunc('month', current_date)
ORDER BY day DESC, net_revenue DESC;


-- ── 2. Which funnel step is leaking the most sessions? ───────────────────────
-- The biggest absolute drop, not the worst percentage: a 90% drop on 12
-- sessions is noise, a 30% drop on 40 000 is the quarter.
SELECT
    channel,
    sum(viewed - carted)      AS lost_view_to_cart,
    sum(carted - checked_out) AS lost_cart_to_checkout,
    sum(checked_out - ordered) AS lost_checkout_to_order
FROM analytics.agg_funnel_daily
WHERE partition_date::date >= current_date - 30
GROUP BY channel
ORDER BY greatest(
    sum(viewed - carted), sum(carted - checked_out), sum(checked_out - ordered)
) DESC;


-- ── 3. Who is about to churn, and how much is that worth? ────────────────────
SELECT
    rfm_segment,
    count(*)                       AS customers,
    round(sum(monetary), 2)        AS revenue_at_stake,
    round(avg(recency_days), 1)    AS avg_days_since_last_order
FROM analytics.dim_customer_rfm
WHERE is_buyer
GROUP BY rfm_segment
ORDER BY revenue_at_stake DESC;


-- ── 4. Which products get looked at but not bought? ──────────────────────────
-- High views with a low view-to-unit rate is a pricing, imagery or stock
-- problem — the event stream can point at it even without knowing unit cost.
SELECT
    product_name,
    category,
    lifetime_views,
    lifetime_units_sold,
    lifetime_revenue,
    view_to_unit_pct
FROM analytics.v_product_performance
WHERE lifetime_views >= 100
ORDER BY view_to_unit_pct ASC NULLS LAST, lifetime_views DESC
LIMIT 20;


-- ── 5. Does the order table agree with the event table? ──────────────────────
-- The reconciliation that catches a broken producer before a dashboard does.
SELECT
    o.partition_date::date                        AS day,
    count(DISTINCT o.order_id)                    AS orders_in_gold,
    count(DISTINCT e.order_id)                    AS orders_in_facts,
    round(sum(o.realized_revenue), 2)             AS revenue_in_gold
FROM analytics.fact_orders o
LEFT JOIN analytics.fact_events e
       ON e.order_id = o.order_id AND e.event_type = 'order_placed'
WHERE o.partition_date::date >= current_date - 7
GROUP BY 1
ORDER BY 1 DESC;


-- ── 6. Session quality: what does a converting session look like? ────────────
SELECT
    converted,
    count(*)                            AS sessions,
    round(avg(duration_seconds), 1)     AS avg_seconds,
    round(avg(events), 1)               AS avg_events,
    round(avg(distinct_products), 1)    AS avg_products_seen,
    round(avg(revenue), 2)              AS avg_revenue
FROM analytics.fact_sessions
WHERE partition_date::date >= current_date - 30
GROUP BY converted;


-- ── 7. Anomaly triage for the last week ──────────────────────────────────────
SELECT day, severity, reasons, events, amount_at_risk
FROM analytics.v_anomalies_recent
WHERE day >= current_date - 7
ORDER BY amount_at_risk DESC NULLS LAST
LIMIT 25;
