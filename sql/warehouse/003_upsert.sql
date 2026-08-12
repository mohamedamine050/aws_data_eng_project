-- =============================================================================
-- Replay-safe loading — the staging + upsert path.
--
-- Spark's JDBC writer speaks INSERT, not MERGE. With the unique index on
-- fact_events, re-running a partition therefore aborts. That is the correct
-- default (loud beats silent double-counting), but re-running a partition is a
-- normal operation, so it needs a supported route:
--
--   1. point the load at the staging table
--        {"dataset": "silver/events", "table": "analytics.stg_fact_events",
--         "mode": "overwrite"}
--   2. call analytics.merge_fact_events()
--
-- Every other table is rebuilt in full on each run, so the default config
-- overwrites those directly and they need no staging table.
--
--   psql "$RDS_URL" -f sql/warehouse/003_upsert.sql
-- =============================================================================

SET search_path TO analytics, public;

-- Staging mirrors the target exactly; no constraints, because Spark truncates
-- and refills it on every run.
CREATE TABLE IF NOT EXISTS analytics.stg_fact_events
    (LIKE analytics.fact_events INCLUDING DEFAULTS);


-- -----------------------------------------------------------------------------
-- Events: insert what is new, ignore what is already there. Events are
-- immutable facts — a second delivery of the same event carries no new truth.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.merge_fact_events() RETURNS bigint AS $$
DECLARE
    inserted bigint;
BEGIN
    INSERT INTO analytics.fact_events
    SELECT DISTINCT ON (idempotency_key) *
    FROM analytics.stg_fact_events
    WHERE idempotency_key IS NOT NULL
    ORDER BY idempotency_key, ingested_at DESC
    ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    TRUNCATE analytics.stg_fact_events;
    RETURN inserted;
END;
$$ LANGUAGE plpgsql;


GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON analytics.stg_fact_events TO pipeline_writer;
GRANT EXECUTE ON FUNCTION analytics.merge_fact_events() TO pipeline_writer;
