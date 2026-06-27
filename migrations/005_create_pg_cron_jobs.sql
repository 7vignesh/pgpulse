-- ===========================================================================
-- 005_create_pg_cron_jobs.sql
-- Scheduled maintenance: MV refresh + rolling partition creation.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Refresh hourly_stats every hour at minute 0. CONCURRENTLY avoids locking
-- readers (requires the UNIQUE index from migration 004).
-- ---------------------------------------------------------------------------
SELECT cron.schedule(
  'refresh-hourly-stats',
  '0 * * * *',
  'REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_stats'
);

-- ---------------------------------------------------------------------------
-- Pre-create next month's partition daily at 02:00 so ingest never hits the
-- default partition. This is a lightweight, idempotent stand-in for pg_partman
-- (see SCALING.md for the pg_partman migration path).
-- ---------------------------------------------------------------------------
SELECT cron.schedule(
  'create-next-partition',
  '0 2 * * *',
  $$SELECT create_events_partition((date_trunc('month', now()) + INTERVAL '1 month')::date)$$
);

-- ---------------------------------------------------------------------------
-- Optional retention example (disabled by default). Detaches partitions older
-- than 12 months. Uncomment and adapt for production retention policy.
-- ---------------------------------------------------------------------------
-- SELECT cron.schedule(
--   'drop-old-partitions',
--   '30 2 * * *',
--   $$ ... custom plpgsql to DETACH + DROP partitions older than retention ... $$
-- );
