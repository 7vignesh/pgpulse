-- ===========================================================================
-- 009_create_rollups.sql
-- Incremental 1-minute and 1-hour rollups of `events`, replacing the
-- hourly_stats materialized view (which was rebuilt from ALL history every hour).
--
-- Each rollup table has a watermark in rollup_watermarks. The worker
-- (src/rollup/worker.ts) aggregates only
--     WHERE ingested_at >= watermark AND ingested_at < new_watermark
-- and advances the watermark in the same transaction. Watermarks are aligned to
-- bucket boundaries and are an exclusive upper bound, so a rollup row always
-- describes a COMPLETE bucket, and re-running a range is idempotent.
--
-- Known limit: events that become visible with an ingested_at older than the
-- watermark (client-supplied timestamps in the past, or transactions that
-- commit later than ROLLUP_SAFETY_LAG_SECONDS after their timestamp) are not
-- picked up. See docs/SCALING.md "Rollups".
-- ===========================================================================

CREATE TABLE IF NOT EXISTS rollup_1m (
  tenant_id      UUID             NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  endpoint       TEXT             NOT NULL,
  bucket         TIMESTAMPTZ      NOT NULL,   -- start of the minute
  request_count  BIGINT           NOT NULL,
  error_count    BIGINT           NOT NULL,
  sum_latency_ms BIGINT           NOT NULL,
  min_latency_ms INTEGER          NOT NULL,
  max_latency_ms INTEGER          NOT NULL,
  p50_latency_ms DOUBLE PRECISION NOT NULL,
  p95_latency_ms DOUBLE PRECISION NOT NULL,
  p99_latency_ms DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (tenant_id, bucket, endpoint)
);

CREATE TABLE IF NOT EXISTS rollup_1h (
  tenant_id      UUID             NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  endpoint       TEXT             NOT NULL,
  bucket         TIMESTAMPTZ      NOT NULL,   -- start of the hour
  request_count  BIGINT           NOT NULL,
  error_count    BIGINT           NOT NULL,
  sum_latency_ms BIGINT           NOT NULL,
  min_latency_ms INTEGER          NOT NULL,
  max_latency_ms INTEGER          NOT NULL,
  p50_latency_ms DOUBLE PRECISION NOT NULL,
  p95_latency_ms DOUBLE PRECISION NOT NULL,
  p99_latency_ms DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (tenant_id, bucket, endpoint)
);

COMMENT ON TABLE rollup_1m IS 'Complete 1-minute buckets of events per tenant+endpoint, maintained incrementally by the rollup worker.';
COMMENT ON TABLE rollup_1h IS 'Complete 1-hour buckets of events per tenant+endpoint, with exact per-bucket percentiles.';

-- Defence in depth, same policy as `events`: a tenant-scoped transaction can
-- only see its own rows. The rollup worker writes across tenants, so its
-- database role needs BYPASSRLS (or to be a superuser, as in docker-compose).
ALTER TABLE rollup_1m ENABLE ROW LEVEL SECURITY;
ALTER TABLE rollup_1m FORCE ROW LEVEL SECURITY;
ALTER TABLE rollup_1h ENABLE ROW LEVEL SECURITY;
ALTER TABLE rollup_1h FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON rollup_1m;
CREATE POLICY tenant_isolation ON rollup_1m
  USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON rollup_1h;
CREATE POLICY tenant_isolation ON rollup_1h
  USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

-- ---------------------------------------------------------------------------
-- Watermark cursors. NULL = never ran; the worker starts at the first bucket
-- that contains an event.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollup_watermarks (
  name       TEXT PRIMARY KEY CHECK (name IN ('rollup_1m', 'rollup_1h')),
  watermark  TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO rollup_watermarks (name) VALUES ('rollup_1m'), ('rollup_1h')
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Retire the hourly_stats materialized view and its full-rebuild cron job.
-- It is derived data; the rollups above replace it for every query it served.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'refresh-hourly-stats') THEN
    PERFORM cron.unschedule('refresh-hourly-stats');
  END IF;
END
$$;

DROP MATERIALIZED VIEW IF EXISTS hourly_stats;
