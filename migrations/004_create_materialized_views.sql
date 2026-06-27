-- ===========================================================================
-- 004_create_materialized_views.sql
-- Hourly per-endpoint rollups. Analytics queries for data older than ~1h read
-- from here; recent data is computed live from `events`.
-- ===========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_stats AS
SELECT
  tenant_id,
  date_trunc('hour', ingested_at)                                  AS hour,
  endpoint,
  COUNT(*)                                                         AS total_requests,
  COUNT(*) FILTER (WHERE status_code >= 400)                       AS error_count,
  ROUND(AVG(latency_ms))                                          AS avg_latency_ms,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)        AS p50_latency_ms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)        AS p95_latency_ms,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)        AS p99_latency_ms
FROM events
GROUP BY tenant_id, date_trunc('hour', ingested_at), endpoint
WITH DATA;

-- REFRESH MATERIALIZED VIEW CONCURRENTLY requires a UNIQUE index.
CREATE UNIQUE INDEX IF NOT EXISTS uq_hourly_stats
  ON hourly_stats (tenant_id, hour, endpoint);

-- Supports the common "latest first" scan per tenant.
CREATE INDEX IF NOT EXISTS idx_hourly_stats_tenant_hour
  ON hourly_stats (tenant_id, hour DESC);

COMMENT ON MATERIALIZED VIEW hourly_stats IS
  'Hourly per-endpoint rollups, refreshed CONCURRENTLY each hour via pg_cron.';
