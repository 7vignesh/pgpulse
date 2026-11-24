-- ===========================================================================
-- 011_perf_indexes.sql
-- Index added on measured evidence (see benchmarks/explain-analyze/README.md,
-- "PR 5 index review", 1,000,000 seeded events, 3 tenants).
--
-- Reviewed and KEPT unchanged:
--   * idx_events_dashboard (tenant_id, ingested_at DESC) INCLUDE (...)
--     Already the composite (tenant_id, timestamp DESC) index. Every tenant
--     time-range query in src/query/service.ts runs as an Index Only Scan on
--     it (overview, endpoints, timeseries).
--   * idx_events_ingested_brin (ingested_at), default pages_per_range = 128.
--     On append-ordered data (correlation 1.0) it cuts a 6-hour cross-tenant
--     range from 6,272 to 137 buffers (33.6 ms -> 1.0 ms). Smaller
--     pages_per_range (32, 8) gave the same buffers and time with a larger
--     index, so the default stays.
--
-- ADDED: idx_events_tenant_endpoint_time
--   The latency endpoint with ?endpoint= filters on (tenant_id, endpoint,
--   ingested_at range). With only the dashboard index, Postgres reads the
--   tenant's whole time range and discards the other endpoints ("Rows Removed
--   by Filter: 67,996" for 30 days). Leading with endpoint makes the scan touch
--   only that endpoint's rows, and INCLUDE (latency_ms, status_code) keeps it
--   index-only. Median of 7 runs, per-endpoint p50/p95/p99 by hour:
--       24h:  2.85 ms /    822 buffers  ->  1.31 ms /   162 buffers
--        7d: 13.95 ms /  5,607 buffers  ->  6.37 ms / 1,062 buffers
--       30d: 50.98 ms / 23,769 buffers  -> 24.09 ms / 4,658 buffers
--   Cost: ~40 MB per 650k rows of index, and one more index to maintain on
--   every insert.
--
-- Built without CONCURRENTLY: the migration runner wraps each file in a
-- transaction, and CREATE INDEX CONCURRENTLY cannot run in one (nor on a
-- partitioned parent). On a large live table, build it per partition with
-- CREATE INDEX CONCURRENTLY and attach each one with ALTER INDEX ... ATTACH
-- PARTITION before running this migration.
-- ===========================================================================

CREATE INDEX IF NOT EXISTS idx_events_tenant_endpoint_time
  ON events (tenant_id, endpoint, ingested_at DESC)
  INCLUDE (latency_ms, status_code);
