-- ===========================================================================
-- 003_create_indexes.sql
-- Indexes on the partitioned `events` table. Created on the parent so every
-- partition (current + future) inherits them automatically.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- BRIN on ingested_at.
-- Append-only ingest => rows arrive in ingested_at order, so block ranges
-- correlate tightly with time. BRIN is tiny (KBs vs GBs for btree) and ideal
-- for wide time-range scans like the timeseries endpoint.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_events_ingested_brin
  ON events USING BRIN (ingested_at);

-- ---------------------------------------------------------------------------
-- Partial index for error tracking.
-- The /analytics/errors endpoint only ever looks at status_code >= 400, which
-- is a small fraction of rows. A partial index stays small and is highly
-- selective for the error dashboard.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_events_errors
  ON events (tenant_id, ingested_at)
  WHERE status_code >= 400;

-- ---------------------------------------------------------------------------
-- Covering index for the primary dashboard query.
-- (tenant_id, ingested_at DESC) matches the WHERE + ORDER BY of overview /
-- endpoint queries; INCLUDE columns let those queries be satisfied
-- index-only (no heap fetch) for the hottest path.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_events_dashboard
  ON events (tenant_id, ingested_at DESC)
  INCLUDE (endpoint, status_code, latency_ms);

-- ---------------------------------------------------------------------------
-- GIN on metadata for JSONB containment queries (metadata @> '{"k":"v"}').
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_events_metadata
  ON events USING GIN (metadata);
