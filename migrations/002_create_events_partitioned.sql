-- ===========================================================================
-- 002_create_events_partitioned.sql
-- Events table, RANGE-partitioned by month on ingested_at.
--
-- Why RANGE on ingested_at:
--   * Analytics queries are time-bounded (?from=&to=). RANGE partitioning by
--     month lets the planner prune to only the partitions overlapping the
--     range, dramatically cutting scanned data.
--   * Append-only ingest means new data lands in the "current" partition,
--     keeping hot data physically clustered and the BRIN index tight.
--   * Old months can be DETACHed/DROPped in O(1) for cheap retention.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS events (
  id          BIGSERIAL,
  tenant_id   UUID NOT NULL REFERENCES tenants(id),
  endpoint    TEXT NOT NULL,
  method      TEXT NOT NULL CHECK (method IN ('GET','POST','PUT','PATCH','DELETE')),
  status_code SMALLINT NOT NULL,
  latency_ms  INTEGER NOT NULL,
  user_agent  TEXT,
  ip_address  INET,
  metadata    JSONB DEFAULT '{}',
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Primary key MUST include the partition key in a partitioned table.
  PRIMARY KEY (id, ingested_at)
) PARTITION BY RANGE (ingested_at);

COMMENT ON TABLE events IS 'API request events. RANGE-partitioned monthly on ingested_at.';

-- ---------------------------------------------------------------------------
-- Default partition: catches rows that fall outside any explicit partition
-- so ingest never fails on a missing partition. Maintenance job migrates /
-- creates proper partitions ahead of time (see 005).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

-- ---------------------------------------------------------------------------
-- Helper: create a monthly partition idempotently.
-- Usage: SELECT create_events_partition(date_trunc('month', now())::date);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_events_partition(p_month DATE)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_start DATE := date_trunc('month', p_month)::date;
  v_end   DATE := (date_trunc('month', p_month) + INTERVAL '1 month')::date;
  v_name  TEXT := format('events_%s', to_char(v_start, 'YYYY_MM'));
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class WHERE relname = v_name
  ) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
      v_name, v_start, v_end
    );
  END IF;
END;
$$;

COMMENT ON FUNCTION create_events_partition(DATE) IS
  'Idempotently create the monthly events partition containing p_month.';

-- ---------------------------------------------------------------------------
-- Provision partitions: previous, current, and next two months so ingest
-- works immediately regardless of clock and benchmarks have room.
-- ---------------------------------------------------------------------------
SELECT create_events_partition((date_trunc('month', now()) - INTERVAL '1 month')::date);
SELECT create_events_partition(date_trunc('month', now())::date);
SELECT create_events_partition((date_trunc('month', now()) + INTERVAL '1 month')::date);
SELECT create_events_partition((date_trunc('month', now()) + INTERVAL '2 month')::date);

-- ---------------------------------------------------------------------------
-- Row-Level Security: each tenant only sees its own rows.
-- The app issues `SET LOCAL app.current_tenant_id = $1` inside every tenant
-- transaction. Policies compare tenant_id against that GUC.
--
-- current_setting('app.current_tenant_id', true) returns NULL when unset,
-- which evaluates the policy to NULL (=> no rows), failing closed.
-- ---------------------------------------------------------------------------
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE events FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON events;
CREATE POLICY tenant_isolation ON events
  USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

-- Note: RLS policies are inherited by all current and future partitions
-- automatically, since policies attach to the partitioned parent.
