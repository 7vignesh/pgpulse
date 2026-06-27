-- ===========================================================================
-- 001_create_tenants.sql
-- Extensions + tenants table.
-- ===========================================================================

-- pgcrypto provides gen_random_uuid() and gen_random_bytes().
-- (Postgres 16 also has gen_random_uuid() in core, but pgcrypto is needed
--  for gen_random_bytes used to mint API keys.)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- pg_stat_statements: surfaced via /health for slow-query observability.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- pg_cron: scheduled materialized-view refresh + partition maintenance.
-- Installed into the database named in cron.database_name (postgresql.conf).
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- ---------------------------------------------------------------------------
-- Tenants
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenants (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name       TEXT NOT NULL,
  api_key    TEXT UNIQUE NOT NULL DEFAULT encode(gen_random_bytes(32), 'hex'),
  plan       TEXT NOT NULL DEFAULT 'free' CHECK (plan IN ('free', 'pro', 'enterprise')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- API-key lookups happen on every authenticated request. The UNIQUE
-- constraint already creates a btree index on api_key, so no extra index
-- is required here.

COMMENT ON TABLE tenants IS 'One row per customer. api_key authenticates ingest + analytics requests.';
