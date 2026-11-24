-- ===========================================================================
-- 007_create_ingest_keys.sql
-- Per-tenant idempotency keys for buffered ingest.
--
-- Why a side table and not a unique index on `events`:
--   `events` is partitioned by RANGE (ingested_at), so any unique index on it
--   must include ingested_at. A retry that is stamped with a different
--   timestamp would then not conflict. This table is keyed on
--   (tenant_id, idempotency_key) alone and is written in the same transaction
--   as the event insert, so a rolled-back flush releases its keys.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ingest_keys (
  tenant_id       UUID        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  idempotency_key TEXT        NOT NULL,
  claimed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key)
);

-- Supports the TTL sweep below.
CREATE INDEX IF NOT EXISTS idx_ingest_keys_claimed_at
  ON ingest_keys (claimed_at);

COMMENT ON TABLE ingest_keys IS
  'Claimed idempotency keys for ingest. Rows older than 7 days are swept daily, so the dedup window is 7 days.';

SELECT cron.schedule(
  'sweep-ingest-keys',
  '15 3 * * *',
  $$DELETE FROM ingest_keys WHERE claimed_at < now() - INTERVAL '7 days'$$
);
