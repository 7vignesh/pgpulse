-- ===========================================================================
-- 010_partition_lifecycle.sql
-- Automated partition lifecycle: daily partitions created ahead of time,
-- expired partitions detached WITHOUT blocking ingest, archived to Parquet on
-- S3/MinIO, verified, then dropped (src/partitions/manager.ts).
--
--   * ensure_events_daily_partitions(): creates today .. today + N daily
--     partitions. Used by the manager AND by a pg_cron job, so ingest keeps
--     working while the app is down (the monthly cron job it replaces had the
--     same property).
--   * partition_archive_log: one row per archived partition, the state that
--     makes every step of the archival pipeline resumable and auditable.
--   * The DEFAULT partition is retired. `DETACH PARTITION ... CONCURRENTLY` is
--     refused by Postgres while a default partition exists, and the manager
--     keeps real partitions ahead of ingest anyway.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Daily partition creation.
--
-- Built as CREATE TABLE ... (LIKE events) + ATTACH PARTITION rather than
-- CREATE TABLE ... PARTITION OF: attaching takes only SHARE UPDATE EXCLUSIVE
-- on the parent, so concurrent inserts and queries are not blocked, while
-- PARTITION OF takes ACCESS EXCLUSIVE. The new table is empty, so the
-- partition-constraint validation scan is free. A short lock_timeout makes the
-- call give up (and retry on the next run) instead of queueing behind a long
-- transaction and stalling everything queued behind it.
--
-- Days already covered by ANY existing partition (for example one of the
-- monthly partitions from migration 002) are skipped, so both layouts coexist.
-- p_back > 0 backfills past days (seed script, importing old data).
-- Returns the number of partitions created.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ensure_events_daily_partitions(p_ahead INTEGER DEFAULT 7, p_back INTEGER DEFAULT 0)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_today   TIMESTAMPTZ := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
  v_start   TIMESTAMPTZ;
  v_end     TIMESTAMPTZ;
  v_name    TEXT;
  v_created INTEGER := 0;
BEGIN
  IF p_ahead < 0 OR p_ahead > 366 OR p_back < 0 OR p_back > 3660 THEN
    RAISE EXCEPTION 'p_ahead must be 0..366 and p_back 0..3660, got % and %', p_ahead, p_back;
  END IF;

  PERFORM set_config('lock_timeout', '5s', true);

  FOR i IN -p_back..p_ahead LOOP
    v_start := v_today + make_interval(days => i);
    v_end   := v_start + INTERVAL '1 day';

    -- Covered (even partially) by an existing, attached partition?
    IF EXISTS (
      SELECT 1
        FROM pg_inherits inh
        JOIN pg_class c ON c.oid = inh.inhrelid
        CROSS JOIN LATERAL (
          SELECT regexp_match(
                   pg_get_expr(c.relpartbound, c.oid),
                   'FROM \(''([^'']+)''\) TO \(''([^'']+)''\)') AS m
        ) b
       WHERE inh.inhparent = 'events'::regclass
         AND NOT inh.inhdetachpending
         AND b.m IS NOT NULL
         AND b.m[1]::timestamptz < v_end
         AND b.m[2]::timestamptz > v_start
    ) THEN
      CONTINUE;
    END IF;

    v_name := format('events_%s', to_char(v_start AT TIME ZONE 'UTC', 'YYYYMMDD'));
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I (LIKE events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
    EXECUTE format(
      'ALTER TABLE events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
      v_name, v_start, v_end);
    v_created := v_created + 1;
  END LOOP;

  RETURN v_created;
END;
$$;

COMMENT ON FUNCTION ensure_events_daily_partitions(INTEGER, INTEGER) IS
  'Create daily events partitions for (today - p_back)..(today + p_ahead) (UTC) that no existing partition covers. Uses ATTACH PARTITION so it does not block ingest.';

-- ---------------------------------------------------------------------------
-- Archive log. status: detaching -> detached -> verified -> dropped. A row
-- whose status is not 'dropped' is resumed by the manager on its next run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partition_archive_log (
  partition_name TEXT PRIMARY KEY,
  range_from     TIMESTAMPTZ NOT NULL,
  range_to       TIMESTAMPTZ NOT NULL,
  status         TEXT NOT NULL DEFAULT 'detaching'
                 CHECK (status IN ('detaching', 'detached', 'verified', 'dropped')),
  row_count      BIGINT,
  s3_key         TEXT,
  bytes          BIGINT,
  sha256         TEXT,
  attempts       INTEGER NOT NULL DEFAULT 0,
  last_error     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  detached_at    TIMESTAMPTZ,
  verified_at    TIMESTAMPTZ,
  dropped_at     TIMESTAMPTZ
);

COMMENT ON TABLE partition_archive_log IS
  'Progress of detach -> Parquet export -> verify -> drop for each expired events partition.';

-- ---------------------------------------------------------------------------
-- Retire the DEFAULT partition. Rows that landed in it are moved into daily
-- partitions first, so nothing is lost.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  d RECORD;
  v_start TIMESTAMPTZ;
  v_name  TEXT;
BEGIN
  IF to_regclass('events_default') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE events DETACH PARTITION events_default;

  -- Fill each standalone child BEFORE attaching it. Inserting into the child
  -- directly (instead of through `events`) avoids the forced RLS policy on the
  -- parent, which would reject these rows for a non-superuser migration role.
  FOR d IN
    SELECT DISTINCT date_trunc('day', ingested_at AT TIME ZONE 'UTC') AS day
      FROM events_default
  LOOP
    v_start := d.day AT TIME ZONE 'UTC';
    v_name  := 'events_' || to_char(d.day, 'YYYYMMDD');
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I (LIKE events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
    EXECUTE format(
      'INSERT INTO %I SELECT * FROM events_default WHERE ingested_at >= %L AND ingested_at < %L',
      v_name, v_start, v_start + INTERVAL '1 day');
    EXECUTE format(
      'ALTER TABLE events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
      v_name, v_start, v_start + INTERVAL '1 day');
  END LOOP;

  DROP TABLE events_default;
END
$$;

-- Cover today..today+7 right away so ingest works immediately after this migration.
SELECT ensure_events_daily_partitions(7);

-- ---------------------------------------------------------------------------
-- pg_cron: replace the monthly pre-creation job (which would now try to create
-- a monthly partition overlapping the daily ones) with a daily safety net that
-- calls the same function the manager uses.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'create-next-partition') THEN
    PERFORM cron.unschedule('create-next-partition');
  END IF;
END
$$;

SELECT cron.schedule(
  'ensure-daily-partitions',
  '5 * * * *',
  'SELECT ensure_events_daily_partitions(7)'
);
