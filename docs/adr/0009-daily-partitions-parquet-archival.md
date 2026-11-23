# 0009. Daily partitions, lock-free detach, Parquet archival

Status: Accepted. Supersedes the partition maintenance and retention parts of
[0003](0003-range-partitioning-by-ingested-at.md).

## Context

ADR 0003 created monthly partitions with a `DEFAULT` catch-all and left
retention manual, so storage grew forever. A plain `DETACH PARTITION` takes
ACCESS EXCLUSIVE on `events` and stalls all ingest behind it.

## Decision

- Daily partitions, `events_YYYYMMDD`, for today .. today + 7, created by
  `ensure_events_daily_partitions()` (migration `010`) with `ATTACH PARTITION`
  and a short `lock_timeout`. It runs hourly from pg_cron and from the
  partition manager. Days already covered by a monthly partition are skipped.
- The partition manager (`src/partitions/manager.ts`), one instance at a time
  through `pg_try_advisory_lock` on a direct session: for each partition past
  `PARTITION_RETENTION_DAYS`, `DETACH ... CONCURRENTLY`, export to GZIP Parquet,
  upload to S3 or MinIO, verify (object size and Parquet footer row count equal
  to `count(*)`), and only then `DROP TABLE`. Progress lives in
  `partition_archive_log`, so every step resumes after a failure.
- The `DEFAULT` partition is removed (its rows moved into daily partitions),
  because Postgres refuses `DETACH ... CONCURRENTLY` while one exists.

## Consequences

- Inserts keep flowing while a detach waits on readers (measured: a plain
  DETACH blocked every insert for 1.5 s; CONCURRENTLY did not).
- With no DEFAULT partition, an `ingested_at` outside the provisioned window
  cannot be stored, so the API rejects it with 400 and the seed script
  backfills partitions first.
- Nothing is dropped unless an object store is configured (`S3_BUCKET`).
- Archives are cold storage, not queryable through the API. Rollups (ADR 0010)
  are kept independently, so long-range dashboards survive raw-row archival.
