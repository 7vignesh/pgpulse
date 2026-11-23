# 0003. RANGE-partition `events` on `ingested_at`

Status: Accepted; partition maintenance and retention superseded by [0009](0009-daily-partitions-parquet-archival.md)

## Context

`events` is append-only and every analytics query is bounded by a time range.
Retention means removing whole months of old data.

## Decision

Partition `events` by `RANGE (ingested_at)` into monthly children
(`migrations/002`), with a `DEFAULT` partition as a safety net and a daily
pg_cron job that pre-creates next month's partition. The primary key is
`(id, ingested_at)` because a partitioned table's unique keys must include the
partition key.

## Consequences

- The planner prunes partitions outside the query range (`Subplans Removed` in
  every plan under `benchmarks/explain-analyze/`).
- New rows land in the newest partition, which keeps it hot in cache and keeps
  `ingested_at` well correlated with physical order (good for BRIN, ADR 0004).
- Dropping a whole partition is a cheap retention operation; retention itself
  is not automated yet (`migrations/005` has a commented template).
- No uniqueness can be enforced on a column set that excludes `ingested_at`.
- The `DEFAULT` partition absorbs out-of-range rows, but its presence blocks
  `DETACH PARTITION ... CONCURRENTLY`.
