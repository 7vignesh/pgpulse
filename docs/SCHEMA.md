# SCHEMA.md

Schema design for PgPulse. See [`../schema/ERD.md`](../schema/ERD.md) for the
diagram and [`../benchmarks/explain-analyze/`](../benchmarks/explain-analyze/)
for query plans.

---

## Tables

### `tenants`
One row per customer. The `api_key` (a 64-char hex of 32 random bytes) is the
sole credential for ingest + analytics. The `UNIQUE` constraint on `api_key`
doubles as the lookup index used on every authenticated request. `plan` is a
checked enum-like text column (`free | pro | enterprise`).

### `events`
Append-only fact table — one row per observed API request. RANGE-partitioned
by month on `ingested_at`.

The primary key is the composite `(id, ingested_at)`. Postgres requires the
partition key to be part of every unique/primary key on a partitioned table,
so a bare `id` PK is not possible; the composite is the standard pattern.

---

## Partitioning strategy

### Why RANGE on `ingested_at`

- **Query shape match.** Every analytics endpoint is time-bounded
  (`?from=&to=`). With monthly RANGE partitions the planner prunes to only the
  partitions overlapping the window. Plans show `Subplans Removed: N` — the
  single largest scan reduction, independent of any index.
- **Append-only locality.** New ingest always lands in the current month's
  partition, keeping hot data physically clustered. That clustering is exactly
  what makes the BRIN index on `ingested_at` effective.
- **Cheap retention.** Dropping or detaching an old month is an `O(1)` catalog
  operation — no giant `DELETE`, no bloat, no vacuum storm.

### Partition provisioning

- Migration `002` creates the previous, current, and next two months plus a
  `DEFAULT` partition (so ingest never fails on a missing range).
- `create_events_partition(date)` is an idempotent helper.
- A daily `pg_cron` job (`create-next-partition`) pre-creates next month's
  partition so the `DEFAULT` partition stays empty. See SCALING.md for the
  `pg_partman` upgrade path.

---

## Indexes — what each one serves

All four are declared on the parent and inherited by every partition.

### `idx_events_ingested_brin` — BRIN on `ingested_at`
Block-Range INdex. Because ingest is append-only, physical block order
correlates with time, so BRIN stores just min/max per block range — kilobytes
vs. the gigabytes a btree would need. Ideal for wide time-range scans like the
timeseries endpoint where btree precision is wasted.

### `idx_events_errors` — partial `(tenant_id, ingested_at) WHERE status_code >= 400`
The `/analytics/errors` endpoint only ever touches error rows, a small fraction
of traffic. A partial index indexes *only* those rows, staying small and highly
selective. Plan `02` shows a Bitmap Index Scan resolving a 1-day error
breakdown in ~1.6 ms.

### `idx_events_dashboard` — covering `(tenant_id, ingested_at DESC) INCLUDE (endpoint, status_code, latency_ms)`
The hottest dashboard query: latest rows for a tenant, newest first. The
key order matches `WHERE tenant_id=? ... ORDER BY ingested_at DESC`, and the
`INCLUDE` columns let it run as an **Index-Only Scan** — no heap access.
Measured ~24x faster and ~42x fewer buffers than the seq-scan baseline
(see `benchmarks/explain-analyze/`).

### `idx_events_metadata` — GIN on `metadata`
Supports JSONB containment queries (`metadata @> '{"region":"us-east-1"}'`)
for ad-hoc segmentation without schema changes.

---

## Rollups: `rollup_1m` and `rollup_1h`

Per `(tenant_id, endpoint, bucket)` aggregates of `events`, one table per
resolution (1 minute, 1 hour). Columns: `request_count`, `error_count`,
`sum_latency_ms`, `min_latency_ms`, `max_latency_ms`, `p50/p95/p99_latency_ms`.
Primary key `(tenant_id, bucket, endpoint)`. RLS is enabled and forced with the
same policy as `events`. They replace the old `hourly_stats` materialized view,
which was rebuilt from all history every hour.

### Maintenance: watermark cursors

`rollup_watermarks (name, watermark)` holds one cursor per table. Each worker
pass aggregates only `ingested_at >= watermark AND ingested_at < new_watermark`,
upserts the buckets, and advances the cursor in the same transaction. Cursors
are aligned to the bucket size, so a rollup row always describes a complete
bucket and re-running a range is idempotent. The cursor row is taken with
`FOR UPDATE SKIP LOCKED`, so several app instances can run the worker.
Events that become visible with an `ingested_at` before the cursor (old
client-supplied timestamps, or commits later than
`ROLLUP_SAFETY_LAG_SECONDS`) are not picked up.

### Read routing (rollup vs raw)

Ranges of one hour or less read raw `events`. Longer ranges are split by
`planSegments()` (`src/rollup/plan.ts`) into whole hours from `rollup_1h`,
whole minutes from `rollup_1m`, and raw edges, based on the watermarks read in
the same transaction. A rollup that is behind leaves the gap to a finer source,
so it makes queries slower but never wrong. Counts, sums, min and max combine
exactly. Percentiles do not: per-endpoint hourly percentiles are exact, while a
tenant-wide hourly percentile is a request-weighted average of the endpoints'
percentiles (an approximation), and the 1-minute table is never used for
percentiles. `/v1/analytics/errors` always reads raw events because the rollups
do not keep status codes.

---

## Row-Level Security

`events` has RLS `ENABLE`d and `FORCE`d (so even the table owner is subject to
it). The policy:

```sql
USING      (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true)::uuid)
```

The application opens a transaction and issues
`SET LOCAL app.current_tenant_id = '<uuid>'` before any tenant query (see
`src/db/pool.ts → withTransaction`). `current_setting(..., true)` returns NULL
when unset, so an un-scoped session matches no rows — **fail closed**. This is
defence in depth: queries also carry explicit `WHERE tenant_id = $1`.
