# 0004. Index strategy: composite, BRIN, partial, evidence-driven additions

Status: Accepted

## Context

Analytics queries filter by tenant and a time range, sometimes by endpoint or
by error status. Every index slows every insert, so each one has to earn its
place with a measured plan.

## Decision

Indexes declared on the partitioned parent (inherited by every partition):

- `idx_events_dashboard (tenant_id, ingested_at DESC) INCLUDE (endpoint,
  status_code, latency_ms)`: the composite tenant + time index. Overview,
  endpoint breakdown and timeseries run as Index Only Scans on it.
- `idx_events_ingested_brin (ingested_at)`, default `pages_per_range = 128`:
  cheap cross-tenant time scans on append-ordered data.
- `idx_events_errors (tenant_id, ingested_at) WHERE status_code >= 400`: the
  error breakdown.
- `idx_events_metadata` GIN: JSONB containment.
- `idx_events_tenant_endpoint_time (tenant_id, endpoint, ingested_at DESC)
  INCLUDE (latency_ms, status_code)` (migration `011`): the per-endpoint
  latency query.

## Evidence (1,000,000 seeded events, median of 7 runs)

- Per-endpoint p50/p95/p99 by hour over 30 days: 50.98 ms / 23,769 buffers
  with only the dashboard index (it reads the tenant's whole range and
  discards other endpoints), 24.09 ms / 4,658 buffers with the new composite.
  Plans: `benchmarks/explain-analyze/latency_per_endpoint_30d_*.txt`.
- BRIN on append-ordered rows (correlation 1.0): a 6-hour range dropped from
  6,272 to 137 buffers (33.6 ms to 1.0 ms). `pages_per_range` 32 and 8 gave the
  same result with a larger index, so the default stays.

## Consequences

- One more btree to maintain on every insert, about 40 MB per 650k rows.
- BRIN is only as good as the physical order. The seed script inserts rows in
  random time order (correlation near 0), so BRIN does nothing on seeded data;
  real append-only ingest keeps correlation near 1.
- Indexes built in a migration run inside a transaction, so they cannot use
  `CONCURRENTLY`. On a large live table, build per partition concurrently and
  attach before running the migration.
