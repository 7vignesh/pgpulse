# 0010. Lag-aware read routing and watermark rollups

Status: Accepted. Supersedes [0006](0006-streaming-replica-for-analytics.md).

## Context

ADR 0006 sent every analytics read to the replica without checking it, so a
lagging replica served stale data and a broken one failed requests. Long
dashboard ranges scanned raw events or the `hourly_stats` materialized view,
which was rebuilt from all history every hour.

## Decision

- Read router (`src/db/router.ts`): a background probe measures replica lag
  every second. Analytics reads use the replica while lag is at most
  `REPLICA_MAX_LAG_MS` (2000) and the primary otherwise. Failover is immediate;
  returning needs 3 good probes in a row. A read that fails on the replica with
  a connection-level error is re-run on the primary.
- Rollups (migration `009`): `rollup_1m` and `rollup_1h` keep count, errors,
  sum, min, max and p50/p95/p99 per tenant, endpoint and bucket. The worker
  (`src/rollup/worker.ts`) aggregates only `ingested_at` from the stored
  watermark to a new aligned watermark and advances it in the same
  transaction. The watermark row is locked with `FOR UPDATE SKIP LOCKED`.
- Ranges over 1 h are split into whole hours, whole minutes and raw edges
  (`src/rollup/plan.ts`). `hourly_stats` and its refresh job are dropped.

## Consequences

- A replica read can be up to the lag limit plus one probe interval stale.
  When the replica is out, the primary takes all analytics load.
- A rollup that is behind makes queries slower, never wrong. Events that
  become visible with an `ingested_at` behind the watermark are not rolled up.
- Counts, sums, min and max combine exactly. A tenant-wide hourly percentile is
  a request-weighted average of per-endpoint percentiles, so it is approximate.
- The rollup worker writes across tenants, so its role needs BYPASSRLS.
