# 0007. Separate, token-gated `/internal/db-health`

Status: Accepted

## Context

`/health` is an unauthenticated probe for load balancers and orchestrators,
and should be cheap. Operators also need database diagnostics: cache hit
ratio, connections, replication lag, top slow queries. Those cost more to
compute and include normalized SQL text, which reveals schema and query shapes.

## Decision

Add `GET /internal/db-health` (`src/health/db-health.ts`), separate from
`/health`:

- Requires `Authorization: Bearer <INTERNAL_API_TOKEN>`, compared in constant
  time (both sides SHA-256 hashed, then `timingSafeEqual`).
- Answers 404 when `INTERNAL_API_TOKEN` is unset, so it is off by default.
- Sections: `cache_hit_ratio`, `connections`, `replication_lag`,
  `ingest_buffer`, `slow_queries` (top 10 from `pg_stat_statements` by mean
  time, text capped at 500 chars). Each section is computed independently with
  a 2 s timeout, so one failing source does not hide the others.
- `Cache-Control: no-store`.

## Consequences

- Probes stay cheap and diagnostics are not public.
- A single shared token is coarse: anyone holding it sees all diagnostics.
  Rotation means restarting with a new value. Put the path behind network
  controls as well in production.
- `pg_stat_statements` text is normalized (constants replaced by `$n`), so
  tenant data does not appear, but table and column names do.
- `ingest_buffer` reports the buffered-ingest depth, fill ratio and dropped
  count, or `available: false` when the pipeline is not running.
- `/health` still returns its existing `slow_queries` field; removing it would
  break current consumers, so that is left for a separate change.
