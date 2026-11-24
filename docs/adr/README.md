# Architecture Decision Records

Short records of the decisions that shape PgPulse: what was decided, why, and
what it costs. Format: context, decision, consequences (MADR-style, kept brief).

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-pgbouncer-transaction-pooling.md) | PgBouncer in transaction pooling mode | Accepted |
| [0002](0002-rls-tenant-isolation-set-local.md) | Tenant isolation with RLS + `SET LOCAL` | Accepted |
| [0003](0003-range-partitioning-by-ingested-at.md) | RANGE-partition `events` on `ingested_at` | Accepted (maintenance superseded by 0009) |
| [0004](0004-index-strategy.md) | Index strategy: composite, BRIN, partial, evidence-driven additions | Accepted |
| [0005](0005-alerting-in-postgres-pg-cron.md) | Alert evaluation in Postgres via pg_cron | Superseded by 0011 |
| [0006](0006-streaming-replica-for-analytics.md) | Streaming replica for analytics reads | Superseded by 0010 |
| [0007](0007-operator-diagnostics-endpoint.md) | Separate, token-gated `/internal/db-health` | Accepted |
| [0008](0008-buffered-copy-ingest.md) | Buffered ingest with binary COPY | Accepted |
| [0009](0009-daily-partitions-parquet-archival.md) | Daily partitions, lock-free detach, Parquet archival | Accepted |
| [0010](0010-lag-aware-routing-and-rollups.md) | Lag-aware read routing and watermark rollups | Accepted |
| [0011](0011-distributed-alerting-outbox.md) | Distributed alerting: SKIP LOCKED leases, hysteresis, outbox | Accepted |

New decision: copy the shape of an existing record, take the next number, and
add it to the table. Do not edit an accepted record to reverse it; write a new
one that supersedes it.
