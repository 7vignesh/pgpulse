# 0006. Streaming replica for analytics reads

Status: Superseded by [0010](0010-lag-aware-routing-and-rollups.md)

## Context

Analytics aggregates are the heaviest queries and must not slow ingest on the
primary.

## Decision

Run a physical streaming replica (`postgres-replica` in `docker-compose.yml`,
`pg_basebackup --wal-method=stream`). The analytics service sends every query
to `replicaPool` (`REPLICA_DATABASE_URL`) in a read-only transaction. Writes and
API-key lookups use the primary, so a rotated key takes effect immediately.
With no replica configured, `replicaPool` falls back to the primary.

## Consequences

- Read load scales independently of write load.
- Replication is asynchronous, so dashboards can be slightly stale. `/health`
  and `/internal/db-health` report lag in bytes and seconds.
- The routing is static: a lagging or unreachable replica is not detected, so
  analytics requests serve stale data or fail instead of falling back.
