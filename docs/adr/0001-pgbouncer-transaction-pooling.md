# 0001. PgBouncer in transaction pooling mode

Status: Accepted

## Context

Every Postgres backend costs memory and scheduler time, and `max_connections`
is 100. The API runs many short transactions (an ingest insert, an analytics
aggregate), and several app instances may each open a pool of 20.

## Decision

Put PgBouncer in front of the primary with `pool_mode = transaction`
(`default_pool_size = 20`, `max_client_conn = 100`, `docker/pgbouncer/pgbouncer.ini`).
The app's write pool (`PRIMARY_DATABASE_URL`) goes through it. Migrations and
other DDL use a direct session (`ADMIN_DATABASE_URL`).

## Consequences

- A server connection is held only for one transaction, so a small backend
  pool serves many clients.
- No session state may outlive a transaction: no named prepared statements
  (`max_prepared_statements = 0`), no `LISTEN/NOTIFY`, no session advisory
  locks, no plain `SET`. Tenant scoping therefore uses `SET LOCAL` (ADR 0002).
- `CREATE INDEX CONCURRENTLY`, multi-statement DDL and pg_cron scheduling must
  run on the direct connection (`src/db/migrate.ts`).
- Pool exhaustion surfaces as `PoolExhaustedError` and HTTP 503, not a hang.
