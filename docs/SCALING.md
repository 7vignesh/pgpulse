# SCALING.md

How PgPulse scales reads, writes, and storage, and the operational tradeoffs
behind each choice.

---

## Connection pooling — PgBouncer in transaction mode

```
app (pg Pool, up to 100 client conns)
   │
   ▼
PgBouncer :6432  pool_mode = transaction   default_pool_size = 20
   │
   ▼
Postgres primary :5432  (max_connections = 100)
```

### Why transaction mode

In transaction pooling a server connection is returned to the pool at the
**end of each transaction**, so a small server-side pool (`default_pool_size =
20`) multiplexes a much larger set of client connections (`max_client_conn =
100`). This maximises backend reuse and protects Postgres from connection
storms — each Postgres backend costs memory and scheduler overhead, so keeping
the real backend count low is the goal.

### The tradeoff (and how the app respects it)

Transaction mode means **no session-level state survives between
transactions**. Specifically the app must avoid:

- **Server-side prepared statements.** A prepared statement is bound to one
  backend; PgBouncer may hand the next statement to a different backend.
  - *In code:* `pg` only uses named/server-prepared statements when you pass a
    `name`. We never do. `pgbouncer.ini` also sets `max_prepared_statements = 0`.
- **`LISTEN`/`NOTIFY`.** Notifications are session-scoped and would be lost.
  PgPulse does not use them.
- **Plain `SET` (session GUCs), advisory *session* locks, `WITH HOLD` cursors,
  temp tables across statements.**
  - *In code:* tenant scoping uses `SET LOCAL app.current_tenant_id` inside a
    transaction, so it is scoped to the transaction PgBouncer keeps pinned and
    is discarded on `COMMIT`. `server_reset_query = DISCARD ALL` is a backstop.

### Migrations and DDL bypass PgBouncer

`CREATE INDEX [CONCURRENTLY]`, `REFRESH MATERIALIZED VIEW CONCURRENTLY`,
multi-statement transactional DDL, and `pg_cron` scheduling all need a stable
session. The migration runner (`src/db/migrate.ts`) therefore connects on
`ADMIN_DATABASE_URL` — a **direct** session-mode connection to Postgres on
:5432, not through PgBouncer.

### Handling pool exhaustion

The app-side `pg.Pool` has `connectionTimeoutMillis`. When no client is
available in time, `pool.connect()` rejects; `withTransaction` wraps that in a
typed `PoolExhaustedError`, and the Fastify error handler maps it to **HTTP
503** instead of hanging or 500-ing. `/health` exposes live pool stats
(`total / idle / waiting`) so exhaustion is observable before it bites.

---

## Read replica routing

```
writes  (ingest)     → PgBouncer → primary
reads   (analytics)  → postgres-replica  (streaming replica, hot standby)
auth    (key lookup) → primary           (must see freshly rotated keys)
health  replica lag  → replica           (pg_last_wal_replay_timestamp)
```

- `src/db/pool.ts` exposes `primaryPool` and `replicaPool`. The analytics
  service runs every query via `withTransaction(replicaPool, …, {readOnly:true})`.
- If `REPLICA_DATABASE_URL` is unset, `replicaPool` falls back to the primary,
  so the app runs single-node without code changes.
- The replica is a physical streaming replica (`pg_basebackup --wal-method=
  stream`, `standby.signal`, `primary_conninfo`). `wal_level = replica`,
  `hot_standby = on` on the primary.

### Replication lag and read-your-writes

A streaming replica is asynchronous, so analytics reads can be slightly stale.
PgPulse tolerates this deliberately: analytics is an aggregate dashboard, not a
transactional read-after-write path, and the MV-vs-raw routing already treats
the most recent hour specially. `/health` reports lag in bytes and seconds
(`pg_wal_lsn_diff`, `pg_last_xact_replay_timestamp`) so lag is monitorable. For
any future read-your-writes requirement, route that specific query to the
primary.

---

## Partition maintenance

### Adding future partitions

- Built-in: `create_events_partition(date)` + the daily `create-next-partition`
  pg_cron job keep one month ahead of ingest.
- The `DEFAULT` partition is a safety net; if rows land there it means the
  maintenance job fell behind. Migrating them out:

  ```sql
  -- create the proper partition, then move misrouted rows
  SELECT create_events_partition('2026-09-01');
  WITH moved AS (
    DELETE FROM events_default
    WHERE ingested_at >= '2026-09-01' AND ingested_at < '2026-10-01'
    RETURNING *
  )
  INSERT INTO events SELECT * FROM moved;
  ```

### Production: pg_partman

For real deployments, replace the hand-rolled helper + cron job with
**pg_partman**, which automates rolling creation and retention:

```sql
CREATE EXTENSION pg_partman;
SELECT partman.create_parent(
  p_parent_table => 'public.events',
  p_control      => 'ingested_at',
  p_type         => 'native',
  p_interval     => '1 month',
  p_premake      => 3            -- keep 3 months provisioned ahead
);
-- retention: drop partitions older than 12 months
UPDATE partman.part_config
   SET retention = '12 months', retention_keep_table = false
 WHERE parent_table = 'public.events';
-- run partman.run_maintenance_proc() from pg_cron
```

### Retention

Migration `005` includes a commented `drop-old-partitions` cron template.
Dropping a monthly partition is `O(1)` and reclaims storage instantly — the
core operational payoff of RANGE partitioning.

### When a partition gets too large (sub-partitioning)

If a single month grows beyond what one partition handles well (very large
tenants, or month-long retention with huge volume), sub-partition the monthly
child by `tenant_id` (HASH) or by a finer time grain:

```sql
-- monthly parent partitioned again by HASH(tenant_id)
CREATE TABLE events_2026_09 PARTITION OF events
  FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')
  PARTITION BY HASH (tenant_id);

CREATE TABLE events_2026_09_h0 PARTITION OF events_2026_09
  FOR VALUES WITH (MODULUS 4, REMAINDER 0);
-- ... h1..h3
```

This keeps each leaf partition small, spreads a hot tenant's writes across
sub-partitions, and preserves time pruning at the top level. Alternatively
drop to weekly/daily partitions for the high-volume months only.

---

## pg_stat_statements

Enabled via `shared_preload_libraries`. `/health` surfaces the top 10 slowest
statements by mean execution time:

```sql
SELECT query, calls,
       ROUND(mean_exec_time::numeric, 2)  AS mean_exec_time,
       ROUND(total_exec_time::numeric, 2) AS total_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
```

The handler degrades gracefully if the extension is unavailable.

---

## EXPLAIN ANALYZE — before / after indexes

Full plans in [`../benchmarks/explain-analyze/`](../benchmarks/explain-analyze/).
Hottest dashboard query (`tenant + last 2h, newest first, LIMIT 100`) on ~50k
rows in the hot partition:

| metric | seq scan (no index) | index-only scan (covering) | delta |
|--------|--------------------:|---------------------------:|------:|
| Execution time | 6.024 ms | 0.246 ms | ~24x faster |
| Shared buffers | 631 | 15 | ~42x fewer |
| Rows removed by filter | 49,834 | 0 | scan avoided |

Every plan also shows `Subplans Removed: N` from partition pruning — the
time-range predicate eliminates non-matching monthly partitions at plan time
before any index is consulted.
