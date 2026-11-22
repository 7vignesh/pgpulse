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

---

## Alert System

Tenants define alert rules (e.g. "error_rate > 5% over 10 minutes on
`/api/checkout`"). A rule moves between `OK` and `FIRING`, and each
transition is delivered once to the tenant's webhook through a transactional
outbox. Migration `006` created rules and events; migration `008` added
the state machine, leases, and `alert_outbox`.

### Evaluation: leases + SKIP LOCKED (`src/alerts/evaluator.ts`)

Any number of workers, in any number of app instances, can evaluate at once:

1. **Claim** (short transaction): `SELECT ... FROM alert_rules WHERE enabled AND
   next_eval_at <= now() AND (lease_until IS NULL OR lease_until < now())
   FOR UPDATE SKIP LOCKED`, then stamp `lease_owner` / `lease_until`. A locked
   row is skipped rather than waited on, and a leased rule is invisible to the
   other workers.
2. **Compute** the metric from raw `events` with no row lock held.
3. **Finalize** (one transaction): re-lock the rule and continue only if this
   worker still owns an unexpired lease. Update `alert_state`, and on a
   transition insert the `alert_events` row and the `alert_outbox` row, advance
   `next_eval_at`, and release the lease. A worker that stalled past its lease
   fails the ownership check and writes nothing, which is what prevents a
   double fire after a takeover. A worker that crashes simply lets its lease
   expire.

The claim and the finalize are separate transactions on purpose: a lease can
span a slow metric query without holding a row lock for its duration.

### Hysteresis (`src/alerts/state-machine.ts`)

`OK -> FIRING` after `required_consecutive_breaches` consecutive breaching
evaluations; `FIRING -> OK` (`RESOLVED`) after `recovery_evaluations`
consecutive clean ones. Both default to 1 per rule. A window with no events
counts as clean. This replaces the old cooldown (one fire per `window_minutes`):
a rule now announces each transition once, instead of re-announcing every window
while it stays in breach.

### Delivery: transactional outbox (`src/alerts/dispatcher.ts`)

```
evaluator (any worker)                 dispatcher (any worker)
  BEGIN                                  claim: SELECT ... FROM alert_outbox
  UPDATE alert_state                       WHERE status='pending' AND next_attempt_at<=now()
  INSERT alert_events                      FOR UPDATE SKIP LOCKED  (+ lease)
  INSERT alert_outbox   ---- commit --->  POST payload (5s timeout, https only)
  COMMIT                                   sent | retry(backoff+jitter) | dead
```

The outbox row commits atomically with the state change, so an alert can
neither be lost nor announced without its state change. Retries use exponential
backoff with equal jitter (delay uniform in [ceiling/2, ceiling), ceiling =
min(cap, base * 2^(attempt-1))). After `ALERT_MAX_ATTEMPTS` (default 8) the row
becomes `dead` and is never retried. The result is recorded only if the worker
still holds the row's lease.

Delivery is **at-least-once**. A worker that dies between the POST and recording
the result causes a re-delivery after its lease expires. Payloads carry
`alert_event_id` so receivers can deduplicate.

### Trade-offs and limitations

- **Evaluation now needs the app.** Before migration `008`, evaluation ran in
  Postgres via pg_cron and continued while every app instance was down. Now at
  least one app instance must be running. In exchange, evaluation scales across
  workers and the state machine is unit-tested. Running a second instance, or a
  dedicated worker process, removes the single point of failure.
- **Lease sizing.** `ALERT_LEASE_MS` (default 60s) must exceed the slowest metric
  query, and `ALERT_DISPATCH_LEASE_MS` must exceed the webhook timeout, or a
  healthy worker loses its lease and its work is repeated (safely, but wasted).
- **Dead letters are not replayed automatically.** Inspect with
  `SELECT * FROM alert_outbox WHERE status = 'dead'`.
- **https-only.** Webhook URLs must be `https://` (validated on rule creation and
  re-checked at delivery). Plain `http://` is rejected.
