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

`CREATE INDEX [CONCURRENTLY]`, multi-statement transactional DDL, and `pg_cron` scheduling all need a stable
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
  service runs every query via `readTransaction()` (`src/db/router.ts`), which
  picks the pool per request: the replica while it is healthy, else the primary.
- If `REPLICA_DATABASE_URL` is unset, `replicaPool` falls back to the primary,
  so the app runs single-node without code changes.
- The replica is a physical streaming replica (`pg_basebackup --wal-method=
  stream`, `standby.signal`, `primary_conninfo`). `wal_level = replica`,
  `hot_standby = on` on the primary.

### Lag-aware routing (`src/db/router.ts`)

A background probe runs on the replica every `REPLICA_PROBE_INTERVAL_MS`
(default 1s) and computes lag as `now() - pg_last_xact_replay_timestamp()`. A
replica whose received LSN equals its replayed LSN counts as zero lag, so an
idle primary does not look like a lagging replica. It is unusable when it is not
in recovery, when `pg_stat_wal_receiver` shows no streaming connection, or when
the probe errors or exceeds `REPLICA_PROBE_TIMEOUT_MS`.

- Lag above `REPLICA_MAX_LAG_MS` (default 2000) fails over to the primary on the
  next probe, and a request sent to the replica that then fails with a
  connection-level error is re-run on the primary (reads are read-only, so this
  is safe) while a fresh probe is triggered.
- Returning to the replica needs `REPLICA_RECOVERY_PROBES` (default 3)
  consecutive good probes, so a replica hovering near the limit does not flap.
- `/health` shows `read_router`: current target, lag, last probe, reason,
  failover count.
- `pg_stat_wal_receiver` needs superuser or `pg_monitor`. Without it the router
  treats the replica as not streaming and uses the primary (fails safe).

**Trade-offs.** Failover can take up to one probe interval (about 1s) to notice,
so a read can still be served up to roughly the limit plus one interval stale.
When the replica is out, the primary absorbs all analytics load. Per-request lag
checks would be tighter but add a round trip to every query. For a
read-your-writes requirement, query the primary directly.

---

## Rollups

`rollup_1m` and `rollup_1h` replace the hourly materialized view; see
[`SCHEMA.md`](SCHEMA.md#rollups-rollup_1m-and-rollup_1h) for the tables, the
watermark cursor mechanism, and how queries over one hour are split across
rollups and raw events.

---

## Partition lifecycle

`events` is RANGE-partitioned on `ingested_at`. Migration `010` and the
partition manager (`src/partitions/manager.ts`) keep daily partitions ahead of
ingest, and move expired ones to cold storage.

```
every PARTITION_INTERVAL_MS (1h), one instance at a time (pg_try_advisory_lock):
  1. ensure_events_daily_partitions(PARTITION_AHEAD_DAYS=7)   today .. today+7
  2. for each partition that ended before now - PARTITION_RETENTION_DAYS (90):
       detaching  ALTER TABLE events DETACH PARTITION p CONCURRENTLY  (or FINALIZE)
       detached   export to GZIP Parquet -> upload to S3/MinIO
       verified   object exists, size matches upload, Parquet footer row count
                  == count(*) of the detached table
       dropped    DROP TABLE p      (only after the checks above passed)
```

State lives in `partition_archive_log`, so every step is resumable: if the
upload fails, the verification fails, or the process dies, the table stays
detached and the next run continues from where it stopped. A partition is only
dropped by the run that has just verified it.

### Why these choices

- **`DETACH ... CONCURRENTLY`.** A plain `DETACH` takes ACCESS EXCLUSIVE on
  `events` and queues behind any long reader, and then every insert queues
  behind it. The concurrent form waits only on the partition. The tests hold a
  reader on the old partition for 1.5s: inserts keep flowing with CONCURRENTLY
  and stop completely with a plain DETACH. It cannot run in a transaction block,
  so the manager uses a direct `ADMIN_DATABASE_URL` session (not PgBouncer, not
  the migration runner).
- **No DEFAULT partition.** Postgres refuses `DETACH ... CONCURRENTLY` while a
  default partition exists, so migration `010` retires `events_default` (moving
  any rows into daily partitions). The consequence: an insert with an
  `ingested_at` outside the pre-created window now fails. The API therefore
  rejects a client-supplied `ingested_at` older than the retention cutoff or
  beyond today + 7 days with a 400, and `seed.ts` backfills its 90 days first.
- **`ATTACH PARTITION` for creation.** Daily partitions are built as a plain table
  plus `ATTACH`, which takes a weaker lock on `events` than `CREATE TABLE ...
  PARTITION OF`, with a 5s `lock_timeout` so it gives up instead of queueing.
- **pg_cron safety net.** `ensure_events_daily_partitions(7)` also runs hourly
  from pg_cron, so ingest keeps working if the app is down for days. Only
  creation is in SQL; archival needs the app (S3 and Parquet).
- **Parquet.** Written with `hyparquet-writer` (pure JS, no native build), GZIP,
  one row group per 50k-row cursor chunk so memory stays bounded. Timestamps keep
  microsecond precision, `id` stays INT64, `tenant_id` is a UUID. Layout:
  `s3://<bucket>/<S3_PREFIX>/events/dt=YYYY-MM-DD/<partition>.parquet`, readable by
  DuckDB, Spark, Athena.
- **Monthly and daily partitions coexist.** Existing monthly partitions stay; the
  manager only creates a daily partition for a day no existing partition covers.
  A monthly partition is archived as one file once its whole month is past
  retention.

### Limitations

- Archival needs `S3_BUCKET`. Without it partitions are still created but nothing
  is detached or dropped (disk grows), and the manager says so in the log.
- The archive is not queryable through `/v1/analytics`; it is cold storage.
  Restoring means reading the Parquet back and attaching a table.
- A tenant deleted before its partition is archived is archived with its rows,
  and a restore must handle the missing `tenants` row.
- The export runs on the app host and writes a temp file the size of the
  compressed partition.
- Rollup tables are kept regardless of partition retention, so long-range
  dashboards stay available after raw rows are archived.

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
