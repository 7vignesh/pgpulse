# PgPulse

A production-grade, multi-tenant API analytics platform built on PostgreSQL 16.
Ingest API request events per tenant, then query overview / endpoint /
latency / error / timeseries analytics — all tenant-isolated by Row-Level
Security and scaled with partitioning, a read replica, PgBouncer, and a
materialized view refreshed by pg_cron.

## What is this, in plain terms?

PgPulse answers one question for a backend team: **"how are my API endpoints
doing?"** — how much traffic, how many errors, how slow.

It works like a fitness tracker for your API. PgPulse can't see your traffic on
its own; your application **reports** each request to it ("`/checkout` returned
200 in 42ms"). PgPulse stores those records and lets you query analytics over
them — total requests, error rates, slowest endpoints, latency percentiles,
traffic over time.

It's the same idea as Datadog / New Relic / Stripe's API dashboards, scoped to
API request analytics, and built to serve **many separate customers (tenants)**
at once, each fully isolated from the others.

Key points to understand before using it:

- **It's a backend service (an API), not a tracking script.** Reporting happens
  on *your server*, not in a visitor's browser. That makes it accurate,
  private, and usable from any backend language.
- **A tenant = one customer/project.** Each tenant gets an `api_key` that both
  authenticates ingest and scopes every analytics query to that tenant's data.
- **Two kinds of traffic:** *writes* (your server reporting events) and *reads*
  (you querying analytics). Writes go to the primary database; reads are served
  from a read replica so heavy dashboards never slow down ingestion.
- **There is no built-in UI.** You interact over HTTP — from your own code, a
  script, or a dashboard you build on top. The `curl` examples below are just
  the simplest way to see it work.

## How would I use this on my own website / app?

Three steps. The only real integration work is step 2.

**1. Get a tenant + API key** (see [Try it](#try-it) below). Store the key as a
secret in your server's environment — never in browser code.

**2. Make your app report each request to PgPulse.** Add a small hook in your
backend that fires after every response. Example for an Express/Node app — the
idea is identical in any framework (Django, Rails, Go, etc.): on each request,
send one HTTP POST.

```javascript
// runs on every request to YOUR app
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    // fire-and-forget; never let analytics break your site
    fetch('http://localhost:3000/v1/events', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': process.env.PGPULSE_API_KEY, // your secret key
      },
      body: JSON.stringify({
        endpoint: req.path,             // "/checkout"
        method: req.method,             // "POST"
        status_code: res.statusCode,    // 200, 500, ...
        latency_ms: Date.now() - start, // how long it took
      }),
    }).catch(() => {});
  });
  next();
});
```

For high traffic, buffer events and send them in bulk to `/v1/events/batch`
(up to 1000 per request, inserted in one transaction) instead of one at a time.

**3. Query your analytics** whenever you want insight — directly, on a
schedule, or from a dashboard you build. See the analytics calls under
[Try it](#try-it). You'll learn things like "`/checkout` has a 12% error rate"
or "my p99 latency on `/search` is 2s."

> Note on browsers: the `x-api-key` is a server-side secret and must not be
> embedded in browser JavaScript (anyone could read it in DevTools). To build a
> human-facing dashboard, put a login/session layer in front and have *your
> server* call these endpoints — keep the key on the server. (Not included in
> this project; flagged intentionally.)

## Stack

- **API:** Node.js + Fastify + TypeScript (no Express, no ORM)
- **DB:** PostgreSQL 16 — RANGE-partitioned events, RLS, BRIN/partial/covering/
  GIN indexes, `hourly_stats` materialized view
- **Query layer:** `pg` (node-postgres), parameterized everywhere, no `SELECT *`
- **Pooling:** PgBouncer in transaction mode
- **Scheduling:** pg_cron (hourly MV refresh, daily partition pre-creation)
- **Observability:** pg_stat_statements + replica lag via `/health`
- **Tests:** Vitest (unit) + supertest (integration)

## Architecture

```
                   ┌──────────────┐  writes   ┌────────────┐   ┌──────────────┐
 client ──x-api-key──▶  Fastify   ├──────────▶│ PgBouncer  ├──▶│  Postgres    │
                   │   app :3000  │  (txn pool):6432        │   │  primary     │
                   │              │           └────────────┘   │  pg_cron +    │
                   │              │  reads                     │  pg_stat_stmts│
                   │              ├───────────────────────────▶│  (streaming) │
                   └──────────────┘  analytics                 └──────┬───────┘
                                       │                               │ WAL
                                       ▼                        ┌──────▼───────┐
                                   read replica  ◀──────────────│  replica     │
                                                                └──────────────┘
```

## Quick start

```bash
cp .env.example .env
docker compose up -d --build      # postgres, replica, pgbouncer, migrate, app
curl -s localhost:3000/health | jq
```

`migrate` runs once on startup and applies `migrations/*.sql` in order via a
direct (session-mode) connection. The app then serves on :3000.

<a id="try-it"></a>
### Try it

```bash
# create a tenant -> returns api_key (shown once)
KEY=$(curl -s -XPOST localhost:3000/v1/tenants \
  -H 'content-type: application/json' -d '{"name":"acme","plan":"pro"}' \
  | jq -r .api_key)

# ingest
curl -s -XPOST localhost:3000/v1/events -H "x-api-key: $KEY" \
  -H 'content-type: application/json' \
  -d '{"endpoint":"/api/login","method":"POST","status_code":200,"latency_ms":42}'

# batch (up to 1000, single transaction)
curl -s -XPOST localhost:3000/v1/events/batch -H "x-api-key: $KEY" \
  -H 'content-type: application/json' \
  -d '{"events":[{"endpoint":"/x","method":"GET","status_code":500,"latency_ms":9}]}'

# analytics (tenant-scoped, served from replica)
curl -s "localhost:3000/v1/analytics/overview"   -H "x-api-key: $KEY"
curl -s "localhost:3000/v1/analytics/endpoints?limit=5" -H "x-api-key: $KEY"
curl -s "localhost:3000/v1/analytics/errors"     -H "x-api-key: $KEY"
curl -s "localhost:3000/v1/analytics/timeseries?granularity=hour" -H "x-api-key: $KEY"
```

## API

| Method | Path | Auth | Notes |
|--------|------|------|-------|
| POST | `/v1/tenants` | admin* | create tenant, returns `api_key` |
| GET  | `/v1/tenants/:id` | admin* | tenant info (no key) |
| POST | `/v1/tenants/:id/rotate-key` | admin* | rotate `api_key` |
| POST | `/v1/events` | x-api-key | single event |
| POST | `/v1/events/batch` | x-api-key | up to 1000, one transaction |
| GET  | `/v1/analytics/overview` | x-api-key | totals, error rate, avg latency |
| GET  | `/v1/analytics/endpoints` | x-api-key | per-endpoint, sorted by count |
| GET  | `/v1/analytics/latency` | x-api-key | p50/p95/p99 over time |
| GET  | `/v1/analytics/errors` | x-api-key | breakdown by status + endpoint |
| GET  | `/v1/analytics/timeseries` | x-api-key | volume by hour/day |
| GET  | `/health` | none | pool stats, replica lag, slow queries |

Analytics endpoints take `?from=&to=` (ISO-8601, default last 24h).

\* The tenant admin routes are intentionally **unauthenticated in this
project** — in production they must sit behind an operator auth layer (admin
JWT/mTLS). This is flagged, not silently shipped.

## Development

```bash
npm install
npm run typecheck
npm test                 # unit tests; integration tests auto-skip without a DB

# run integration tests against the running stack
# (5432 may be taken by a host Postgres; the app uses PgBouncer :6432 anyway)
ADMIN_DATABASE_URL=postgres://pgpulse:pgpulse_secret@localhost:6432/pgpulse \
PRIMARY_DATABASE_URL=postgres://pgpulse:pgpulse_secret@localhost:6432/pgpulse \
REPLICA_DATABASE_URL=postgres://pgpulse:pgpulse_secret@localhost:5434/pgpulse \
npm test
```

Routes are thin (auth + validate + delegate); all logic lives in `*/service.ts`.

## Docs

- [`docs/SCHEMA.md`](docs/SCHEMA.md) — tables, partitioning, every index, MV strategy, RLS
- [`docs/SCALING.md`](docs/SCALING.md) — PgBouncer, replica routing, partition maintenance, pg_partman, sub-partitioning, EXPLAIN ANALYZE
- [`schema/ERD.md`](schema/ERD.md) — Mermaid ERD + partition layout
- [`benchmarks/explain-analyze/`](benchmarks/explain-analyze/) — captured query plans, before/after indexes

## Project layout

```
migrations/           sequential raw SQL (tenants, partitions, indexes, MV, cron)
src/db/               PgBouncer-aware pools + migration runner
src/ingest/           POST /v1/events[/batch]  (multi-row INSERT + COPY path)
src/query/            analytics services + MV-vs-raw routing (range.ts is pure)
src/tenants/          tenant CRUD + key rotation
src/middleware/       api-key auth, per-tenant rate limiting
src/health/           /health: pool stats, replica lag, pg_stat_statements
docker/               primary (pg_cron image + conf), replica, pgbouncer
```
