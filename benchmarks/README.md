# Benchmarks

Two kinds of measurement, both reproducible from this repo.

| folder | what | how |
|--------|------|-----|
| [`explain-analyze/`](explain-analyze/) | `EXPLAIN (ANALYZE, BUFFERS)` plans for the analytics queries, with and without indexes | `npm run seed` then `npm run benchmark` |
| [`k6/`](k6/) | HTTP load test, 50 concurrent tenants | `npm run bench:k6` |

## k6 load test

`k6/load.js` creates `TENANTS` (default 50) fresh tenants through the API in
`setup()`, gives each 500 events of history, then runs one virtual user per
tenant. Each VU loops with a `THINK_S` (default 0.2 s) pause, so the offered load
is about 250 req/s at 50 tenants.

Scenarios (`SCENARIO`):

- `ingest`: half single events (`POST /v1/events`), half 50-event batches.
- `analytics`: overview, endpoints, timeseries, errors, per-endpoint latency.
- `mixed` (default): 70 % ingest, 30 % analytics.

Thresholds (the run fails if any is missed): error rate < 1 %, ingest p95 <
250 ms, analytics p95 < 500 ms.

```bash
docker compose up -d --build                        # app on :3000
npm run bench:k6                                    # mixed, 50 tenants, 2 min
scripts/k6.sh ingest                                # one scenario
TENANTS=100 DURATION=5m THINK_S=0 scripts/k6.sh     # heavier / saturation
```

`scripts/k6.sh` runs the `grafana/k6` image, so k6 need not be installed. The
summary JSON goes to `k6/results/<scenario>.json` (git-ignored). Note that each
run creates new tenants and inserts events, so run it against a disposable
database.

## Results

Measured 2026-11-23 on a laptop: Intel Core i5-1135G7 (4 cores / 8 threads),
16 GB RAM, Windows 11 with Docker Desktop (WSL2 VM: 4 CPUs, 8 GB). The whole
stack (primary, replica, PgBouncer, app, k6) shared that VM. Database
pre-seeded with 1,000,000 events (`npm run seed`); about 2.6 M after the runs.
50 tenants, 2 minutes per scenario, default settings (`PG_POOL_MAX=20`,
PgBouncer `default_pool_size=20`), migrations 001-006 + 011.

| scenario | throughput | failed | ingest p50 / p95 / p99 | analytics p50 / p95 / p99 | thresholds |
|----------|-----------:|-------:|-----------------------:|--------------------------:|:----------:|
| ingest    | 207 req/s | 0 % | 21.4 / 116.2 / 249.1 ms | - | pass |
| analytics | 234 req/s | 0 % | - | 7.9 / 25.4 / 44.6 ms | pass |
| mixed     | 213 req/s | 0 % | 20.7 / 76.5 / 366.0 ms | 13.5 / 44.6 / 86.5 ms | pass |
| mixed, `THINK_S=0`, 1 min | 410 req/s | 0 % | 96.4 / 309.8 / 457.8 ms | 67.0 / 166.8 / 282.6 ms | ingest p95 **fail** |

Reading the numbers:

- With think time the offered load (~250 req/s) is below capacity, so these
  are latencies at moderate load, not a maximum.
- With no think time the stack reaches about 410 req/s before ingest p95 goes
  past 250 ms. Ingest is the bottleneck: each event or batch is its own
  transaction through PgBouncer. Batch ingest (50 events per request) means the
  event rate is much higher than the request rate.
- Laptop numbers with everything in one VM. Treat them as a baseline to compare
  changes against on the same machine, not as production capacity.
- The mixed p99 (366 ms) is above its p95 by a wide margin: occasional stalls,
  likely checkpoints and autovacuum on the 2.6 M-row table sharing the VM.
