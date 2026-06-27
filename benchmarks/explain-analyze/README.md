# EXPLAIN ANALYZE benchmarks

Captured against a PgPulse stack seeded with ~50k events for one tenant
(`tenant='acme'`), spread over ~20 days, in the current monthly partition.
All queries set `app.current_tenant_id` first so RLS + tenant filters apply.

Reproduce:

```bash
docker compose up -d
# seed (see commands in this folder's git history / SCHEMA.md)
TID=$(docker compose exec -T postgres psql -U pgpulse -d pgpulse -tAc \
  "SELECT id FROM tenants WHERE name='acme' LIMIT 1")
docker compose exec -T postgres psql -U pgpulse -d pgpulse \
  -c "SET app.current_tenant_id='$TID'; EXPLAIN (ANALYZE, BUFFERS) <query>;"
```

## Files

| file | query | takeaway |
|------|-------|----------|
| `01_overview_with_indexes.txt` | 7-day overview aggregate | partition pruning removes 4 of 5 partitions (`Subplans Removed: 4`); wide window => seq scan of the one hot partition is optimal |
| `02_errors_partial_index.txt`  | 1-day error breakdown | Bitmap Index Scan on `(tenant_id, ingested_at)`; partial error index keeps candidate set tiny — 1.6 ms |
| `03_dashboard_covering_index.txt` | recent 100 rows, ordered | **Index-Only Scan** on the covering index — `Heap Fetches` served from the index; **0.25 ms** |
| `04_dashboard_NO_index_seqscan.txt` | same as 03, indexes disabled | forced Seq Scan baseline — **6.0 ms**, 631 buffers |

## Before / after — the recent-dashboard query

The hottest dashboard query (`WHERE tenant_id=? AND ingested_at >= now()-2h
ORDER BY ingested_at DESC LIMIT 100`) is the clearest win:

| metric | no index (seq scan) | covering index (index-only) | improvement |
|--------|--------------------:|----------------------------:|------------:|
| Execution time | 6.024 ms | 0.246 ms | **~24x faster** |
| Shared buffers hit | 631 | 15 | **~42x fewer** |
| Rows removed by filter | 49,834 | 0 | scan avoided |

The covering index `idx_events_dashboard (tenant_id, ingested_at DESC)
INCLUDE (endpoint, status_code, latency_ms)` lets Postgres satisfy the query
entirely from the index (`Index Only Scan`, `Heap Fetches` against the visible
index) without touching the heap.

## Partition pruning

Every plan shows `Subplans Removed: N` — the planner prunes partitions that
cannot match the `ingested_at` predicate at plan time. With monthly partitions
and time-bounded analytics queries this is the single biggest scan reduction,
independent of the indexes.
