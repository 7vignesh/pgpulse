# 0008. Buffered ingest with binary COPY

Status: Accepted

## Context

Ingest ran one transaction per HTTP request (`BEGIN`, `SET LOCAL`, one
`INSERT`, `COMMIT`), so the common pattern of one event per request paid a WAL
flush per event. Retries after a timeout inserted duplicates, and the only
backpressure was a per-key request rate limit.

## Decision

- `POST /v1/events` and `/v1/events/batch` validate, then put events into a
  bounded in-memory ring buffer and return `202 Accepted`
  (`src/ingest/buffer.ts`, `pipeline.ts`).
- A flush runs at `INGEST_BATCH_SIZE` events or every `INGEST_FLUSH_INTERVAL_MS`.
  Per tenant, in one transaction: temp staging table (`ON COMMIT DROP`), binary
  `COPY` into it, then one `INSERT ... SELECT` into `events` (`src/ingest/flush.ts`).
  One backend and one transaction, so it works through PgBouncer transaction
  pooling (ADR 0001), and `SET LOCAL` keeps RLS in force (ADR 0002).
- Idempotency: `ingest_keys (tenant_id, idempotency_key)`, claimed with
  `ON CONFLICT DO NOTHING` in the same transaction, swept after 7 days. A side
  table is used because a unique index on partitioned `events` would have to
  include `ingested_at`.
- Backpressure: per-tenant token bucket counted in events, plus a buffer
  high-watermark of 85 %. Either returns `429` with `Retry-After`.
- Shutdown drains the buffer before the pools close.

## Consequences

- `202` means buffered, not committed. A hard crash loses up to one flush
  interval of events; writes that keep failing are retried 3 times, then
  dropped and counted (`/internal/db-health` shows the count).
- The API no longer returns the event id.
- Buckets and buffer are per app instance: N instances allow N times the quota.
- Values Postgres would reject (NUL bytes, bad IP, a timestamp outside the
  partition window, ADR 0009) are rejected before buffering, so one bad event
  cannot fail a whole tenant batch.
