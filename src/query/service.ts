import type { PoolClient } from 'pg';
import { readTransaction } from '../db/router.js';
import {
  planSegments,
  shouldUseRollups,
  usesRollup,
  type Segment,
  type SegmentSource,
} from '../rollup/plan.js';
import type { TimeRange } from './range.js';

export type { TimeRange } from './range.js';

/**
 * Analytics service. Reads go through the lag-aware router: the replica while
 * it is healthy, the primary when it lags more than REPLICA_MAX_LAG_MS or
 * cannot be reached. Every query is tenant-scoped via SET LOCAL
 * app.current_tenant_id so RLS enforces isolation in addition to the explicit
 * WHERE tenant_id filters (defence in depth).
 *
 * Rollup routing:
 *   Ranges of one hour or less read raw `events`. Longer ranges are split by
 *   planSegments() into whole hours (rollup_1h), whole minutes (rollup_1m) and
 *   raw edges, using the rollup watermarks read in the same transaction. The
 *   segments are queried separately and merged here. A rollup that has not
 *   caught up simply leaves more of the range to the finer source.
 *
 * No SELECT * anywhere. Every column is explicit.
 */

type Source = 'rollup' | 'raw';

const TABLE: Record<Exclude<SegmentSource, 'raw'>, string> = {
  rollup_1m: 'rollup_1m',
  rollup_1h: 'rollup_1h',
};

// Bucket truncation in UTC, independent of the session time zone, to match how
// the rollup worker aligns buckets.
const utcTrunc = (unit: string, expr: string): string =>
  `date_trunc(${unit}, (${expr}) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'`;

const scoped = <T>(tenantId: string, fn: (client: PoolClient) => Promise<T>): Promise<T> =>
  readTransaction(fn, { tenantId });

async function readWatermarks(client: PoolClient): Promise<{ w1m: number; w1h: number }> {
  const { rows } = await client.query<{ name: string; watermark: Date | null }>(
    `SELECT name, watermark FROM rollup_watermarks`,
  );
  const get = (n: string): number => {
    const w = rows.find((r) => r.name === n)?.watermark;
    return w ? w.getTime() : 0;
  };
  return { w1m: get('rollup_1m'), w1h: get('rollup_1h') };
}

async function segmentsFor(
  client: PoolClient,
  range: TimeRange,
  useMinute: boolean,
): Promise<Segment[]> {
  if (!shouldUseRollups(range)) return [{ source: 'raw', from: range.from, to: range.to }];
  return planSegments(range, await readWatermarks(client), { useMinute });
}

const sourceOf = (segments: Segment[]): Source => (usesRollup(segments) ? 'rollup' : 'raw');

const num = (v: string | number | null | undefined): number => Number(v ?? 0);

// ---------------------------------------------------------------------------
// Overview: total requests, error rate, avg latency in range.
// ---------------------------------------------------------------------------
export interface Overview {
  total_requests: number;
  error_count: number;
  error_rate: number;
  avg_latency_ms: number;
  source: Source;
}

export async function overview(tenantId: string, range: TimeRange): Promise<Overview> {
  return scoped(tenantId, async (client) => {
    const segments = await segmentsFor(client, range, true);
    let total = 0;
    let errors = 0;
    let latencySum = 0;

    for (const seg of segments) {
      const { rows } = await client.query<{ n: string; e: string; s: string }>(
        seg.source === 'raw'
          ? `SELECT COUNT(*)                                   AS n,
                    COUNT(*) FILTER (WHERE status_code >= 400) AS e,
                    COALESCE(SUM(latency_ms), 0)               AS s
               FROM events
              WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3`
          : `SELECT COALESCE(SUM(request_count), 0)  AS n,
                    COALESCE(SUM(error_count), 0)    AS e,
                    COALESCE(SUM(sum_latency_ms), 0) AS s
               FROM ${TABLE[seg.source]}
              WHERE tenant_id = $1 AND bucket >= $2 AND bucket < $3`,
        [tenantId, seg.from, seg.to],
      );
      total += num(rows[0].n);
      errors += num(rows[0].e);
      latencySum += num(rows[0].s);
    }

    return {
      total_requests: total,
      error_count: errors,
      error_rate: total > 0 ? Number((errors / total).toFixed(4)) : 0,
      avg_latency_ms: total > 0 ? Math.round(latencySum / total) : 0,
      source: sourceOf(segments),
    };
  });
}

// ---------------------------------------------------------------------------
// Per-endpoint breakdown, sorted by request count.
// ---------------------------------------------------------------------------
export interface EndpointRow {
  endpoint: string;
  total_requests: number;
  error_count: number;
  avg_latency_ms: number;
}

export async function endpoints(
  tenantId: string,
  range: TimeRange,
  limit: number,
): Promise<{ source: Source; rows: EndpointRow[] }> {
  return scoped(tenantId, async (client) => {
    const segments = await segmentsFor(client, range, true);
    const acc = new Map<string, { n: number; e: number; s: number }>();

    for (const seg of segments) {
      const { rows } = await client.query<{ endpoint: string; n: string; e: string; s: string }>(
        seg.source === 'raw'
          ? `SELECT endpoint,
                    COUNT(*)                                   AS n,
                    COUNT(*) FILTER (WHERE status_code >= 400) AS e,
                    COALESCE(SUM(latency_ms), 0)               AS s
               FROM events
              WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
              GROUP BY endpoint`
          : `SELECT endpoint,
                    SUM(request_count)  AS n,
                    SUM(error_count)    AS e,
                    SUM(sum_latency_ms) AS s
               FROM ${TABLE[seg.source]}
              WHERE tenant_id = $1 AND bucket >= $2 AND bucket < $3
              GROUP BY endpoint`,
        [tenantId, seg.from, seg.to],
      );
      for (const r of rows) {
        const cur = acc.get(r.endpoint) ?? { n: 0, e: 0, s: 0 };
        cur.n += num(r.n);
        cur.e += num(r.e);
        cur.s += num(r.s);
        acc.set(r.endpoint, cur);
      }
    }

    const out: EndpointRow[] = [...acc]
      .map(([endpoint, v]) => ({
        endpoint,
        total_requests: v.n,
        error_count: v.e,
        avg_latency_ms: v.n > 0 ? Math.round(v.s / v.n) : 0,
      }))
      .sort((a, b) => b.total_requests - a.total_requests || a.endpoint.localeCompare(b.endpoint))
      .slice(0, limit);
    return { source: sourceOf(segments), rows: out };
  });
}

// ---------------------------------------------------------------------------
// Latency percentiles per hour (p50/p95/p99), optionally for one endpoint.
//
// Raw edges are exact. A rollup_1h bucket is exact per (endpoint, hour); for a
// tenant-wide query the per-endpoint percentiles of an hour are combined as a
// request-count-weighted average, which is an approximation (percentiles cannot
// be merged exactly). rollup_1m is not used: minute percentiles cannot be
// combined into an hour at all.
// ---------------------------------------------------------------------------
export interface LatencyRow {
  bucket: string;
  p50_latency_ms: number;
  p95_latency_ms: number;
  p99_latency_ms: number;
}

export async function latency(
  tenantId: string,
  range: TimeRange,
  endpoint?: string,
): Promise<{ source: Source; rows: LatencyRow[] }> {
  return scoped(tenantId, async (client) => {
    const segments = await segmentsFor(client, range, false);
    const byBucket = new Map<string, LatencyRow>();

    for (const seg of segments) {
      const { rows } = await client.query<LatencyRow>(
        seg.source === 'raw'
          ? `SELECT ${utcTrunc("'hour'", 'ingested_at')} AS bucket,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_latency_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency_ms
               FROM events
              WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
                AND ($4::text IS NULL OR endpoint = $4)
              GROUP BY 1`
          : `SELECT bucket,
                    SUM(p50_latency_ms * request_count) / SUM(request_count) AS p50_latency_ms,
                    SUM(p95_latency_ms * request_count) / SUM(request_count) AS p95_latency_ms,
                    SUM(p99_latency_ms * request_count) / SUM(request_count) AS p99_latency_ms
               FROM rollup_1h
              WHERE tenant_id = $1 AND bucket >= $2 AND bucket < $3
                AND ($4::text IS NULL OR endpoint = $4)
              GROUP BY bucket`,
        [tenantId, seg.from, seg.to, endpoint ?? null],
      );
      for (const r of rows) {
        const row = numifyLatency(r);
        byBucket.set(row.bucket, row);
      }
    }

    const rows = [...byBucket.values()].sort((a, b) => a.bucket.localeCompare(b.bucket));
    return { source: sourceOf(segments), rows };
  });
}

function numifyLatency(r: LatencyRow): LatencyRow {
  return {
    bucket: toIso(r.bucket),
    p50_latency_ms: Number(r.p50_latency_ms),
    p95_latency_ms: Number(r.p95_latency_ms),
    p99_latency_ms: Number(r.p99_latency_ms),
  };
}

// ---------------------------------------------------------------------------
// Error breakdown by status code and endpoint. Needs status_code, which the
// rollups do not keep, so it always reads raw events (partial error index).
// ---------------------------------------------------------------------------
export interface ErrorRow {
  endpoint: string;
  status_code: number;
  count: number;
}

export async function errors(
  tenantId: string,
  range: TimeRange,
): Promise<{ rows: ErrorRow[] }> {
  return scoped(tenantId, async (client) => {
    const { rows } = await client.query<ErrorRow>(
      `SELECT endpoint, status_code, COUNT(*)::bigint AS count
       FROM events
       WHERE tenant_id = $1
         AND ingested_at >= $2 AND ingested_at < $3
         AND status_code >= 400
       GROUP BY endpoint, status_code
       ORDER BY count DESC
       LIMIT 1000`,
      [tenantId, range.from, range.to],
    );
    return {
      rows: rows.map((r) => ({
        endpoint: r.endpoint,
        status_code: Number(r.status_code),
        count: Number(r.count),
      })),
    };
  });
}

// ---------------------------------------------------------------------------
// Request volume time series, bucketed by hour or day (UTC).
// ---------------------------------------------------------------------------
export interface TimeseriesRow {
  bucket: string;
  total_requests: number;
  error_count: number;
}

export async function timeseries(
  tenantId: string,
  range: TimeRange,
  granularity: 'hour' | 'day',
): Promise<{ source: Source; rows: TimeseriesRow[] }> {
  return scoped(tenantId, async (client) => {
    const segments = await segmentsFor(client, range, true);
    const byBucket = new Map<string, TimeseriesRow>();

    for (const seg of segments) {
      const { rows } = await client.query<TimeseriesRow>(
        seg.source === 'raw'
          ? `SELECT ${utcTrunc('$4', 'ingested_at')}                AS bucket,
                    COUNT(*)::bigint                                AS total_requests,
                    COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count
               FROM events
              WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
              GROUP BY 1`
          : `SELECT ${utcTrunc('$4', 'bucket')}  AS bucket,
                    SUM(request_count)::bigint   AS total_requests,
                    SUM(error_count)::bigint     AS error_count
               FROM ${TABLE[seg.source]}
              WHERE tenant_id = $1 AND bucket >= $2 AND bucket < $3
              GROUP BY 1`,
        [tenantId, seg.from, seg.to, granularity],
      );
      for (const r of rows) {
        const row = numifyTs(r);
        const cur = byBucket.get(row.bucket);
        if (cur) {
          cur.total_requests += row.total_requests;
          cur.error_count += row.error_count;
        } else {
          byBucket.set(row.bucket, row);
        }
      }
    }

    const rows = [...byBucket.values()].sort((a, b) => a.bucket.localeCompare(b.bucket));
    return { source: sourceOf(segments), rows };
  });
}

function numifyTs(r: TimeseriesRow): TimeseriesRow {
  return {
    bucket: toIso(r.bucket),
    total_requests: Number(r.total_requests),
    error_count: Number(r.error_count),
  };
}

// Time buckets come back from pg as Date objects (timestamptz columns) even
// though the row types declare them as string. Normalize to an ISO string so
// the returned value matches its declared type for non-HTTP callers.
function toIso(bucket: string | Date): string {
  return bucket instanceof Date ? bucket.toISOString() : String(bucket);
}
