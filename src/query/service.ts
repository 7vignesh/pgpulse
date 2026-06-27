import type { PoolClient } from 'pg';
import { replicaPool, withTransaction } from '../db/pool.js';
import { servableFromMV, type TimeRange } from './range.js';

export type { TimeRange } from './range.js';

/**
 * Analytics service. All reads route to the replica pool (read scaling).
 * Every query is tenant-scoped via SET LOCAL app.current_tenant_id so RLS
 * enforces isolation in addition to explicit WHERE tenant_id filters
 * (defence in depth).
 *
 * Materialized-view routing:
 *   The `hourly_stats` MV is refreshed hourly. For any time window whose end
 *   is older than the start of the current hour, completed hours can be served
 *   from the MV. Data inside the current (incomplete) hour must come from raw
 *   `events`. queryWindow() decides per request which source(s) to use.
 *
 * No SELECT * anywhere. Every column is explicit.
 */

async function scoped<T>(
  tenantId: string,
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> {
  return withTransaction(replicaPool, fn, { tenantId, readOnly: true });
}

// ---------------------------------------------------------------------------
// Overview: total requests, error rate, avg latency in range.
// ---------------------------------------------------------------------------
export interface Overview {
  total_requests: number;
  error_count: number;
  error_rate: number;
  avg_latency_ms: number;
  source: 'materialized_view' | 'raw';
}

export async function overview(tenantId: string, range: TimeRange): Promise<Overview> {
  const fromMV = servableFromMV(range);
  return scoped(tenantId, async (client) => {
    if (fromMV) {
      const { rows } = await client.query<{
        total_requests: string;
        error_count: string;
        avg_latency_ms: string | null;
      }>(
        `SELECT
           COALESCE(SUM(total_requests), 0)              AS total_requests,
           COALESCE(SUM(error_count), 0)                 AS error_count,
           COALESCE(ROUND(
             SUM(avg_latency_ms * total_requests)
             / NULLIF(SUM(total_requests), 0)), 0)       AS avg_latency_ms
         FROM hourly_stats
         WHERE tenant_id = $1 AND hour >= $2 AND hour < $3`,
        [tenantId, range.from, range.to],
      );
      return shapeOverview(rows[0], 'materialized_view');
    }

    const { rows } = await client.query<{
      total_requests: string;
      error_count: string;
      avg_latency_ms: string | null;
    }>(
      `SELECT
         COUNT(*)                                     AS total_requests,
         COUNT(*) FILTER (WHERE status_code >= 400)   AS error_count,
         COALESCE(ROUND(AVG(latency_ms)), 0)          AS avg_latency_ms
       FROM events
       WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3`,
      [tenantId, range.from, range.to],
    );
    return shapeOverview(rows[0], 'raw');
  });
}

function shapeOverview(
  row: { total_requests: string; error_count: string; avg_latency_ms: string | null },
  source: Overview['source'],
): Overview {
  const total = Number(row.total_requests);
  const errors = Number(row.error_count);
  return {
    total_requests: total,
    error_count: errors,
    error_rate: total > 0 ? Number((errors / total).toFixed(4)) : 0,
    avg_latency_ms: Number(row.avg_latency_ms ?? 0),
    source,
  };
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
): Promise<{ source: string; rows: EndpointRow[] }> {
  const fromMV = servableFromMV(range);
  return scoped(tenantId, async (client) => {
    if (fromMV) {
      const { rows } = await client.query<EndpointRow>(
        `SELECT
           endpoint,
           SUM(total_requests)::bigint                       AS total_requests,
           SUM(error_count)::bigint                          AS error_count,
           ROUND(SUM(avg_latency_ms * total_requests)
             / NULLIF(SUM(total_requests), 0))               AS avg_latency_ms
         FROM hourly_stats
         WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
         GROUP BY endpoint
         ORDER BY total_requests DESC
         LIMIT $4`,
        [tenantId, range.from, range.to, limit],
      );
      return { source: 'materialized_view', rows: rows.map(numifyEndpoint) };
    }

    const { rows } = await client.query<EndpointRow>(
      `SELECT
         endpoint,
         COUNT(*)::bigint                                    AS total_requests,
         COUNT(*) FILTER (WHERE status_code >= 400)::bigint  AS error_count,
         ROUND(AVG(latency_ms))                              AS avg_latency_ms
       FROM events
       WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
       GROUP BY endpoint
       ORDER BY total_requests DESC
       LIMIT $4`,
      [tenantId, range.from, range.to, limit],
    );
    return { source: 'raw', rows: rows.map(numifyEndpoint) };
  });
}

function numifyEndpoint(r: EndpointRow): EndpointRow {
  return {
    endpoint: r.endpoint,
    total_requests: Number(r.total_requests),
    error_count: Number(r.error_count),
    avg_latency_ms: Number(r.avg_latency_ms),
  };
}

// ---------------------------------------------------------------------------
// Latency percentiles over time (p50/p95/p99), optionally per endpoint.
// Percentiles cannot be re-aggregated from hourly averages, so when an exact
// percentile across an arbitrary window is needed we compute from raw events.
// The MV already stores per-hour percentiles, used for the completed-hours
// path bucketed by hour.
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
): Promise<{ source: string; rows: LatencyRow[] }> {
  const fromMV = servableFromMV(range) && !endpoint;
  return scoped(tenantId, async (client) => {
    if (fromMV) {
      const { rows } = await client.query<LatencyRow>(
        `SELECT
           hour AS bucket,
           ROUND(AVG(p50_latency_ms)) AS p50_latency_ms,
           ROUND(AVG(p95_latency_ms)) AS p95_latency_ms,
           ROUND(AVG(p99_latency_ms)) AS p99_latency_ms
         FROM hourly_stats
         WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
         GROUP BY hour
         ORDER BY hour`,
        [tenantId, range.from, range.to],
      );
      return { source: 'materialized_view', rows: rows.map(numifyLatency) };
    }

    const { rows } = await client.query<LatencyRow>(
      `SELECT
         date_trunc('hour', ingested_at) AS bucket,
         PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_latency_ms,
         PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
         PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency_ms
       FROM events
       WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
         AND ($4::text IS NULL OR endpoint = $4)
       GROUP BY date_trunc('hour', ingested_at)
       ORDER BY 1`,
      [tenantId, range.from, range.to, endpoint ?? null],
    );
    return { source: 'raw', rows: rows.map(numifyLatency) };
  });
}

function numifyLatency(r: LatencyRow): LatencyRow {
  return {
    bucket: r.bucket,
    p50_latency_ms: Number(r.p50_latency_ms),
    p95_latency_ms: Number(r.p95_latency_ms),
    p99_latency_ms: Number(r.p99_latency_ms),
  };
}

// ---------------------------------------------------------------------------
// Error breakdown by status code and endpoint. Uses the partial error index.
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
       ORDER BY count DESC`,
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
// Request volume time series, bucketed by hour or day.
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
): Promise<{ source: string; rows: TimeseriesRow[] }> {
  const fromMV = servableFromMV(range);
  return scoped(tenantId, async (client) => {
    if (fromMV) {
      const { rows } = await client.query<TimeseriesRow>(
        `SELECT
           date_trunc($4, hour) AS bucket,
           SUM(total_requests)::bigint AS total_requests,
           SUM(error_count)::bigint    AS error_count
         FROM hourly_stats
         WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
         GROUP BY date_trunc($4, hour)
         ORDER BY 1`,
        [tenantId, range.from, range.to, granularity],
      );
      return { source: 'materialized_view', rows: rows.map(numifyTs) };
    }

    const { rows } = await client.query<TimeseriesRow>(
      `SELECT
         date_trunc($4, ingested_at) AS bucket,
         COUNT(*)::bigint                                   AS total_requests,
         COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count
       FROM events
       WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
       GROUP BY date_trunc($4, ingested_at)
       ORDER BY 1`,
      [tenantId, range.from, range.to, granularity],
    );
    return { source: 'raw', rows: rows.map(numifyTs) };
  });
}

function numifyTs(r: TimeseriesRow): TimeseriesRow {
  return {
    bucket: r.bucket,
    total_requests: Number(r.total_requests),
    error_count: Number(r.error_count),
  };
}
