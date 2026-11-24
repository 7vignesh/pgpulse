import type { Pool } from 'pg';
import process from 'node:process';
import { withTransaction } from '../db/pool.js';
import { parsePositiveInt } from '../middleware/config.js';

/**
 * Incremental rollup worker.
 *
 * One pass for a level, in a single transaction:
 *   1. lock the level's watermark row (FOR UPDATE SKIP LOCKED, so concurrent
 *      workers on other app instances skip the level instead of waiting)
 *   2. pick [start, end): start = watermark, end = min(now - safety lag
 *      truncated to the bucket size, start + max span)
 *   3. aggregate raw events WHERE ingested_at >= start AND ingested_at < end
 *   4. upsert into the rollup table and advance the watermark to `end`
 *
 * The watermark is an exclusive upper bound aligned to the bucket size, so the
 * next pass reads exactly the events at or after it (`>=` instead of `>`, so
 * an event stamped exactly on the boundary is not lost). Because every bucket
 * in [start, end) is new, a pass never has to revisit old buckets, and the
 * upsert only matters when a range is deliberately re-run.
 */

export interface RollupLevel {
  name: 'rollup_1m' | 'rollup_1h';
  unit: 'minute' | 'hour';
  /** Longest range one pass covers, so a first run over old data is chunked. */
  maxSpanSec: number;
}

// Table and unit names are interpolated into SQL; they only ever come from this
// constant list, never from input.
export const LEVELS: readonly RollupLevel[] = [
  { name: 'rollup_1m', unit: 'minute', maxSpanSec: 24 * 3600 },
  { name: 'rollup_1h', unit: 'hour', maxSpanSec: 30 * 24 * 3600 },
];

/** Truncate to the level's bucket in UTC, independent of the session time zone. */
const trunc = (unit: RollupLevel['unit'], expr: string): string =>
  `date_trunc('${unit}', (${expr}) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'`;

export interface PassResult {
  level: RollupLevel['name'];
  status: 'advanced' | 'idle' | 'busy';
  from: Date | null;
  to: Date | null;
  rows: number;
}

export async function runRollupPass(
  pool: Pool,
  level: RollupLevel,
  safetyLagSec: number,
): Promise<PassResult> {
  const idle = (status: PassResult['status']): PassResult => ({
    level: level.name, status, from: null, to: null, rows: 0,
  });

  return withTransaction(pool, async (c) => {
    const wm = await c.query<{ watermark: Date | null }>(
      `SELECT watermark FROM rollup_watermarks WHERE name = $1 FOR UPDATE SKIP LOCKED`,
      [level.name],
    );
    if (wm.rowCount === 0) return idle('busy');

    let start = wm.rows[0].watermark;
    if (start === null) {
      const first = await c.query<{ first: Date | null }>(
        `SELECT ${trunc(level.unit, 'min(ingested_at)')} AS first FROM events`,
      );
      start = first.rows[0].first;
      if (start === null) return idle('idle'); // no events yet
    }

    const bounds = await c.query<{ hi: Date; stop: Date }>(
      `SELECT ${trunc(level.unit, 'now() - make_interval(secs => $1::float8)')} AS hi,
              LEAST(${trunc(level.unit, 'now() - make_interval(secs => $1::float8)')},
                    $2::timestamptz + make_interval(secs => $3::float8)) AS stop`,
      [safetyLagSec, start, level.maxSpanSec],
    );
    const end = bounds.rows[0].stop;
    if (end <= start) return idle('idle');

    const res = await c.query(
      `INSERT INTO ${level.name}
         (tenant_id, endpoint, bucket, request_count, error_count, sum_latency_ms,
          min_latency_ms, max_latency_ms, p50_latency_ms, p95_latency_ms, p99_latency_ms)
       SELECT tenant_id, endpoint, ${trunc(level.unit, 'ingested_at')} AS bucket,
              COUNT(*),
              COUNT(*) FILTER (WHERE status_code >= 400),
              SUM(latency_ms)::bigint,
              MIN(latency_ms),
              MAX(latency_ms),
              PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms),
              PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms),
              PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)
         FROM events
        WHERE ingested_at >= $1 AND ingested_at < $2
        GROUP BY tenant_id, endpoint, ${trunc(level.unit, 'ingested_at')}
       ON CONFLICT (tenant_id, bucket, endpoint) DO UPDATE SET
         request_count  = EXCLUDED.request_count,
         error_count    = EXCLUDED.error_count,
         sum_latency_ms = EXCLUDED.sum_latency_ms,
         min_latency_ms = EXCLUDED.min_latency_ms,
         max_latency_ms = EXCLUDED.max_latency_ms,
         p50_latency_ms = EXCLUDED.p50_latency_ms,
         p95_latency_ms = EXCLUDED.p95_latency_ms,
         p99_latency_ms = EXCLUDED.p99_latency_ms`,
      [start, end],
    );

    await c.query(
      `UPDATE rollup_watermarks SET watermark = $2, updated_at = now() WHERE name = $1`,
      [level.name, end],
    );
    return { level: level.name, status: 'advanced', from: start, to: end, rows: res.rowCount ?? 0 };
  });
}

/** Run passes for every level until each is caught up. */
export async function catchUp(
  pool: Pool,
  safetyLagSec: number,
  log: (msg: string) => void = () => {},
  maxPasses = 5_000,
): Promise<PassResult[]> {
  const results: PassResult[] = [];
  for (const level of LEVELS) {
    for (let i = 0; i < maxPasses; i++) {
      const r = await runRollupPass(pool, level, safetyLagSec);
      if (r.status !== 'advanced') break;
      results.push(r);
      if (r.rows > 0) log(`rollup ${level.name}: +${r.rows} rows up to ${r.to?.toISOString()}`);
    }
  }
  return results;
}

/** Background loop. Returns an async stop(). Safe to run on many instances. */
export function startRollupWorker(log: (msg: string) => void, pool: Pool): () => Promise<void> {
  const intervalMs = parsePositiveInt(process.env.ROLLUP_INTERVAL_MS, 30_000);
  const safetyLagSec = parsePositiveInt(process.env.ROLLUP_SAFETY_LAG_SECONDS, 30);
  let inFlight: Promise<void> | null = null;
  let stopped = false;

  const tick = (): void => {
    if (inFlight || stopped) return;
    inFlight = (async () => {
      try {
        await catchUp(pool, safetyLagSec, log);
      } catch (err) {
        log(`rollup worker error: ${err instanceof Error ? err.message : String(err)}`);
      }
    })().finally(() => {
      inFlight = null;
    });
  };

  const first = setTimeout(tick, 2_000);
  const timer = setInterval(tick, intervalMs);
  first.unref();
  timer.unref();
  return async () => {
    stopped = true;
    clearTimeout(first);
    clearInterval(timer);
    await inFlight;
  };
}
