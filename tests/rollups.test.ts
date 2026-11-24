import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import request from 'supertest';
import type { FastifyInstance } from 'fastify';

/**
 * Rollup worker + query routing against a real Postgres (and the streaming
 * replica, which serves the queries). Skipped when no DB is reachable.
 */

const { Client } = pg;

async function dbReachable(): Promise<boolean> {
  const cs = process.env.ADMIN_DATABASE_URL;
  if (!cs) return false;
  const c = new Client({ connectionString: cs, connectionTimeoutMillis: 2000 });
  try {
    await c.connect();
    await c.query('SELECT 1');
    await c.end();
    return true;
  } catch {
    try { await c.end(); } catch { /* ignore */ }
    return false;
  }
}

const reachable = await dbReachable();
const d = reachable ? describe : describe.skip;

const HOUR = 3_600_000;

d('integration: incremental rollups + query routing', () => {
  let app: FastifyInstance;
  let admin: pg.Client;
  let pool: pg.Pool;
  let apiKey: string;
  let tenantId: string;
  let otherKey: string;
  let base: Date; // aligned UTC hour, 6h ago; data covers [base, base + 4h)
  let worker: typeof import('../src/rollup/worker.js');

  const q = async <T extends pg.QueryResultRow = Record<string, unknown>>(sql: string, p: unknown[] = []) =>
    (await admin.query<T>(sql, p)).rows;

  /** Wait until the replica has replayed everything the primary has written. */
  const waitForReplica = async (): Promise<void> => {
    if (!process.env.REPLICA_DATABASE_URL) return;
    const lsn = (await q<{ l: string }>('SELECT pg_current_wal_lsn()::text AS l'))[0].l;
    const rep = new Client({ connectionString: process.env.REPLICA_DATABASE_URL });
    await rep.connect();
    try {
      for (let i = 0; i < 100; i++) {
        const r = await rep.query<{ ok: boolean }>(
          'SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn AS ok',
          [lsn],
        );
        if (r.rows[0].ok) return;
        await new Promise((res) => setTimeout(res, 100));
      }
      throw new Error('replica did not catch up');
    } finally {
      await rep.end();
    }
  };

  const get = (path: string, key = apiKey) =>
    request(app.server).get(path).set('x-api-key', key);

  const qs = (from: Date, to: Date, extra = ''): string =>
    `from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(to.toISOString())}${extra}`;

  const resetRollups = async (): Promise<void> => {
    await admin.query('TRUNCATE rollup_1m, rollup_1h');
    await admin.query('UPDATE rollup_watermarks SET watermark = NULL');
  };

  const rawAgg = async (from: Date, to: Date, endpoint?: string) =>
    (
      await q<{ n: string; e: string; s: string; mn: number; mx: number; p95: number }>(
        `SELECT COUNT(*) AS n, COUNT(*) FILTER (WHERE status_code >= 400) AS e,
                SUM(latency_ms) AS s, MIN(latency_ms) AS mn, MAX(latency_ms) AS mx,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95
           FROM events
          WHERE tenant_id = $1 AND ingested_at >= $2 AND ingested_at < $3
            AND ($4::text IS NULL OR endpoint = $4)`,
        [tenantId, from, to, endpoint ?? null],
      )
    )[0];

  beforeAll(async () => {
    worker = await import('../src/rollup/worker.js');
    const { buildServer } = await import('../src/server.js');
    app = await buildServer();
    await app.ready();
    admin = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await admin.connect();
    pool = new pg.Pool({ connectionString: process.env.PRIMARY_DATABASE_URL, max: 6 });

    const t = await request(app.server).post('/v1/tenants').send({ name: 'rollup-a' });
    tenantId = t.body.id;
    apiKey = t.body.api_key;
    const o = await request(app.server).post('/v1/tenants').send({ name: 'rollup-b' });
    otherKey = o.body.api_key;

    base = new Date(Math.floor(Date.now() / HOUR) * HOUR - 6 * HOUR);
    // 4 hours x 3 endpoints x 40 events, spread over the hour at 5-minute steps.
    await admin.query(
      `INSERT INTO events (tenant_id, endpoint, method, status_code, latency_ms, ingested_at)
       SELECT $1, e, 'GET', CASE WHEN g % 5 = 0 THEN 500 ELSE 200 END,
              10 + (g * 7 % 90) + k * 3,
              $2::timestamptz + k * interval '1 hour' + (g % 12) * interval '5 minutes' + interval '10 seconds'
         FROM generate_series(0, 3) k, unnest(array['/a', '/b', '/c']) e, generate_series(1, 40) g`,
      [tenantId, base],
    );
    await resetRollups();
  });

  afterAll(async () => {
    await admin?.query('TRUNCATE rollup_1m, rollup_1h').catch(() => {});
    await admin?.query('UPDATE rollup_watermarks SET watermark = NULL').catch(() => {});
    await pool?.end();
    await admin?.end();
    await app?.close();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  describe('before any rollup has run', () => {
    it('serves long ranges from raw events with correct totals', async () => {
      await waitForReplica();
      const res = await get(`/v1/analytics/overview?${qs(base, new Date(base.getTime() + 4 * HOUR))}`);
      expect(res.status).toBe(200);
      expect(res.body.source).toBe('raw');
      expect(res.body.total_requests).toBe(480);
    });
  });

  describe('worker', () => {
    it('catches up from the first event, and rollup values equal a direct aggregate', async () => {
      await worker.catchUp(pool, 0);

      for (const unit of ['rollup_1m', 'rollup_1h']) {
        const rows = await q<{ n: string }>(
          `SELECT COALESCE(SUM(request_count), 0) AS n FROM ${unit} WHERE tenant_id = $1`,
          [tenantId],
        );
        expect(Number(rows[0].n)).toBe(480);
      }

      // Per (endpoint, hour): count, errors, sum, min, max and exact p95 match raw.
      for (let k = 0; k < 4; k++) {
        const from = new Date(base.getTime() + k * HOUR);
        const to = new Date(from.getTime() + HOUR);
        for (const ep of ['/a', '/b', '/c']) {
          const raw = await rawAgg(from, to, ep);
          const [r] = await q<Record<string, string | number>>(
            `SELECT request_count, error_count, sum_latency_ms, min_latency_ms, max_latency_ms, p95_latency_ms
               FROM rollup_1h WHERE tenant_id = $1 AND endpoint = $2 AND bucket = $3`,
            [tenantId, ep, from],
          );
          expect(Number(r.request_count)).toBe(Number(raw.n));
          expect(Number(r.error_count)).toBe(Number(raw.e));
          expect(Number(r.sum_latency_ms)).toBe(Number(raw.s));
          expect(r.min_latency_ms).toBe(raw.mn);
          expect(r.max_latency_ms).toBe(raw.mx);
          expect(Number(r.p95_latency_ms)).toBeCloseTo(Number(raw.p95), 6);
        }
      }
    });

    it('is incremental: a second pass does not re-read data behind the watermark', async () => {
      const [{ watermark: w1h }] = await q<{ watermark: Date }>(
        `SELECT watermark FROM rollup_watermarks WHERE name = 'rollup_1h'`,
      );
      // Corrupt an old bucket. If the worker re-aggregated history it would be repaired.
      await admin.query(
        `UPDATE rollup_1h SET request_count = 999999 WHERE tenant_id = $1 AND bucket = $2`,
        [tenantId, base],
      );
      const results = await worker.catchUp(pool, 0);
      expect(results.reduce((s, r) => s + r.rows, 0)).toBe(0);
      const [row] = await q<{ request_count: string }>(
        `SELECT SUM(request_count) AS request_count FROM rollup_1h WHERE tenant_id = $1 AND bucket = $2`,
        [tenantId, base],
      );
      // 3 endpoint rows x 999999: still corrupted, so history was not re-read.
      expect(Number(row.request_count)).toBe(3 * 999999);
      const [{ watermark: after }] = await q<{ watermark: Date }>(
        `SELECT watermark FROM rollup_watermarks WHERE name = 'rollup_1h'`,
      );
      expect(after.getTime()).toBeGreaterThanOrEqual(w1h.getTime());
    });

    it('re-running a range is idempotent and repairs the bucket', async () => {
      // Rewind both watermarks to just before the corrupted bucket and run again.
      await admin.query(`UPDATE rollup_watermarks SET watermark = $1`, [base]);
      await worker.catchUp(pool, 0);
      const rows = await q<{ n: string }>(
        `SELECT SUM(request_count) AS n FROM rollup_1h WHERE tenant_id = $1`,
        [tenantId],
      );
      expect(Number(rows[0].n)).toBe(480); // not doubled, corruption gone
      const m = await q<{ n: string }>(
        `SELECT SUM(request_count) AS n FROM rollup_1m WHERE tenant_id = $1`,
        [tenantId],
      );
      expect(Number(m[0].n)).toBe(480);
    });

    it('only picks up events at or after the watermark (documented limit for late events)', async () => {
      const [{ watermark }] = await q<{ watermark: Date }>(
        `SELECT watermark FROM rollup_watermarks WHERE name = 'rollup_1m'`,
      );
      // Behind the watermark: never aggregated.
      await admin.query(
        `INSERT INTO events (tenant_id, endpoint, method, status_code, latency_ms, ingested_at)
         VALUES ($1, '/late', 'GET', 200, 5, $2)`,
        [tenantId, new Date(watermark.getTime() - 3 * 60_000)],
      );
      await worker.catchUp(pool, 0);
      const late = await q<{ c: string }>(
        `SELECT COUNT(*) AS c FROM rollup_1m WHERE tenant_id = $1 AND endpoint = '/late'`,
        [tenantId],
      );
      expect(Number(late[0].c)).toBe(0);
      await admin.query(`DELETE FROM events WHERE tenant_id = $1 AND endpoint = '/late'`, [tenantId]);
    });

    it('advances the watermark only when the transaction commits', async () => {
      await admin.query(`UPDATE rollup_watermarks SET watermark = $1 WHERE name = 'rollup_1h'`, [base]);
      await admin.query(`TRUNCATE rollup_1h`);
      await admin.query(
        `ALTER TABLE rollup_1h ADD CONSTRAINT itest_block CHECK (request_count < 0) NOT VALID`,
      );
      try {
        await expect(
          worker.runRollupPass(pool, worker.LEVELS[1], 0),
        ).rejects.toThrow();
      } finally {
        await admin.query('ALTER TABLE rollup_1h DROP CONSTRAINT itest_block');
      }
      const [{ watermark }] = await q<{ watermark: Date }>(
        `SELECT watermark FROM rollup_watermarks WHERE name = 'rollup_1h'`,
      );
      expect(watermark.getTime()).toBe(base.getTime());
      const rows = await q<{ c: string }>('SELECT COUNT(*) AS c FROM rollup_1h');
      expect(Number(rows[0].c)).toBe(0);
      await worker.catchUp(pool, 0); // and it recovers on the next pass
      const ok = await q<{ n: string }>(
        `SELECT SUM(request_count) AS n FROM rollup_1h WHERE tenant_id = $1`,
        [tenantId],
      );
      expect(Number(ok[0].n)).toBe(480);
    });

    it('two concurrent workers never double-count (SKIP LOCKED)', async () => {
      await resetRollups();
      await Promise.all([worker.catchUp(pool, 0), worker.catchUp(pool, 0), worker.catchUp(pool, 0)]);
      await worker.catchUp(pool, 0); // finish anything a skipped worker left behind
      for (const unit of ['rollup_1m', 'rollup_1h']) {
        const rows = await q<{ n: string }>(
          `SELECT SUM(request_count) AS n FROM ${unit} WHERE tenant_id = $1`,
          [tenantId],
        );
        expect(Number(rows[0].n)).toBe(480);
      }
    });
  });

  describe('query routing (rollups caught up)', () => {
    const end = (): Date => new Date(base.getTime() + 4 * HOUR);

    it('ranges over 1h come from rollups, with the same answer as raw', async () => {
      await waitForReplica();
      const res = await get(`/v1/analytics/overview?${qs(base, end())}`);
      expect(res.body.source).toBe('rollup');
      const raw = await rawAgg(base, end());
      expect(res.body.total_requests).toBe(Number(raw.n));
      expect(res.body.error_count).toBe(Number(raw.e));
      expect(res.body.avg_latency_ms).toBe(Math.round(Number(raw.s) / Number(raw.n)));
    });

    it('ranges of 1h or less read raw events', async () => {
      const res = await get(`/v1/analytics/overview?${qs(base, new Date(base.getTime() + HOUR))}`);
      expect(res.body.source).toBe('raw');
      expect(res.body.total_requests).toBe(120);
    });

    it('unaligned ranges mix 1h, 1m and raw segments and still match raw exactly', async () => {
      const from = new Date(base.getTime() + 25 * 60_000 + 7_000); // 00:25:07
      const to = new Date(base.getTime() + 3 * HOUR + 40 * 60_000 + 30_000); // 03:40:30
      const res = await get(`/v1/analytics/overview?${qs(from, to)}`);
      expect(res.body.source).toBe('rollup');
      const raw = await rawAgg(from, to);
      expect(res.body.total_requests).toBe(Number(raw.n));
      expect(res.body.error_count).toBe(Number(raw.e));
    });

    it('endpoints breakdown matches raw', async () => {
      const res = await get(`/v1/analytics/endpoints?${qs(base, end())}`);
      expect(res.body.source).toBe('rollup');
      expect(res.body.rows).toHaveLength(3);
      for (const r of res.body.rows as { endpoint: string; total_requests: number }[]) {
        const raw = await rawAgg(base, end(), r.endpoint);
        expect(r.total_requests).toBe(Number(raw.n));
      }
    });

    it('timeseries (hour and day) matches raw', async () => {
      const hourly = await get(`/v1/analytics/timeseries?${qs(base, end(), '&granularity=hour')}`);
      expect(hourly.body.source).toBe('rollup');
      expect(hourly.body.rows).toHaveLength(4);
      expect(hourly.body.rows.every((r: { total_requests: number }) => r.total_requests === 120)).toBe(true);

      const daily = await get(`/v1/analytics/timeseries?${qs(base, end(), '&granularity=day')}`);
      const sum = (daily.body.rows as { total_requests: number }[]).reduce((s, r) => s + r.total_requests, 0);
      expect(sum).toBe(480);
    });

    it('latency for one endpoint over whole hours is exact', async () => {
      const res = await get(`/v1/analytics/latency?${qs(base, end(), '&endpoint=%2Fb')}`);
      expect(res.body.source).toBe('rollup');
      expect(res.body.rows).toHaveLength(4);
      const first = res.body.rows[0] as { bucket: string; p95_latency_ms: number };
      const raw = await rawAgg(base, new Date(base.getTime() + HOUR), '/b');
      expect(first.bucket).toBe(base.toISOString());
      expect(first.p95_latency_ms).toBeCloseTo(Number(raw.p95), 6);
    });

    it('errors always read raw events (status codes are not rolled up)', async () => {
      const res = await get(`/v1/analytics/errors?${qs(base, end())}`);
      expect(res.status).toBe(200);
      const total = (res.body.rows as { count: number }[]).reduce((s, r) => s + r.count, 0);
      expect(total).toBe(Number((await rawAgg(base, end())).e));
    });

    it('rollups are tenant-isolated (RLS)', async () => {
      const res = await get(`/v1/analytics/overview?${qs(base, end())}`, otherKey);
      expect(res.body.total_requests).toBe(0);
      const hidden = await get(`/v1/analytics/endpoints?${qs(base, end())}`, otherKey);
      expect(hidden.body.rows).toHaveLength(0);
    });

    it('a rollup that is behind leaves the gap to raw, so totals stay correct', async () => {
      // Pretend the hourly rollup only ran up to base+2h and minutes to base+3h.
      await admin.query(`UPDATE rollup_watermarks SET watermark = $1 WHERE name = 'rollup_1h'`, [new Date(base.getTime() + 2 * HOUR)]);
      await admin.query(`UPDATE rollup_watermarks SET watermark = $1 WHERE name = 'rollup_1m'`, [new Date(base.getTime() + 3 * HOUR)]);
      await waitForReplica();
      const res = await get(`/v1/analytics/overview?${qs(base, end())}`);
      expect(res.body.source).toBe('rollup');
      expect(res.body.total_requests).toBe(480);
    });
  });

  // Real replica: pause WAL replay, let lag grow past 2s, and watch the router.
  const failover = process.env.REPLICA_DATABASE_URL ? it : it.skip;
  failover('fails over to the primary when replication lag exceeds 2s, then recovers', async () => {
    const { readRouter } = await import('../src/db/router.js');
    const rep = new Client({ connectionString: process.env.REPLICA_DATABASE_URL });
    await rep.connect();
    try {
      await waitForReplica();
      await readRouter.probeOnce();
      expect(readRouter.status().target).toBe('replica');
      expect(readRouter.status().lag_ms).toBeLessThan(2000);

      await rep.query('SELECT pg_wal_replay_pause()');
      // A write the replica receives but cannot replay, then let the lag build.
      await admin.query(`UPDATE rollup_watermarks SET updated_at = now() WHERE name = 'rollup_1m'`);
      await new Promise((r) => setTimeout(r, 2600));
      await readRouter.probeOnce();
      const down = readRouter.status();
      expect(down.target).toBe('primary');
      expect(down.lag_ms).toBeGreaterThan(2000);

      // Reads keep working (served by the primary) while the replica is stale.
      const res = await get(`/v1/analytics/overview?${qs(base, new Date(base.getTime() + 4 * HOUR))}`);
      expect(res.status).toBe(200);
      expect(res.body.total_requests).toBe(480);

      await rep.query('SELECT pg_wal_replay_resume()');
      await waitForReplica();
      // Return needs REPLICA_RECOVERY_PROBES (3) consecutive good probes.
      for (let i = 0; i < 3; i++) await readRouter.probeOnce();
      expect(readRouter.status().target).toBe('replica');
      expect(readRouter.status().failovers).toBeGreaterThanOrEqual(1);
    } finally {
      await rep.query('SELECT pg_wal_replay_resume()').catch(() => {});
      await rep.end();
    }
  });

  it('exposes read router state on /health', async () => {
    const res = await request(app.server).get('/health');
    expect(res.body.read_router).toHaveProperty('target');
    expect(res.body.read_router).toHaveProperty('max_lag_ms', 2000);
  });
});
