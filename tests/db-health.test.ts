import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import request from 'supertest';
import type { FastifyInstance } from 'fastify';
import {
  cacheHitRatio,
  isAuthorized,
  shapeConnections,
  truncateQuery,
} from '../src/health/db-health.js';

describe('isAuthorized', () => {
  it('accepts the exact bearer token, case-insensitive scheme', () => {
    expect(isAuthorized('Bearer s3cret', 's3cret')).toBe(true);
    expect(isAuthorized('bearer s3cret', 's3cret')).toBe(true);
    expect(isAuthorized('  Bearer   s3cret  ', 's3cret')).toBe(true);
  });
  it('rejects wrong, partial, longer, or missing tokens', () => {
    expect(isAuthorized('Bearer s3cre', 's3cret')).toBe(false);
    expect(isAuthorized('Bearer s3cret!', 's3cret')).toBe(false);
    expect(isAuthorized('s3cret', 's3cret')).toBe(false);
    expect(isAuthorized('Basic s3cret', 's3cret')).toBe(false);
    expect(isAuthorized(undefined, 's3cret')).toBe(false);
    expect(isAuthorized('Bearer ', 's3cret')).toBe(false);
  });
  it('is never satisfied when no token is configured', () => {
    expect(isAuthorized('Bearer ', '')).toBe(false);
    expect(isAuthorized('Bearer x', undefined)).toBe(false);
  });
});

describe('metric shaping', () => {
  it('cacheHitRatio', () => {
    expect(cacheHitRatio(990, 10)).toBe(0.99);
    expect(cacheHitRatio(2, 1)).toBe(0.6667);
    expect(cacheHitRatio(0, 0)).toBeNull();
  });
  it('shapeConnections groups states and computes utilization', () => {
    const s = shapeConnections(
      [
        { state: 'active', count: 3 },
        { state: 'idle', count: 10 },
        { state: 'idle in transaction', count: 1 },
        { state: 'idle in transaction (aborted)', count: 1 },
        { state: null, count: 5 },
      ],
      100,
    );
    expect(s).toEqual({ total: 20, active: 3, idle: 10, idle_in_transaction: 2, max_connections: 100, utilization: 0.2 });
    expect(shapeConnections([], 0).utilization).toBe(0);
  });
  it('truncateQuery collapses whitespace and caps length', () => {
    expect(truncateQuery('SELECT  1\n  FROM\tt')).toBe('SELECT 1 FROM t');
    const long = truncateQuery('x'.repeat(1000), 20);
    expect(long).toHaveLength(20);
    expect(long.endsWith('...')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Route, against a real database (skipped when none is reachable).
// ---------------------------------------------------------------------------

async function dbReachable(): Promise<boolean> {
  const cs = process.env.ADMIN_DATABASE_URL;
  if (!cs) return false;
  const c = new pg.Client({ connectionString: cs, connectionTimeoutMillis: 2000 });
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

d('integration: GET /internal/db-health', () => {
  let app: FastifyInstance;
  const saved = process.env.INTERNAL_API_TOKEN;

  beforeAll(async () => {
    const { buildServer } = await import('../src/server.js');
    app = await buildServer();
    await app.ready();
  });

  afterAll(async () => {
    if (saved === undefined) delete process.env.INTERNAL_API_TOKEN;
    else process.env.INTERNAL_API_TOKEN = saved;
    await app?.close();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  it('does not exist (404) when INTERNAL_API_TOKEN is unset', async () => {
    delete process.env.INTERNAL_API_TOKEN;
    const res = await request(app.server).get('/internal/db-health').set('authorization', 'Bearer anything');
    expect(res.status).toBe(404);
  });

  it('requires the bearer token', async () => {
    process.env.INTERNAL_API_TOKEN = 'itest-token';
    const none = await request(app.server).get('/internal/db-health');
    expect(none.status).toBe(401);
    expect(none.headers['www-authenticate']).toBe('Bearer');
    const wrong = await request(app.server).get('/internal/db-health').set('authorization', 'Bearer nope');
    expect(wrong.status).toBe(401);
  });

  it('returns all five sections with live values', async () => {
    process.env.INTERNAL_API_TOKEN = 'itest-token';
    // Make sure pg_stat_statements has at least one entry to report.
    await request(app.server).get('/health');
    const res = await request(app.server).get('/internal/db-health').set('authorization', 'Bearer itest-token');
    expect(res.status).toBe(200);
    expect(res.headers['cache-control']).toBe('no-store');
    const b = res.body;

    expect(b.cache_hit_ratio.ok).toBe(true);
    expect(b.cache_hit_ratio.ratio).toBeGreaterThan(0);
    expect(b.cache_hit_ratio.ratio).toBeLessThanOrEqual(1);

    expect(b.connections.ok).toBe(true);
    expect(b.connections.total).toBeGreaterThanOrEqual(1);
    expect(b.connections.max_connections).toBeGreaterThan(0);

    expect(b.replication_lag.ok).toBe(true);
    expect(b.replication_lag).toHaveProperty('configured');

    expect(b.ingest_buffer.ok).toBe(true);
    expect(b.ingest_buffer.available).toBe(true);
    expect(b.ingest_buffer.depth).toBeGreaterThanOrEqual(0);
    expect(b.ingest_buffer.fill_ratio).toBeGreaterThanOrEqual(0);
    expect(b.ingest_buffer.fill_ratio).toBeLessThanOrEqual(1);
    expect(typeof b.ingest_buffer.dropped).toBe('number');

    expect(b.slow_queries.ok).toBe(true);
    expect(Array.isArray(b.slow_queries.queries)).toBe(true);
    expect(b.slow_queries.queries.length).toBeGreaterThan(0);
    const q = b.slow_queries.queries[0];
    expect(q.query.length).toBeLessThanOrEqual(500);
    expect(typeof q.mean_exec_ms).toBe('number');
    expect(typeof q.calls).toBe('number');
  });
});
