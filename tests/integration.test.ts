import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import request from 'supertest';
import type { FastifyInstance } from 'fastify';

/**
 * End-to-end integration test against a real Postgres + the Fastify app.
 *
 * Requires migrations to have been applied to the target DB. Set
 * ADMIN_DATABASE_URL (and optionally PRIMARY/REPLICA) to a reachable instance.
 * If no DB is reachable the whole suite is skipped so `npm test` stays green
 * in environments without Docker.
 *
 * Run the stack first:  docker compose up -d
 * Then:                 ADMIN_DATABASE_URL=postgres://pgpulse:pgpulse_secret@localhost:5432/pgpulse \
 *                       PRIMARY_DATABASE_URL=postgres://pgpulse:pgpulse_secret@localhost:6432/pgpulse \
 *                       npm test
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

d('integration: ingest + analytics', () => {
  let app: FastifyInstance;
  let apiKey: string;
  let tenantId: string;

  beforeAll(async () => {
    const { buildServer } = await import('../src/server.js');
    app = await buildServer();
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  it('creates a tenant and returns an api key', async () => {
    const res = await request(app.server)
      .post('/v1/tenants')
      .send({ name: 'itest-tenant', plan: 'pro' });
    expect(res.status).toBe(201);
    expect(res.body.api_key).toMatch(/^[0-9a-f]{64}$/);
    apiKey = res.body.api_key;
    tenantId = res.body.id;
  });

  it('rejects ingest without an api key', async () => {
    const res = await request(app.server)
      .post('/v1/events')
      .send({ endpoint: '/x', method: 'GET', status_code: 200, latency_ms: 5 });
    expect(res.status).toBe(401);
  });

  it('ingests a single event', async () => {
    const res = await request(app.server)
      .post('/v1/events')
      .set('x-api-key', apiKey)
      .send({
        endpoint: '/api/login',
        method: 'POST',
        status_code: 200,
        latency_ms: 42,
        metadata: { region: 'us-east-1' },
      });
    expect(res.status).toBe(201);
    expect(res.body.id).toBeDefined();
  });

  it('batch-ingests events in one transaction', async () => {
    const events = Array.from({ length: 50 }, (_, i) => ({
      endpoint: i % 2 === 0 ? '/api/login' : '/api/orders',
      method: 'GET' as const,
      status_code: i % 10 === 0 ? 500 : 200,
      latency_ms: 10 + i,
    }));
    const res = await request(app.server)
      .post('/v1/events/batch')
      .set('x-api-key', apiKey)
      .send({ events });
    expect(res.status).toBe(201);
    expect(res.body.inserted).toBe(50);
  });

  it('returns an overview scoped to the tenant', async () => {
    const res = await request(app.server)
      .get('/v1/analytics/overview')
      .set('x-api-key', apiKey);
    expect(res.status).toBe(200);
    expect(res.body.total_requests).toBeGreaterThanOrEqual(51);
    expect(res.body).toHaveProperty('error_rate');
    expect(['raw', 'materialized_view']).toContain(res.body.source);
  });

  it('returns per-endpoint breakdown sorted by count', async () => {
    const res = await request(app.server)
      .get('/v1/analytics/endpoints?limit=10')
      .set('x-api-key', apiKey);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body.rows)).toBe(true);
    const counts = res.body.rows.map((r: { total_requests: number }) => r.total_requests);
    const sorted = [...counts].sort((a, b) => b - a);
    expect(counts).toEqual(sorted);
  });

  it('returns error breakdown', async () => {
    const res = await request(app.server)
      .get('/v1/analytics/errors')
      .set('x-api-key', apiKey);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body.rows)).toBe(true);
  });

  it('enforces tenant isolation via RLS (second tenant sees nothing)', async () => {
    const other = await request(app.server)
      .post('/v1/tenants')
      .send({ name: 'itest-other' });
    const res = await request(app.server)
      .get('/v1/analytics/overview')
      .set('x-api-key', other.body.api_key);
    expect(res.status).toBe(200);
    expect(res.body.total_requests).toBe(0);
  });

  it('rotates the api key, invalidating the old one', async () => {
    const rot = await request(app.server).post(`/v1/tenants/${tenantId}/rotate-key`);
    expect(rot.status).toBe(200);
    expect(rot.body.api_key).not.toBe(apiKey);

    const stale = await request(app.server)
      .get('/v1/analytics/overview')
      .set('x-api-key', apiKey);
    expect(stale.status).toBe(401);
    apiKey = rot.body.api_key;
  });

  it('reports health with pool stats', async () => {
    const res = await request(app.server).get('/health');
    expect([200, 503]).toContain(res.status);
    expect(res.body.pools.primary).toHaveProperty('total');
  });
});
