import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import request from 'supertest';
import type { FastifyInstance } from 'fastify';

/**
 * Integration tests for buffered ingest. Need a reachable DB with migrations
 * applied (same setup as integration.test.ts); skipped otherwise.
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

const ev = (endpoint: string, extra: Record<string, unknown> = {}) => ({
  endpoint,
  method: 'GET' as const,
  status_code: 200,
  latency_ms: 5,
  ...extra,
});

d('integration: buffered COPY ingest', () => {
  let app: FastifyInstance;
  let admin: pg.Client;
  let keyA: string;
  let keyB: string;
  let tenantA: string;
  let tenantB: string;
  let flushIngest: () => Promise<void>;
  let stopIngest: () => Promise<void>;

  const count = async (tenant: string, endpoint: string): Promise<number> => {
    const { rows } = await admin.query<{ c: number }>(
      'SELECT COUNT(*)::int AS c FROM events WHERE tenant_id = $1 AND endpoint = $2',
      [tenant, endpoint],
    );
    return rows[0].c;
  };

  const post = (key: string, body: unknown) =>
    request(app.server).post('/v1/events/batch').set('x-api-key', key).send(body as object);

  beforeAll(async () => {
    // Small tenant quota so the 429 test is cheap. Read at startIngest().
    process.env.INGEST_TENANT_RATE = '1';
    process.env.INGEST_TENANT_BURST = '50';
    const { buildServer } = await import('../src/server.js');
    ({ flushIngest, stopIngest } = await import('../src/ingest/pipeline.js'));
    app = await buildServer();
    await app.ready();
    admin = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await admin.connect();

    const a = await request(app.server).post('/v1/tenants').send({ name: 'copy-a' });
    const b = await request(app.server).post('/v1/tenants').send({ name: 'copy-b' });
    keyA = a.body.api_key; tenantA = a.body.id;
    keyB = b.body.api_key; tenantB = b.body.id;
  });

  afterAll(async () => {
    await admin?.end();
    await app?.close();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  it('commits buffered events through binary COPY with correct column values', async () => {
    // Inside the partitioned window (retention .. today + 7d), with a
    // millisecond component to prove timestamps round-trip exactly.
    const at = new Date(Math.floor(Date.now() / 1000) * 1000 - 3 * 86_400_000 + 123).toISOString();
    const res = await post(keyA, {
      events: [
        ev('/copy/values', {
          method: 'POST',
          status_code: 503,
          latency_ms: 1234,
          user_agent: 'ua\twith\ttabs',
          ip_address: '10.1.2.3',
          metadata: { note: 'line1\nline2', n: 1 },
          ingested_at: at,
        }),
      ],
    });
    expect(res.status).toBe(202);
    await flushIngest();
    const { rows } = await admin.query(
      `SELECT method, status_code, latency_ms, user_agent, host(ip_address) AS ip,
              metadata, ingested_at
         FROM events WHERE tenant_id = $1 AND endpoint = '/copy/values'`,
      [tenantA],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].method).toBe('POST');
    expect(rows[0].status_code).toBe(503);
    expect(rows[0].latency_ms).toBe(1234);
    expect(rows[0].user_agent).toBe('ua\twith\ttabs');
    expect(rows[0].ip).toBe('10.1.2.3');
    expect(rows[0].metadata).toEqual({ note: 'line1\nline2', n: 1 });
    expect((rows[0].ingested_at as Date).toISOString()).toBe(at);
  });

  it('dedupes by idempotency_key within one batch and across batches', async () => {
    const body = {
      events: [
        ev('/idem', { idempotency_key: 'k1' }),
        ev('/idem', { idempotency_key: 'k1' }),
        ev('/idem', { idempotency_key: 'k2' }),
        ev('/idem'),
        ev('/idem'),
      ],
    };
    expect((await post(keyA, body)).status).toBe(202);
    await flushIngest();
    expect(await count(tenantA, '/idem')).toBe(4); // k1, k2, 2 keyless

    expect((await post(keyA, { events: [ev('/idem', { idempotency_key: 'k1' })] })).status).toBe(202);
    await flushIngest();
    expect(await count(tenantA, '/idem')).toBe(4);
  });

  it('accepts the Idempotency-Key header on single events', async () => {
    for (let i = 0; i < 2; i++) {
      const r = await request(app.server)
        .post('/v1/events')
        .set('x-api-key', keyA)
        .set('Idempotency-Key', 'hdr-1')
        .send(ev('/idem-hdr'));
      expect(r.status).toBe(202);
    }
    await flushIngest();
    expect(await count(tenantA, '/idem-hdr')).toBe(1);
  });

  it('scopes idempotency keys per tenant', async () => {
    await post(keyA, { events: [ev('/idem-scope', { idempotency_key: 'shared' })] });
    await post(keyB, { events: [ev('/idem-scope', { idempotency_key: 'shared' })] });
    await flushIngest();
    expect(await count(tenantA, '/idem-scope')).toBe(1);
    expect(await count(tenantB, '/idem-scope')).toBe(1);
  });

  it("writes each tenant's events under its own tenant_id when one flush mixes tenants", async () => {
    await Promise.all([
      post(keyA, { events: [ev('/mixed'), ev('/mixed')] }),
      post(keyB, { events: [ev('/mixed')] }),
    ]);
    await flushIngest();
    expect(await count(tenantA, '/mixed')).toBe(2);
    expect(await count(tenantB, '/mixed')).toBe(1);
  });

  it('rejects events Postgres could not store before buffering them', async () => {
    const res = await post(keyA, { events: [ev('/bad\u0000nul')] });
    expect(res.status).toBe(400);
    const ip = await post(keyA, { events: [ev('/bad-ip', { ip_address: 'not-an-ip' })] });
    expect(ip.status).toBe(400);
  });

  it('returns 429 with Retry-After when the tenant quota is exceeded', async () => {
    const { rows } = await admin.query<{ id: string; api_key: string }>(
      `INSERT INTO tenants (name) VALUES ('copy-quota') RETURNING id, api_key`,
    );
    const key = rows[0].api_key;
    const big = { events: Array.from({ length: 40 }, () => ev('/quota')) };
    expect((await post(key, big)).status).toBe(202); // 40 of 50 burst tokens
    const res = await post(key, big); // needs 40, ~10 left
    expect(res.status).toBe(429);
    expect(Number(res.headers['retry-after'])).toBeGreaterThanOrEqual(1);
    expect(res.body.error).toMatch(/quota/);
    await flushIngest();
  });

  // Keep this last: stopIngest() is what the app's onClose hook calls, and it
  // leaves the pipeline stopped.
  it('drains the buffer on shutdown before the pool closes', async () => {
    const { rows } = await admin.query<{ id: string; api_key: string }>(
      `INSERT INTO tenants (name) VALUES ('copy-drain') RETURNING id, api_key`,
    );
    const r = await post(rows[0].api_key, { events: [ev('/drain'), ev('/drain'), ev('/drain')] });
    expect(r.status).toBe(202);
    await stopIngest();
    expect(await count(rows[0].id, '/drain')).toBe(3);
  });
});
