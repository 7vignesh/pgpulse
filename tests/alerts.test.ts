import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import request from 'supertest';
import type { FastifyInstance } from 'fastify';

/**
 * Integration tests for the alert system. Requires a reachable DB with
 * migrations applied (same setup as integration.test.ts). Skips otherwise.
 *
 * Covers: rule CRUD + tenant isolation, validation (https-only, enums),
 * the in-DB evaluate_alert_rules() function firing on a breached threshold,
 * cooldown, and the alert_events listing.
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

d('integration: alert system', () => {
  let app: FastifyInstance;
  let admin: pg.Client;
  let apiKey: string;
  let tenantId: string;
  let ruleId: string;

  beforeAll(async () => {
    const { buildServer } = await import('../src/server.js');
    app = await buildServer();
    await app.ready();

    admin = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await admin.connect();

    const res = await request(app.server)
      .post('/v1/tenants')
      .send({ name: 'alert-itest', plan: 'pro' });
    apiKey = res.body.api_key;
    tenantId = res.body.id;
  });

  afterAll(async () => {
    await admin?.end();
    await app?.close();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  it('creates an alert rule', async () => {
    const res = await request(app.server)
      .post('/v1/alerts/rules')
      .set('x-api-key', apiKey)
      .send({
        name: 'High error rate',
        metric: 'error_rate',
        operator: '>',
        threshold: 5,
        window_minutes: 10,
        endpoint_filter: '/api/checkout',
        webhook_url: 'https://example.com/webhook',
      });
    expect(res.status).toBe(201);
    expect(res.body.id).toBeDefined();
    expect(res.body.tenant_id).toBe(tenantId);
    ruleId = res.body.id;
  });

  it('rejects http:// webhook URLs', async () => {
    const res = await request(app.server)
      .post('/v1/alerts/rules')
      .set('x-api-key', apiKey)
      .send({
        name: 'insecure',
        metric: 'request_volume',
        operator: '>',
        threshold: 1,
        window_minutes: 5,
        webhook_url: 'http://example.com/webhook',
      });
    expect(res.status).toBe(400);
  });

  it('rejects invalid metric / window', async () => {
    const bad = await request(app.server)
      .post('/v1/alerts/rules')
      .set('x-api-key', apiKey)
      .send({
        name: 'bad',
        metric: 'cpu_usage',
        operator: '>',
        threshold: 1,
        window_minutes: 7,
        webhook_url: 'https://example.com/webhook',
      });
    expect(bad.status).toBe(400);
  });

  it('lists rules for the tenant only', async () => {
    const res = await request(app.server)
      .get('/v1/alerts/rules')
      .set('x-api-key', apiKey);
    expect(res.status).toBe(200);
    expect(res.body.rules.length).toBeGreaterThanOrEqual(1);
    expect(res.body.rules.every((r: { tenant_id: string }) => r.tenant_id === tenantId)).toBe(true);

    // a different tenant sees none of these rules
    const other = await request(app.server).post('/v1/tenants').send({ name: 'alert-other' });
    const otherList = await request(app.server)
      .get('/v1/alerts/rules')
      .set('x-api-key', other.body.api_key);
    expect(otherList.body.rules.length).toBe(0);
  });

  it('evaluate_alert_rules() fires when the threshold is breached', async () => {
    // Ingest 10 events on /api/checkout, 6 of them errors => 60% error rate > 5.
    const events = Array.from({ length: 10 }, (_, i) => ({
      endpoint: '/api/checkout',
      method: 'POST' as const,
      status_code: i < 6 ? 500 : 200,
      latency_ms: 20,
    }));
    await request(app.server)
      .post('/v1/events/batch')
      .set('x-api-key', apiKey)
      .send({ events });

    const { rows } = await admin.query<{ rules_evaluated: number; rules_fired: number }>(
      'SELECT * FROM evaluate_alert_rules()',
    );
    expect(Number(rows[0].rules_fired)).toBeGreaterThanOrEqual(1);

    // an alert_event now exists for our rule
    const ev = await admin.query(
      'SELECT observed_value, threshold FROM alert_events WHERE rule_id = $1',
      [ruleId],
    );
    expect(ev.rows.length).toBeGreaterThanOrEqual(1);
    expect(Number(ev.rows[0].observed_value)).toBeGreaterThan(5);
  });

  it('respects cooldown: a second immediate evaluation does not re-fire', async () => {
    const before = await admin.query<{ c: string }>(
      'SELECT COUNT(*)::int AS c FROM alert_events WHERE rule_id = $1',
      [ruleId],
    );
    await admin.query('SELECT * FROM evaluate_alert_rules()');
    const after = await admin.query<{ c: string }>(
      'SELECT COUNT(*)::int AS c FROM alert_events WHERE rule_id = $1',
      [ruleId],
    );
    expect(Number(after.rows[0].c)).toBe(Number(before.rows[0].c));
  });

  it('lists alert events for the tenant', async () => {
    const res = await request(app.server)
      .get('/v1/alerts/events?limit=10')
      .set('x-api-key', apiKey);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body.events)).toBe(true);
    expect(res.body.events[0]).toHaveProperty('rule_name', 'High error rate');
    expect(res.body.events[0]).toHaveProperty('observed_value');
  });

  it('deletes a rule scoped to the tenant', async () => {
    // another tenant cannot delete our rule
    const other = await request(app.server).post('/v1/tenants').send({ name: 'alert-thief' });
    const steal = await request(app.server)
      .delete(`/v1/alerts/rules/${ruleId}`)
      .set('x-api-key', other.body.api_key);
    expect(steal.status).toBe(404);

    // owner can
    const del = await request(app.server)
      .delete(`/v1/alerts/rules/${ruleId}`)
      .set('x-api-key', apiKey);
    expect(del.status).toBe(204);
  });
});
