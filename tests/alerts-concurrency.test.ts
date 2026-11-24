import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';

/**
 * Concurrency tests for the alert evaluator and outbox dispatcher against a
 * real Postgres. The point: with several workers running at once, a breach is
 * announced exactly once and an outbox row is delivered exactly once.
 *
 * Skipped when no DB is reachable (see integration.test.ts for setup).
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

type Evaluator = typeof import('../src/alerts/evaluator.js');
type Dispatcher = typeof import('../src/alerts/dispatcher.js');
type FetchLike = import('../src/alerts/dispatcher.js').FetchLike;

d('integration: distributed alerting', () => {
  let admin: pg.Client;
  let pool: pg.Pool;
  let ev: Evaluator;
  let disp: Dispatcher;
  let tenantId: string;
  const ruleIds: string[] = [];

  const worker = (kind: string, i: number) => `${kind}-${process.pid}-${i}`;
  const evalCfg = (i: number) => ({ ...ev.evaluatorConfigFromEnv(worker('e', i)), batchSize: 5 });
  const dispCfg = (i: number, over: Partial<ReturnType<Dispatcher['dispatcherConfigFromEnv']>> = {}) => ({
    ...disp.dispatcherConfigFromEnv(worker('d', i)),
    batchSize: 7,
    ...over,
  });

  const newRule = async (over: Record<string, unknown> = {}): Promise<string> => {
    const { rows } = await admin.query<{ id: string }>(
      `INSERT INTO alert_rules
         (tenant_id, name, metric, operator, threshold, window_minutes, webhook_url,
          required_consecutive_breaches, recovery_evaluations)
       VALUES ($1, $2, 'request_volume', '>', 0, 60, 'https://example.com/hook', $3, $4)
       RETURNING id`,
      [tenantId, `conc-${Math.random()}`, over.breaches ?? 1, over.recovery ?? 1],
    );
    ruleIds.push(rows[0].id);
    return rows[0].id;
  };

  const makeDue = (id: string) =>
    admin.query('UPDATE alert_rules SET next_eval_at = now(), lease_until = NULL, lease_owner = NULL WHERE id = $1', [id]);

  const outboxCount = async (ruleId: string, type?: string): Promise<number> => {
    const { rows } = await admin.query<{ c: number }>(
      `SELECT COUNT(*)::int AS c FROM alert_outbox WHERE rule_id = $1 AND ($2::text IS NULL OR event_type = $2)`,
      [ruleId, type ?? null],
    );
    return rows[0].c;
  };

  beforeAll(async () => {
    ev = await import('../src/alerts/evaluator.js');
    disp = await import('../src/alerts/dispatcher.js');
    admin = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await admin.connect();
    // Dedicated pool with enough connections for 6+ concurrent workers.
    pool = new pg.Pool({ connectionString: process.env.PRIMARY_DATABASE_URL, max: 20 });
    const t = await admin.query<{ id: string }>(
      `INSERT INTO tenants (name) VALUES ('alert-conc') RETURNING id`,
    );
    tenantId = t.rows[0].id;
    // Data so request_volume > 0 is breached.
    await admin.query(
      `INSERT INTO events (tenant_id, endpoint, method, status_code, latency_ms)
       SELECT $1, '/c', 'GET', 200, 5 FROM generate_series(1, 5)`,
      [tenantId],
    );
  });

  afterAll(async () => {
    if (tenantId) await admin.query('DELETE FROM tenants WHERE id = $1', [tenantId]).catch(() => {});
    await pool?.end();
    await admin?.end();
    const { closePools } = await import('../src/db/pool.js');
    await closePools();
  });

  it('6 concurrent evaluators over 25 due rules fire each rule exactly once', async () => {
    const ids: string[] = [];
    for (let i = 0; i < 25; i++) ids.push(await newRule());
    await Promise.all(ids.map(makeDue));

    const results = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        (async () => {
          // each worker keeps claiming until nothing is left for it
          let fired = 0;
          for (let round = 0; round < 10; round++) {
            const r = await ev.evaluateDueRules(pool, evalCfg(i));
            fired += r.fired;
            if (r.claimed === 0) break;
          }
          return fired;
        })(),
      ),
    );

    // Other due rules in a shared DB may also be evaluated, so only require
    // that all 25 of ours fired; the per-rule checks below prove exactly once.
    expect(results.reduce((a, b) => a + b, 0)).toBeGreaterThanOrEqual(25);
    for (const id of ids) {
      expect(await outboxCount(id, 'FIRING')).toBe(1);
    }
    // history rows match outbox rows 1:1
    const { rows } = await admin.query<{ c: number }>(
      'SELECT COUNT(*)::int AS c FROM alert_events WHERE rule_id = ANY($1)',
      [ids],
    );
    expect(rows[0].c).toBe(25);
  });

  it('a rule claimed by one worker is invisible to another until the lease expires', async () => {
    const id = await newRule();
    await makeDue(id);
    // Simulate worker A having claimed it.
    await admin.query(
      `UPDATE alert_rules SET lease_owner = 'crashed-worker', lease_until = now() + interval '1 hour' WHERE id = $1`,
      [id],
    );
    const blocked = await ev.evaluateDueRules(pool, evalCfg(1));
    expect(await outboxCount(id)).toBe(0);
    expect(blocked.fired).toBe(0);

    // Lease expires (crash recovery): another worker picks it up.
    await admin.query(`UPDATE alert_rules SET lease_until = now() - interval '1 second' WHERE id = $1`, [id]);
    await ev.evaluateDueRules(pool, evalCfg(2));
    expect(await outboxCount(id, 'FIRING')).toBe(1);
  });

  it('a worker that lost its lease writes nothing (no double fire after takeover)', async () => {
    const id = await newRule();
    await makeDue(id);
    // Worker A computes slowly: claim, then lose the lease to B before finalize.
    // Emulate by running A's claim, letting B run to completion, then A's finalize.
    const cfgA = evalCfg(10);
    const cfgB = evalCfg(11);
    const first = ev.evaluateDueRules(pool, { ...cfgA, leaseMs: 1 }); // lease expires almost immediately
    await new Promise((r) => setTimeout(r, 50));
    await ev.evaluateDueRules(pool, cfgB);
    // A's 1ms lease had expired by finalize time, so it must have written nothing.
    expect((await first).lost).toBeGreaterThanOrEqual(1);
    expect(await outboxCount(id, 'FIRING')).toBe(1);
  });

  it('hysteresis: needs N consecutive evaluations to fire, then M clean ones to resolve', async () => {
    const id = await newRule({ breaches: 3, recovery: 2 });
    const tick = async () => {
      await makeDue(id);
      await ev.evaluateDueRules(pool, evalCfg(20));
    };
    await tick();
    await tick();
    expect(await outboxCount(id)).toBe(0);
    await tick();
    expect(await outboxCount(id, 'FIRING')).toBe(1);
    await tick();
    expect(await outboxCount(id)).toBe(1);

    // make the metric clean: operator '<' 0 can never breach
    await admin.query(`UPDATE alert_rules SET operator = '<' WHERE id = $1`, [id]);
    await tick();
    expect(await outboxCount(id, 'RESOLVED')).toBe(0); // 1 of 2 clean
    await tick();
    expect(await outboxCount(id, 'RESOLVED')).toBe(1);
    const st = await admin.query('SELECT state FROM alert_state WHERE rule_id = $1', [id]);
    expect(st.rows[0].state).toBe('OK');
  });

  it('state change and outbox row commit atomically (forced failure rolls both back)', async () => {
    const id = await newRule();
    await makeDue(id);
    // Make the outbox insert fail: NOT NULL violation via a trigger-free trick.
    // webhook_url is snapshotted from the rule; a rule row cannot hold NULL there,
    // so instead block the insert with a failing constraint on alert_outbox.
    await admin.query(
      `ALTER TABLE alert_outbox ADD CONSTRAINT itest_block CHECK (rule_id <> '${id}'::uuid) NOT VALID`,
    );
    try {
      const r = await ev.evaluateDueRules(pool, evalCfg(30));
      expect(r.errors).toBeGreaterThanOrEqual(1);
    } finally {
      await admin.query('ALTER TABLE alert_outbox DROP CONSTRAINT itest_block');
    }
    const st = await admin.query('SELECT state, breach_count FROM alert_state WHERE rule_id = $1', [id]);
    // rolled back: either no row, or still OK with zero breaches
    expect(st.rows.length === 0 || (st.rows[0].state === 'OK' && st.rows[0].breach_count === 0)).toBe(true);
    const evs = await admin.query('SELECT COUNT(*)::int AS c FROM alert_events WHERE rule_id = $1', [id]);
    expect(evs.rows[0].c).toBe(0);
    expect(await outboxCount(id)).toBe(0);
  });

  describe('outbox dispatcher', () => {
    const deliveries = new Map<string, number>();
    const okFetch: FetchLike = async (_url, init) => {
      const key = (JSON.parse(init.body) as { alert_event_id: string }).alert_event_id;
      deliveries.set(key, (deliveries.get(key) ?? 0) + 1);
      await new Promise((r) => setTimeout(r, 5));
      return { status: 200, ok: true, text: async () => 'ok' };
    };

    const seedOutbox = async (n: number, url = 'https://example.com/hook'): Promise<string[]> => {
      const rid = await newRule();
      const ids: string[] = [];
      for (let i = 0; i < n; i++) {
        const e = await admin.query<{ id: string }>(
          `INSERT INTO alert_events (rule_id, tenant_id, metric, observed_value, threshold)
           VALUES ($1, $2, 'request_volume', 5, 0) RETURNING id`,
          [rid, tenantId],
        );
        await admin.query(
          `INSERT INTO alert_outbox (alert_event_id, rule_id, tenant_id, event_type, payload, webhook_url)
           VALUES ($1, $2, $3, 'FIRING', $4::jsonb, $5)`,
          [e.rows[0].id, rid, tenantId, JSON.stringify({ alert_event_id: e.rows[0].id }), url],
        );
        ids.push(e.rows[0].id);
      }
      return ids;
    };

    it('5 concurrent dispatchers deliver each of 40 rows exactly once', async () => {
      const ids = await seedOutbox(40);
      deliveries.clear();
      await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          (async () => {
            for (let round = 0; round < 10; round++) {
              const r = await disp.dispatchDue(pool, dispCfg(i), okFetch);
              if (r.claimed === 0) break;
            }
          })(),
        ),
      );
      for (const id of ids) expect(deliveries.get(id)).toBe(1);
      const { rows } = await admin.query<{ c: number }>(
        `SELECT COUNT(*)::int AS c FROM alert_outbox WHERE alert_event_id = ANY($1) AND status = 'sent'`,
        [ids],
      );
      expect(rows[0].c).toBe(40);
      // delivery status is mirrored onto the alert_events history row
      const hist = await admin.query(
        `SELECT webhook_status FROM alert_events WHERE id = $1`,
        [ids[0]],
      );
      expect(hist.rows[0].webhook_status).toBe(200);
    });

    it('failed deliveries are rescheduled with backoff, then dead-lettered at max attempts', async () => {
      const [id] = await seedOutbox(1);
      const failFetch: FetchLike = async () => ({ status: 500, ok: false, text: async () => 'boom' });
      const cfg = dispCfg(50, { maxAttempts: 3, backoffBaseMs: 1000, backoffCapMs: 60_000 });

      const r1 = await disp.dispatchDue(pool, cfg, failFetch);
      expect(r1.retried).toBe(1);
      let row = (await admin.query(
        `SELECT status, attempts, last_error, next_attempt_at > now() AS future FROM alert_outbox WHERE alert_event_id = $1`,
        [id],
      )).rows[0];
      expect(row.status).toBe('pending');
      expect(row.attempts).toBe(1);
      expect(row.future).toBe(true);
      expect(row.last_error).toMatch(/500/);

      // Not due yet: our row is left alone.
      await disp.dispatchDue(pool, cfg, failFetch);
      expect((await admin.query(`SELECT attempts FROM alert_outbox WHERE alert_event_id = $1`, [id])).rows[0].attempts).toBe(1);

      for (let i = 0; i < 2; i++) {
        await admin.query(`UPDATE alert_outbox SET next_attempt_at = now() WHERE alert_event_id = $1`, [id]);
        await disp.dispatchDue(pool, cfg, failFetch);
      }
      row = (await admin.query(`SELECT status, attempts FROM alert_outbox WHERE alert_event_id = $1`, [id])).rows[0];
      expect(row.status).toBe('dead');
      expect(row.attempts).toBe(3);

      // dead rows are never picked up again
      await admin.query(`UPDATE alert_outbox SET next_attempt_at = now() WHERE alert_event_id = $1`, [id]);
      await disp.dispatchDue(pool, cfg, failFetch);
      expect((await admin.query(`SELECT attempts FROM alert_outbox WHERE alert_event_id = $1`, [id])).rows[0].attempts).toBe(3);
    });

    it('a network error counts as a failed attempt', async () => {
      const [id] = await seedOutbox(1);
      const boom: FetchLike = async () => { throw new Error('ECONNREFUSED'); };
      const r = await disp.dispatchDue(pool, dispCfg(51), boom);
      expect(r.retried).toBe(1);
      const row = (await admin.query(`SELECT last_error FROM alert_outbox WHERE alert_event_id = $1`, [id])).rows[0];
      expect(row.last_error).toMatch(/ECONNREFUSED/);
    });

    it('non-https webhook urls are dead-lettered without a request', async () => {
      const [id] = await seedOutbox(1, 'http://insecure.example.com/x');
      let called = false;
      const spy: FetchLike = async () => { called = true; return { status: 200, ok: true, text: async () => '' }; };
      const r = await disp.dispatchDue(pool, dispCfg(52), spy);
      expect(r.dead).toBe(1);
      expect(called).toBe(false);
      const row = (await admin.query(`SELECT status FROM alert_outbox WHERE alert_event_id = $1`, [id])).rows[0];
      expect(row.status).toBe('dead');
    });

    it('an expired lease makes a row claimable again; a stale worker cannot overwrite the result', async () => {
      const [id] = await seedOutbox(1);
      await admin.query(
        `UPDATE alert_outbox SET lease_owner = 'crashed', lease_until = now() + interval '1 hour' WHERE alert_event_id = $1`,
        [id],
      );
      deliveries.clear();
      await disp.dispatchDue(pool, dispCfg(60), okFetch);
      expect(deliveries.get(id)).toBeUndefined();

      await admin.query(`UPDATE alert_outbox SET lease_until = now() - interval '1 second' WHERE alert_event_id = $1`, [id]);
      await disp.dispatchDue(pool, dispCfg(61), okFetch);
      expect(deliveries.get(id)).toBe(1);

      // A slow worker whose lease was taken over reports late: ownership check rejects it.
      const [id2] = await seedOutbox(1);
      const slowFetch: FetchLike = async () => {
        await admin.query(
          `UPDATE alert_outbox SET lease_owner = 'someone-else' WHERE alert_event_id = $1`, [id2]);
        return { status: 200, ok: true, text: async () => '' };
      };
      const lost = await disp.dispatchDue(pool, dispCfg(62), async (u, i) => {
        // Only take over our row; other rows in a shared DB are delivered normally.
        if ((JSON.parse(i.body) as { alert_event_id: string }).alert_event_id !== id2) return okFetch(u, i);
        return slowFetch(u, i);
      });
      expect(lost.lost).toBe(1);
      const row = (await admin.query(`SELECT status FROM alert_outbox WHERE alert_event_id = $1`, [id2])).rows[0];
      expect(row.status).toBe('pending');
    });
  });
});
