import type { Pool } from 'pg';
import process from 'node:process';
import { withTransaction } from '../db/pool.js';
import { parsePositiveInt } from '../middleware/config.js';
import {
  INITIAL_STATE,
  isBreached,
  nextState,
  type AlertState,
  type Transition,
} from './state-machine.js';

/**
 * Distributed-safe alert evaluation.
 *
 * Any number of workers (in any number of app instances) can run this loop.
 *
 *   1. CLAIM  (short tx): SELECT due rules FOR UPDATE SKIP LOCKED, stamp
 *      lease_owner / lease_until, commit. Locked rows are skipped, not waited
 *      on, and a leased rule is invisible to other workers until the lease
 *      expires (crash recovery).
 *   2. COMPUTE: run the metric query. No row lock is held, so a slow query
 *      does not block anyone.
 *   3. FINALIZE (one tx): re-lock the rule row, and proceed only if we still
 *      own an unexpired lease. Then update alert_state, and on a transition
 *      insert the alert_events row and the alert_outbox row, advance
 *      next_eval_at and release the lease. All or nothing.
 *
 * Step 3 is what makes double-firing impossible: a worker whose lease expired
 * and was re-claimed by someone else fails the ownership check and writes
 * nothing.
 */

export interface EvaluatorConfig {
  workerId: string;
  batchSize: number;
  leaseMs: number;
  /** How long after an evaluation the rule becomes due again. */
  evalIntervalMs: number;
}

export interface EvaluationResult {
  claimed: number;
  fired: number;
  resolved: number;
  /** Lease was lost before finalize; nothing was written. */
  lost: number;
  errors: number;
}

interface ClaimedRule {
  id: string;
  tenant_id: string;
  name: string;
  metric: 'error_rate' | 'p95_latency_ms' | 'request_volume';
  operator: '>' | '<';
  threshold: string;
  window_minutes: number;
  endpoint_filter: string | null;
  webhook_url: string;
  required_consecutive_breaches: number;
  recovery_evaluations: number;
}

export function evaluatorConfigFromEnv(workerId: string): EvaluatorConfig {
  const env = (k: string, d: number): number => parsePositiveInt(process.env[k], d);
  return {
    workerId,
    batchSize: env('ALERT_BATCH_SIZE', 20),
    leaseMs: env('ALERT_LEASE_MS', 60_000),
    evalIntervalMs: env('ALERT_EVAL_INTERVAL_MS', 60_000),
  };
}

async function claimDueRules(pool: Pool, cfg: EvaluatorConfig): Promise<ClaimedRule[]> {
  return withTransaction(pool, async (c) => {
    const { rows } = await c.query<ClaimedRule>(
      `WITH due AS (
         SELECT id
           FROM alert_rules
          WHERE enabled = true
            AND next_eval_at <= now()
            AND (lease_until IS NULL OR lease_until < now())
          ORDER BY next_eval_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED
       )
       UPDATE alert_rules r
          SET lease_until = now() + make_interval(secs => $2::float8),
              lease_owner = $3
         FROM due
        WHERE r.id = due.id
        RETURNING r.id, r.tenant_id, r.name, r.metric, r.operator, r.threshold,
                  r.window_minutes, r.endpoint_filter, r.webhook_url,
                  r.required_consecutive_breaches, r.recovery_evaluations`,
      [cfg.batchSize, cfg.leaseMs / 1000, cfg.workerId],
    );
    return rows;
  });
}

const METRIC_SQL: Record<ClaimedRule['metric'], string> = {
  error_rate: `SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE status_code >= 400)
                      / NULLIF(COUNT(*), 0), 2) AS v`,
  p95_latency_ms: `SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS v`,
  request_volume: `SELECT COUNT(*) AS v`,
};

/** Metric over the rule's window from raw events. null = no data. */
async function computeMetric(pool: Pool, rule: ClaimedRule): Promise<number | null> {
  return withTransaction(
    pool,
    async (c) => {
      const { rows } = await c.query<{ v: string | null }>(
        `${METRIC_SQL[rule.metric]}
           FROM events
          WHERE tenant_id = $1
            AND ingested_at > now() - make_interval(mins => $2::int)
            AND ($3::text IS NULL OR endpoint = $3)`,
        [rule.tenant_id, rule.window_minutes, rule.endpoint_filter],
      );
      const v = rows[0]?.v;
      return v === null || v === undefined ? null : Number(v);
    },
    { tenantId: rule.tenant_id, readOnly: true },
  );
}

type FinalizeOutcome = { kind: 'lost' } | { kind: 'done'; transition: Transition };

async function finalizeRule(
  pool: Pool,
  cfg: EvaluatorConfig,
  rule: ClaimedRule,
  observed: number | null,
): Promise<FinalizeOutcome> {
  return withTransaction(pool, async (c) => {
    // Re-lock and verify we still hold an unexpired lease.
    const owned = await c.query(
      `SELECT 1 FROM alert_rules
        WHERE id = $1 AND lease_owner = $2 AND lease_until > now()
        FOR UPDATE`,
      [rule.id, cfg.workerId],
    );
    if (owned.rowCount === 0) return { kind: 'lost' };

    await c.query(
      `INSERT INTO alert_state (rule_id) VALUES ($1) ON CONFLICT (rule_id) DO NOTHING`,
      [rule.id],
    );
    const st = await c.query<{ state: AlertState['state']; breach_count: number; ok_count: number }>(
      `SELECT state, breach_count, ok_count FROM alert_state WHERE rule_id = $1 FOR UPDATE`,
      [rule.id],
    );
    const prev: AlertState = st.rows[0]
      ? {
          state: st.rows[0].state,
          breachCount: st.rows[0].breach_count,
          okCount: st.rows[0].ok_count,
        }
      : INITIAL_STATE;

    const threshold = Number(rule.threshold);
    // No data in the window counts as "not breaching".
    const breached = observed !== null && isBreached(rule.operator, observed, threshold);
    const { next, transition } = nextState(prev, breached, {
      requiredBreaches: rule.required_consecutive_breaches,
      recoveryEvaluations: rule.recovery_evaluations,
    });

    await c.query(
      `UPDATE alert_state
          SET state = $2, breach_count = $3, ok_count = $4, last_value = $5,
              changed_at = CASE WHEN $6::boolean THEN now() ELSE changed_at END
        WHERE rule_id = $1`,
      [rule.id, next.state, next.breachCount, next.okCount, observed, transition !== null],
    );

    if (transition) {
      const ev = await c.query<{ id: string; fired_at: Date }>(
        `INSERT INTO alert_events
           (rule_id, tenant_id, metric, observed_value, threshold, endpoint, event_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, fired_at`,
        [rule.id, rule.tenant_id, rule.metric, observed, threshold, rule.endpoint_filter, transition],
      );
      const payload = {
        event_type: transition,
        alert_event_id: ev.rows[0].id,
        alert_rule_id: rule.id,
        rule_name: rule.name,
        tenant_id: rule.tenant_id,
        metric: rule.metric,
        observed_value: observed,
        threshold,
        operator: rule.operator,
        endpoint: rule.endpoint_filter,
        fired_at: ev.rows[0].fired_at.toISOString(),
      };
      await c.query(
        `INSERT INTO alert_outbox
           (alert_event_id, rule_id, tenant_id, event_type, payload, webhook_url)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6)`,
        [ev.rows[0].id, rule.id, rule.tenant_id, transition, JSON.stringify(payload), rule.webhook_url],
      );
    }

    await c.query(
      `UPDATE alert_rules
          SET next_eval_at = now() + make_interval(secs => $2::float8),
              lease_until = NULL, lease_owner = NULL
        WHERE id = $1`,
      [rule.id, cfg.evalIntervalMs / 1000],
    );
    return { kind: 'done', transition };
  });
}

/** Claim and evaluate one batch of due rules. */
export async function evaluateDueRules(
  pool: Pool,
  cfg: EvaluatorConfig,
  log: (msg: string) => void = () => {},
): Promise<EvaluationResult> {
  const result: EvaluationResult = { claimed: 0, fired: 0, resolved: 0, lost: 0, errors: 0 };
  const rules = await claimDueRules(pool, cfg);
  result.claimed = rules.length;

  for (const rule of rules) {
    try {
      const observed = await computeMetric(pool, rule);
      const out = await finalizeRule(pool, cfg, rule, observed);
      if (out.kind === 'lost') result.lost += 1;
      else if (out.transition === 'FIRING') result.fired += 1;
      else if (out.transition === 'RESOLVED') result.resolved += 1;
    } catch (err) {
      // The lease stays in place and expires, which retries the rule later.
      result.errors += 1;
      log(
        `alert evaluator: rule ${rule.id} failed: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }
  }
  return result;
}
