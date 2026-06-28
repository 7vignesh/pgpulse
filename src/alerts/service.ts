import { primaryPool } from '../db/pool.js';

/**
 * Alert rules + events data access. All logic lives here; routes stay thin.
 * Every query filters by tenant_id so no tenant can see another's alerts.
 */

export type AlertMetric = 'error_rate' | 'p95_latency_ms' | 'request_volume';
export type AlertOperator = '>' | '<';
export type AlertWindow = 5 | 10 | 15 | 30 | 60;

export interface AlertRule {
  id: string;
  tenant_id: string;
  name: string;
  metric: AlertMetric;
  operator: AlertOperator;
  threshold: number;
  window_minutes: AlertWindow;
  endpoint_filter: string | null;
  webhook_url: string;
  enabled: boolean;
  created_at: string;
}

export interface CreateRuleInput {
  name: string;
  metric: AlertMetric;
  operator: AlertOperator;
  threshold: number;
  window_minutes: AlertWindow;
  endpoint_filter?: string | null;
  webhook_url: string;
}

const RULE_COLUMNS =
  'id, tenant_id, name, metric, operator, threshold, window_minutes, ' +
  'endpoint_filter, webhook_url, enabled, created_at';

export async function createRule(
  tenantId: string,
  input: CreateRuleInput,
): Promise<AlertRule> {
  const { rows } = await primaryPool.query<AlertRule>(
    `INSERT INTO alert_rules
       (tenant_id, name, metric, operator, threshold, window_minutes,
        endpoint_filter, webhook_url)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     RETURNING ${RULE_COLUMNS}`,
    [
      tenantId,
      input.name,
      input.metric,
      input.operator,
      input.threshold,
      input.window_minutes,
      input.endpoint_filter ?? null,
      input.webhook_url,
    ],
  );
  return rows[0];
}

export async function listRules(tenantId: string): Promise<AlertRule[]> {
  const { rows } = await primaryPool.query<AlertRule>(
    `SELECT ${RULE_COLUMNS}
       FROM alert_rules
      WHERE tenant_id = $1
      ORDER BY created_at DESC`,
    [tenantId],
  );
  return rows;
}

/**
 * Delete a rule, scoped to the tenant. Returns true if a row was deleted.
 * The tenant_id filter guarantees a tenant cannot delete another's rule.
 */
export async function deleteRule(tenantId: string, ruleId: string): Promise<boolean> {
  const res = await primaryPool.query(
    `DELETE FROM alert_rules WHERE id = $1 AND tenant_id = $2`,
    [ruleId, tenantId],
  );
  return (res.rowCount ?? 0) > 0;
}

export interface AlertEventRow {
  id: string;
  rule_id: string;
  rule_name: string;
  metric: string;
  observed_value: number;
  threshold: number;
  endpoint: string | null;
  fired_at: string;
  webhook_status: number | null;
  webhook_fired_at: string | null;
}

export async function listEvents(
  tenantId: string,
  limit: number,
): Promise<AlertEventRow[]> {
  const { rows } = await primaryPool.query<AlertEventRow>(
    `SELECT e.id, e.rule_id, r.name AS rule_name, e.metric,
            e.observed_value, e.threshold, e.endpoint, e.fired_at,
            e.webhook_status, e.webhook_fired_at
       FROM alert_events e
       JOIN alert_rules r ON r.id = e.rule_id
      WHERE e.tenant_id = $1
      ORDER BY e.fired_at DESC
      LIMIT $2`,
    [tenantId, limit],
  );
  return rows.map((r) => ({
    ...r,
    observed_value: Number(r.observed_value),
    threshold: Number(r.threshold),
    webhook_status: r.webhook_status === null ? null : Number(r.webhook_status),
  }));
}
