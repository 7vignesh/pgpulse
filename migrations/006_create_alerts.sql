-- ===========================================================================
-- 006_create_alerts.sql
-- Alert system: tenant-defined rules, fired-event history, an in-database
-- evaluation function, and a pg_cron job that runs it every minute.
--
-- Design note: evaluation lives in Postgres (PL/pgSQL + pg_cron) so it runs
-- even when the Fastify app is down, evaluates atomically, and has no app/DB
-- race. Webhook *delivery* is done by the app (pg_cron cannot make HTTP calls
-- without pg_net); see docs/SCALING.md "Alert System".
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Alert rules: what each tenant wants to be alerted on.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_rules (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name            TEXT NOT NULL,
  metric          TEXT NOT NULL CHECK (metric IN ('error_rate', 'p95_latency_ms', 'request_volume')),
  operator        TEXT NOT NULL CHECK (operator IN ('>', '<')),
  threshold       NUMERIC NOT NULL,
  window_minutes  INTEGER NOT NULL DEFAULT 10 CHECK (window_minutes IN (5, 10, 15, 30, 60)),
  endpoint_filter TEXT DEFAULT NULL,                 -- NULL means all endpoints
  webhook_url     TEXT NOT NULL,
  enabled         BOOLEAN NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only enabled rules are ever scanned by the evaluator; partial index keeps
-- the hot path tight.
CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant
  ON alert_rules (tenant_id) WHERE enabled = true;

COMMENT ON TABLE alert_rules IS 'Tenant-defined alert thresholds on live event metrics.';

-- ---------------------------------------------------------------------------
-- Alert events: one row per firing. Used for cooldown + audit history and as
-- the webhook delivery queue (webhook_fired_at IS NULL => not yet delivered).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_events (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  rule_id          UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
  tenant_id        UUID NOT NULL,
  metric           TEXT NOT NULL,
  observed_value   NUMERIC NOT NULL,
  threshold        NUMERIC NOT NULL,
  endpoint         TEXT,
  fired_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  webhook_status   INTEGER,        -- HTTP status from the webhook call
  webhook_response TEXT,           -- first 500 chars of response body / error
  webhook_fired_at TIMESTAMPTZ     -- when delivery was attempted (success or fail)
);

CREATE INDEX IF NOT EXISTS idx_alert_events_rule_fired
  ON alert_events (rule_id, fired_at DESC);

-- Delivery queue scan: undelivered events oldest-first.
CREATE INDEX IF NOT EXISTS idx_alert_events_undelivered
  ON alert_events (fired_at ASC) WHERE webhook_fired_at IS NULL;

COMMENT ON TABLE alert_events IS 'History of fired alerts; also the webhook delivery queue.';

-- ---------------------------------------------------------------------------
-- evaluate_alert_rules(): the core. Runs every minute via pg_cron.
--   * loops enabled rules
--   * computes the rule metric over its window from RAW events (real-time)
--   * applies a window-based cooldown to avoid spam
--   * inserts an alert_events row when the condition is met
--   * returns (rules_evaluated, rules_fired)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION evaluate_alert_rules()
RETURNS TABLE (rules_evaluated INT, rules_fired INT)
LANGUAGE plpgsql
AS $$
DECLARE
  rule        RECORD;
  observed    NUMERIC;
  last_fired  TIMESTAMPTZ;
  fired_count INT := 0;
  eval_count  INT := 0;
BEGIN
  FOR rule IN
    SELECT * FROM alert_rules WHERE enabled = true
  LOOP
    eval_count := eval_count + 1;

    -- Cooldown: skip if this rule already fired within its own window.
    SELECT MAX(fired_at) INTO last_fired
    FROM alert_events
    WHERE rule_id = rule.id;

    IF last_fired IS NOT NULL
       AND last_fired > now() - (rule.window_minutes || ' minutes')::INTERVAL THEN
      CONTINUE;
    END IF;

    -- Compute the metric over the rule's window from raw events.
    IF rule.metric = 'error_rate' THEN
      SELECT ROUND(
               100.0 * COUNT(*) FILTER (WHERE status_code >= 400)
               / NULLIF(COUNT(*), 0), 2)
        INTO observed
        FROM events
       WHERE tenant_id = rule.tenant_id
         AND ingested_at > now() - (rule.window_minutes || ' minutes')::INTERVAL
         AND (rule.endpoint_filter IS NULL OR endpoint = rule.endpoint_filter);

    ELSIF rule.metric = 'p95_latency_ms' THEN
      SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)
        INTO observed
        FROM events
       WHERE tenant_id = rule.tenant_id
         AND ingested_at > now() - (rule.window_minutes || ' minutes')::INTERVAL
         AND (rule.endpoint_filter IS NULL OR endpoint = rule.endpoint_filter);

    ELSIF rule.metric = 'request_volume' THEN
      SELECT COUNT(*)
        INTO observed
        FROM events
       WHERE tenant_id = rule.tenant_id
         AND ingested_at > now() - (rule.window_minutes || ' minutes')::INTERVAL
         AND (rule.endpoint_filter IS NULL OR endpoint = rule.endpoint_filter);
    END IF;

    -- No data in the window => nothing to evaluate.
    IF observed IS NULL THEN
      CONTINUE;
    END IF;

    IF (rule.operator = '>' AND observed > rule.threshold) OR
       (rule.operator = '<' AND observed < rule.threshold) THEN

      INSERT INTO alert_events (
        rule_id, tenant_id, metric, observed_value, threshold, endpoint
      ) VALUES (
        rule.id, rule.tenant_id, rule.metric, observed, rule.threshold, rule.endpoint_filter
      );

      fired_count := fired_count + 1;
    END IF;
  END LOOP;

  RETURN QUERY SELECT eval_count, fired_count;
END;
$$;

COMMENT ON FUNCTION evaluate_alert_rules() IS
  'Evaluate all enabled alert rules over their windows; insert alert_events on breach. Run every minute by pg_cron.';

-- ---------------------------------------------------------------------------
-- Schedule: evaluate every minute. Webhook delivery is handled by the app's
-- 60s poll worker (pg_cron has no native HTTP).
-- ---------------------------------------------------------------------------
SELECT cron.schedule(
  'evaluate-alert-rules',
  '* * * * *',
  'SELECT * FROM evaluate_alert_rules()'
);
