-- ===========================================================================
-- 008_alert_state_outbox.sql
-- Distributed-safe alerting.
--
--   * alert_rules gains hysteresis settings plus a lease (next_eval_at,
--     lease_until, lease_owner) so many workers can evaluate concurrently:
--     they claim due rules with FOR UPDATE SKIP LOCKED and hold a lease while
--     they compute.
--   * alert_state holds the per-rule OK/FIRING state machine.
--   * alert_outbox is written in the SAME transaction as the state change, and
--     drained by SKIP LOCKED dispatcher workers (retry with backoff, dead-letter).
--   * The pg_cron job and evaluate_alert_rules() from 006 are removed:
--     evaluation now runs in the app (src/alerts/evaluator.ts).
-- ===========================================================================

ALTER TABLE alert_rules
  ADD COLUMN IF NOT EXISTS required_consecutive_breaches INTEGER NOT NULL DEFAULT 1
    CHECK (required_consecutive_breaches BETWEEN 1 AND 60),
  ADD COLUMN IF NOT EXISTS recovery_evaluations INTEGER NOT NULL DEFAULT 1
    CHECK (recovery_evaluations BETWEEN 1 AND 60),
  ADD COLUMN IF NOT EXISTS next_eval_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS lease_until  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS lease_owner  TEXT;

-- Claim scan: enabled rules ordered by when they are due.
CREATE INDEX IF NOT EXISTS idx_alert_rules_due
  ON alert_rules (next_eval_at) WHERE enabled = true;

-- ---------------------------------------------------------------------------
-- Per-rule state machine.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_state (
  rule_id      UUID PRIMARY KEY REFERENCES alert_rules(id) ON DELETE CASCADE,
  state        TEXT NOT NULL DEFAULT 'OK' CHECK (state IN ('OK', 'FIRING')),
  breach_count INTEGER NOT NULL DEFAULT 0,
  ok_count     INTEGER NOT NULL DEFAULT 0,
  last_value   NUMERIC,
  changed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE alert_state IS
  'OK/FIRING state per alert rule. breach_count and ok_count drive hysteresis.';

-- alert_events is now the history of transitions (FIRING and RESOLVED).
ALTER TABLE alert_events
  ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'FIRING'
    CHECK (event_type IN ('FIRING', 'RESOLVED'));

-- ---------------------------------------------------------------------------
-- Transactional outbox for webhook delivery.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_outbox (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  alert_event_id  UUID NOT NULL REFERENCES alert_events(id) ON DELETE CASCADE,
  rule_id         UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
  tenant_id       UUID NOT NULL,
  event_type      TEXT NOT NULL CHECK (event_type IN ('FIRING', 'RESOLVED')),
  payload         JSONB NOT NULL,
  webhook_url     TEXT NOT NULL,                 -- snapshot at emit time
  status          TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'sent', 'dead')),
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until     TIMESTAMPTZ,
  lease_owner     TEXT,
  last_error      TEXT,
  delivered_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dispatcher claim scan.
CREATE INDEX IF NOT EXISTS idx_alert_outbox_pending
  ON alert_outbox (next_attempt_at) WHERE status = 'pending';

COMMENT ON TABLE alert_outbox IS
  'Webhook deliveries. Inserted atomically with the alert_state change; status pending -> sent | dead.';

-- ---------------------------------------------------------------------------
-- Carry over in-flight state from the old model so an upgrade neither drops
-- undelivered webhooks nor re-announces alerts that are already firing.
-- ---------------------------------------------------------------------------
INSERT INTO alert_outbox
  (alert_event_id, rule_id, tenant_id, event_type, payload, webhook_url)
SELECT e.id, e.rule_id, e.tenant_id, 'FIRING',
       jsonb_build_object(
         'event_type', 'FIRING',
         'alert_event_id', e.id,
         'alert_rule_id', r.id,
         'rule_name', r.name,
         'tenant_id', e.tenant_id,
         'metric', e.metric,
         'observed_value', e.observed_value,
         'threshold', e.threshold,
         'operator', r.operator,
         'endpoint', e.endpoint,
         'fired_at', e.fired_at),
       r.webhook_url
  FROM alert_events e
  JOIN alert_rules r ON r.id = e.rule_id
 WHERE e.webhook_fired_at IS NULL;

INSERT INTO alert_state (rule_id, state, breach_count, changed_at)
SELECT r.id, 'FIRING', r.required_consecutive_breaches, MAX(e.fired_at)
  FROM alert_rules r
  JOIN alert_events e ON e.rule_id = r.id
 GROUP BY r.id
HAVING MAX(e.fired_at) > now() - (r.window_minutes || ' minutes')::INTERVAL
ON CONFLICT (rule_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Retire the single-scheduler evaluator and the old delivery-queue index.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'evaluate-alert-rules') THEN
    PERFORM cron.unschedule('evaluate-alert-rules');
  END IF;
END
$$;

DROP FUNCTION IF EXISTS evaluate_alert_rules();
DROP INDEX IF EXISTS idx_alert_events_undelivered;
