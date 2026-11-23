# 0005. Alert evaluation in Postgres via pg_cron

Status: Superseded by [0011](0011-distributed-alerting-outbox.md)

## Context

Tenants set threshold rules (error rate, p95 latency, volume) over a time
window. Alerting has to keep working during an incident, and webhook delivery
needs HTTP, which Postgres cannot do without `pg_net`.

## Decision

`evaluate_alert_rules()` (PL/pgSQL, `migrations/006`) runs every minute from
pg_cron. It computes each enabled rule's metric from raw `events`, applies a
cooldown of `window_minutes`, and inserts `alert_events` rows. An in-app worker
(`src/alerts/webhook.ts`) polls undelivered rows every 60 s and POSTs them,
https-only with a 5 s timeout.

## Consequences

- Evaluation continues while the app is down; only delivery waits.
- There is no row locking, so two concurrent evaluations can fire one rule
  twice, and two app instances can deliver one event twice.
- No hysteresis: a metric hovering at the threshold re-fires every window, and
  there is no "resolved" notification.
- Delivery is fire-and-forget: a failed webhook is recorded and never retried.
