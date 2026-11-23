# 0011. Distributed alerting: SKIP LOCKED leases, hysteresis, outbox

Status: Accepted. Supersedes [0005](0005-alerting-in-postgres-pg-cron.md).

## Context

ADR 0005 evaluated rules in one PL/pgSQL pass from pg_cron, with no row
locking, no hysteresis, and a fire-and-forget webhook poller. Concurrent runs
could fire a rule twice, two app instances could deliver an event twice, and a
failed webhook was never retried.

## Decision

- Evaluator (`src/alerts/evaluator.ts`), any number of workers: claim due rules
  with `FOR UPDATE SKIP LOCKED` and a lease, compute the metric with no lock
  held, then finalize in one transaction that first checks the worker still
  owns an unexpired lease.
- Hysteresis (`src/alerts/state-machine.ts`): `FIRING` after
  `required_consecutive_breaches` breaching evaluations, `RESOLVED` after
  `recovery_evaluations` clean ones.
- Transactional outbox: the state change, the `alert_events` history row and
  the `alert_outbox` row commit together.
- Dispatcher (`src/alerts/dispatcher.ts`): `SKIP LOCKED` claim with a lease,
  https-only POST, exponential backoff with jitter, `dead` after
  `ALERT_MAX_ATTEMPTS`. The pg_cron job and `evaluate_alert_rules()` are
  removed (migration `008`).

## Consequences

- No double fire and no double delivery across workers (tested with 6
  evaluators and 5 dispatchers).
- Evaluation now needs a running app instance; it no longer runs inside
  Postgres while the app is down. Run 2 or more instances.
- Delivery is at-least-once: payloads carry `alert_event_id` for receiver-side
  deduplication.
- A rule that stays in breach fires once instead of once per window.
