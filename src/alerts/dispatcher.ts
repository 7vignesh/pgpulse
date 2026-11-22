import type { Pool } from 'pg';
import process from 'node:process';
import { withTransaction } from '../db/pool.js';
import { parsePositiveInt } from '../middleware/config.js';
import { isHttpsUrl } from './validation.js';
import { backoffDelayMs } from './backoff.js';

/**
 * Outbox dispatcher: delivers alert_outbox rows to tenant webhooks.
 *
 * Any number of dispatchers can run. A row is claimed with
 * FOR UPDATE SKIP LOCKED plus a lease, delivered outside the claim
 * transaction, and then marked sent / rescheduled / dead only if the worker
 * still owns the lease.
 *
 * Delivery is at-least-once: if a worker dies after the POST but before the
 * row is marked, the row is re-delivered once its lease expires. Every payload
 * carries `alert_event_id`, which receivers can use to deduplicate.
 */

export interface DispatcherConfig {
  workerId: string;
  batchSize: number;
  /** Must comfortably exceed webhookTimeoutMs. */
  leaseMs: number;
  maxAttempts: number;
  webhookTimeoutMs: number;
  backoffBaseMs: number;
  backoffCapMs: number;
}

export interface DispatchResult {
  claimed: number;
  sent: number;
  retried: number;
  dead: number;
  /** Lease lost before the result could be recorded. */
  lost: number;
}

export type FetchLike = (
  url: string,
  init: { method: string; headers: Record<string, string>; body: string; signal: AbortSignal },
) => Promise<{ status: number; ok: boolean; text(): Promise<string> }>;

interface ClaimedRow {
  id: string;
  alert_event_id: string;
  payload: unknown;
  webhook_url: string;
  attempts: number;
}

// Only the first slice of a response is stored.
const MAX_STORED_RESPONSE = 500;

export function dispatcherConfigFromEnv(workerId: string): DispatcherConfig {
  const env = (k: string, d: number): number => parsePositiveInt(process.env[k], d);
  return {
    workerId,
    batchSize: env('ALERT_DISPATCH_BATCH_SIZE', 20),
    leaseMs: env('ALERT_DISPATCH_LEASE_MS', 60_000),
    maxAttempts: env('ALERT_MAX_ATTEMPTS', 8),
    webhookTimeoutMs: env('ALERT_WEBHOOK_TIMEOUT_MS', 5_000),
    backoffBaseMs: env('ALERT_BACKOFF_BASE_MS', 1_000),
    backoffCapMs: env('ALERT_BACKOFF_CAP_MS', 15 * 60_000),
  };
}

async function claim(pool: Pool, cfg: DispatcherConfig): Promise<ClaimedRow[]> {
  return withTransaction(pool, async (c) => {
    const { rows } = await c.query<ClaimedRow>(
      `WITH due AS (
         SELECT id
           FROM alert_outbox
          WHERE status = 'pending'
            AND next_attempt_at <= now()
            AND (lease_until IS NULL OR lease_until < now())
          ORDER BY next_attempt_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED
       )
       UPDATE alert_outbox o
          SET lease_until = now() + make_interval(secs => $2::float8),
              lease_owner = $3
         FROM due
        WHERE o.id = due.id
        RETURNING o.id, o.alert_event_id, o.payload, o.webhook_url, o.attempts`,
      [cfg.batchSize, cfg.leaseMs / 1000, cfg.workerId],
    );
    return rows;
  });
}

type Outcome =
  | { kind: 'ok'; status: number; body: string }
  | { kind: 'fail'; status: number | null; body: string; permanent: boolean };

async function attempt(
  row: ClaimedRow,
  cfg: DispatcherConfig,
  doFetch: FetchLike,
): Promise<Outcome> {
  // Re-validate: the URL was checked on rule creation, this is defence in depth.
  if (!isHttpsUrl(row.webhook_url)) {
    return { kind: 'fail', status: null, body: 'rejected: webhook_url is not a valid https URL', permanent: true };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), cfg.webhookTimeoutMs);
  try {
    const res = await doFetch(row.webhook_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-agent': 'pgpulse-webhook/1.0' },
      body: JSON.stringify(row.payload),
      signal: controller.signal,
    });
    let body = '';
    try {
      body = (await res.text()).slice(0, MAX_STORED_RESPONSE);
    } catch {
      body = '';
    }
    return res.ok
      ? { kind: 'ok', status: res.status, body }
      : { kind: 'fail', status: res.status, body, permanent: false };
  } catch (err) {
    return {
      kind: 'fail',
      status: null,
      body: err instanceof Error ? err.message : 'webhook delivery failed',
      permanent: false,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function record(
  pool: Pool,
  cfg: DispatcherConfig,
  row: ClaimedRow,
  out: Outcome,
  random: () => number,
): Promise<'sent' | 'retried' | 'dead' | 'lost'> {
  // Postgres TEXT cannot hold NUL; strip it so a hostile response body cannot
  // make this write fail and leave the row stuck.
  const body = out.body.replace(/\u0000/g, '').slice(0, MAX_STORED_RESPONSE);
  const attempts = row.attempts + 1;

  let status: 'sent' | 'pending' | 'dead';
  let delayMs = 0;
  if (out.kind === 'ok') status = 'sent';
  else if (out.permanent || attempts >= cfg.maxAttempts) status = 'dead';
  else {
    status = 'pending';
    delayMs = backoffDelayMs(attempts, random, cfg.backoffBaseMs, cfg.backoffCapMs);
  }

  return withTransaction(pool, async (c) => {
    const upd = await c.query(
      `UPDATE alert_outbox
          SET status = $3,
              attempts = $4,
              next_attempt_at = now() + make_interval(secs => $5::float8),
              last_error = $6,
              delivered_at = CASE WHEN $3 = 'sent' THEN now() ELSE delivered_at END,
              lease_until = NULL,
              lease_owner = NULL
        WHERE id = $1 AND lease_owner = $2 AND status = 'pending'`,
      [
        row.id,
        cfg.workerId,
        status,
        attempts,
        delayMs / 1000,
        out.kind === 'ok' ? null : `${out.status ?? 'error'}: ${body}`.slice(0, MAX_STORED_RESPONSE),
      ],
    );
    if (upd.rowCount === 0) return 'lost';

    // Mirror the latest attempt onto the event history shown by /v1/alerts/events.
    await c.query(
      `UPDATE alert_events
          SET webhook_status = $2, webhook_response = $3, webhook_fired_at = now()
        WHERE id = $1`,
      [row.alert_event_id, out.status, body],
    );
    return status === 'pending' ? 'retried' : status;
  });
}

/** Claim and deliver one batch of due outbox rows. */
export async function dispatchDue(
  pool: Pool,
  cfg: DispatcherConfig,
  doFetch: FetchLike = fetch as unknown as FetchLike,
  log: (msg: string) => void = () => {},
  random: () => number = Math.random,
): Promise<DispatchResult> {
  const result: DispatchResult = { claimed: 0, sent: 0, retried: 0, dead: 0, lost: 0 };
  const rows = await claim(pool, cfg);
  result.claimed = rows.length;

  // Rows are independent; isolate failures so one bad webhook cannot stall the batch.
  await Promise.allSettled(
    rows.map(async (row) => {
      try {
        const out = await attempt(row, cfg, doFetch);
        const r = await record(pool, cfg, row, out, random);
        result[r] += 1;
      } catch (err) {
        // Could not record the result (DB down). The lease expires and the row is retried.
        log(
          `alert dispatcher: row ${row.id} not recorded: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    }),
  );
  return result;
}
