import { primaryPool } from '../db/pool.js';

/**
 * Webhook delivery worker.
 *
 * pg_cron evaluates rules and inserts alert_events, but Postgres cannot make
 * HTTP calls natively (would need pg_net). So delivery is the app's job: this
 * worker polls for undelivered alert_events and POSTs the payload to each
 * rule's webhook_url.
 *
 * Design:
 *   - Runs on a single setInterval (every 60s), started after pools are ready.
 *   - Fire-and-forget per event: success or failure, we stamp webhook_fired_at
 *     so it is never retried (infinite retry is a documented future
 *     improvement — see docs/SCALING.md).
 *   - Never throws into the event loop: the whole tick is wrapped in try/catch
 *     and individual deliveries are isolated, so a bad webhook can't crash the
 *     server.
 *   - 5s timeout per webhook via AbortController.
 *   - https-only is enforced at rule-creation time; we double-check here.
 */

const POLL_INTERVAL_MS = 60_000;
const BATCH_LIMIT = 50;
const WEBHOOK_TIMEOUT_MS = 5_000;

interface PendingDelivery {
  id: string;
  rule_id: string;
  rule_name: string;
  tenant_id: string;
  metric: string;
  observed_value: string;
  threshold: string;
  operator: string;
  endpoint: string | null;
  fired_at: string;
  webhook_url: string;
}

async function fetchPending(): Promise<PendingDelivery[]> {
  const { rows } = await primaryPool.query<PendingDelivery>(
    `SELECT e.id, e.rule_id, r.name AS rule_name, e.tenant_id, e.metric,
            e.observed_value, e.threshold, r.operator, e.endpoint, e.fired_at,
            r.webhook_url
       FROM alert_events e
       JOIN alert_rules r ON r.id = e.rule_id
      WHERE e.webhook_fired_at IS NULL
      ORDER BY e.fired_at ASC
      LIMIT $1`,
    [BATCH_LIMIT],
  );
  return rows;
}

async function markDelivered(
  eventId: string,
  status: number | null,
  body: string,
): Promise<void> {
  await primaryPool.query(
    `UPDATE alert_events
        SET webhook_status = $2,
            webhook_response = LEFT($3, 500),
            webhook_fired_at = now()
      WHERE id = $1`,
    [eventId, status, body],
  );
}

async function deliverOne(d: PendingDelivery): Promise<void> {
  const payload = {
    alert_rule_id: d.rule_id,
    rule_name: d.rule_name,
    tenant_id: d.tenant_id,
    metric: d.metric,
    observed_value: Number(d.observed_value),
    threshold: Number(d.threshold),
    operator: d.operator,
    endpoint: d.endpoint,
    fired_at: d.fired_at,
  };

  // Defence in depth: https-only is validated on create, re-check here.
  if (!d.webhook_url.startsWith('https://')) {
    await markDelivered(d.id, null, 'rejected: webhook_url is not https');
    return;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);

  try {
    const res = await fetch(d.webhook_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    let text = '';
    try {
      text = await res.text();
    } catch {
      text = '';
    }
    await markDelivered(d.id, res.status, text);
  } catch (err) {
    // Timeout / DNS / connection refused etc. Stamp the failure and move on
    // (no infinite retry — documented future improvement).
    const msg = err instanceof Error ? err.message : 'webhook delivery failed';
    await markDelivered(d.id, null, msg);
  } finally {
    clearTimeout(timer);
  }
}

async function tick(log: (msg: string) => void): Promise<void> {
  try {
    const pending = await fetchPending();
    if (pending.length === 0) return;
    // Deliver concurrently; each isolated so one failure can't reject the batch.
    await Promise.allSettled(pending.map((d) => deliverOne(d)));
    log(`webhook worker: processed ${pending.length} alert event(s)`);
  } catch (err) {
    log(
      `webhook worker tick error: ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  }
}

/**
 * Start the worker. Returns a stop() to clear the interval on shutdown.
 */
export function startWebhookWorker(
  log: (msg: string) => void = () => {},
): () => void {
  // Kick once shortly after start so freshly fired alerts go out quickly,
  // then settle into the 60s cadence.
  const initial = setTimeout(() => void tick(log), 2_000);
  const interval = setInterval(() => void tick(log), POLL_INTERVAL_MS);
  return () => {
    clearTimeout(initial);
    clearInterval(interval);
  };
}
