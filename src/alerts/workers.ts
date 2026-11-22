import { randomUUID } from 'node:crypto';
import process from 'node:process';
import { primaryPool } from '../db/pool.js';
import { parsePositiveInt } from '../middleware/config.js';
import { evaluateDueRules, evaluatorConfigFromEnv } from './evaluator.js';
import { dispatchDue, dispatcherConfigFromEnv } from './dispatcher.js';

/**
 * Background loops for alerting. Start one per app instance; instances
 * coordinate through the database (SKIP LOCKED + leases), so running many is
 * safe. Only started from the long-lived server process, never from tests that
 * import buildServer().
 */

function loop(
  name: string,
  intervalMs: number,
  work: () => Promise<boolean>,
  log: (msg: string) => void,
): () => Promise<void> {
  let inFlight: Promise<void> | null = null;
  let stopped = false;

  const tick = (): void => {
    if (inFlight || stopped) return;
    inFlight = (async () => {
      try {
        // Keep draining while a full batch comes back.
        while (!stopped && (await work())) {
          /* more work available */
        }
      } catch (err) {
        log(`${name} tick error: ${err instanceof Error ? err.message : String(err)}`);
      }
    })().finally(() => {
      inFlight = null;
    });
  };

  const first = setTimeout(tick, 1_000);
  const timer = setInterval(tick, intervalMs);
  first.unref();
  timer.unref();

  return async () => {
    stopped = true;
    clearTimeout(first);
    clearInterval(timer);
    await inFlight;
  };
}

/** Start the evaluator and dispatcher loops. Returns an async stop(). */
export function startAlertWorkers(log: (msg: string) => void = () => {}): () => Promise<void> {
  const workerId = `${process.pid}-${randomUUID().slice(0, 8)}`;
  const pollMs = parsePositiveInt(process.env.ALERT_POLL_MS, 5_000);
  const evalCfg = evaluatorConfigFromEnv(`eval-${workerId}`);
  const dispCfg = dispatcherConfigFromEnv(`disp-${workerId}`);

  const stopEval = loop(
    'alert evaluator',
    pollMs,
    async () => {
      const r = await evaluateDueRules(primaryPool, evalCfg, log);
      if (r.fired || r.resolved || r.errors || r.lost) {
        log(
          `alert evaluator: claimed=${r.claimed} fired=${r.fired} resolved=${r.resolved} lost=${r.lost} errors=${r.errors}`,
        );
      }
      return r.claimed >= evalCfg.batchSize;
    },
    log,
  );

  const stopDispatch = loop(
    'alert dispatcher',
    pollMs,
    async () => {
      const r = await dispatchDue(primaryPool, dispCfg, undefined, log);
      if (r.claimed) {
        log(
          `alert dispatcher: claimed=${r.claimed} sent=${r.sent} retried=${r.retried} dead=${r.dead} lost=${r.lost}`,
        );
      }
      return r.claimed >= dispCfg.batchSize;
    },
    log,
  );

  return async () => {
    await Promise.all([stopEval(), stopDispatch()]);
  };
}
