import process from 'node:process';
import { primaryPool } from '../db/pool.js';
import { parsePositiveInt } from '../middleware/config.js';
import { IngestBuffer, type BufferedEvent } from './buffer.js';
import { makeFlush } from './flush.js';
import { TenantLimiter } from './token-bucket.js';
import type { EventInput } from './service.js';

/**
 * Process-wide ingest pipeline: admission control (per-tenant token bucket +
 * buffer high-watermark) in front of the bounded buffer.
 */

const HIGH_WATERMARK = 0.85;
const MAX_ATTEMPTS = 3;

const env = (key: string, fallback: number): number =>
  parsePositiveInt(process.env[key], fallback);

export type EnqueueResult =
  | { ok: true }
  | { ok: false; reason: 'buffer_full' | 'quota_exceeded'; retryAfterSec: number };

export interface IngestItem {
  event: EventInput;
  idempotencyKey: string | null;
}

let buffer: IngestBuffer | null = null;
let limiter: TenantLimiter | null = null;
let flushIntervalMs = 500;
let pruneTimer: NodeJS.Timeout | null = null;

export function startIngest(log: (msg: string) => void = () => {}): void {
  if (buffer) return;
  flushIntervalMs = env('INGEST_FLUSH_INTERVAL_MS', 500);
  buffer = new IngestBuffer({
    capacity: env('INGEST_BUFFER_CAPACITY', 10_000),
    batchSize: env('INGEST_BATCH_SIZE', 1_000),
    flushIntervalMs,
    highWatermark: HIGH_WATERMARK,
    maxAttempts: MAX_ATTEMPTS,
    flush: makeFlush(primaryPool, log),
    log,
  });
  limiter = new TenantLimiter(env('INGEST_TENANT_RATE', 1_000), env('INGEST_TENANT_BURST', 5_000));
  buffer.start();
  pruneTimer = setInterval(() => limiter?.prune(), 60_000);
  pruneTimer.unref();
}

/** Stop accepting events and flush what is buffered. Safe to call twice. */
export async function stopIngest(): Promise<void> {
  const b = buffer;
  if (pruneTimer) clearInterval(pruneTimer);
  pruneTimer = null;
  buffer = null;
  limiter = null;
  await b?.close();
}

/** Flush everything currently buffered. Used by tests and shutdown paths. */
export async function flushIngest(): Promise<void> {
  while (buffer && buffer.depth > 0) await buffer.flushNow();
  await buffer?.flushNow();
}

export function ingestStats(): { depth: number; fill: number; dropped: number } | null {
  return buffer
    ? { depth: buffer.depth, fill: buffer.fillRatio, dropped: buffer.droppedCount }
    : null;
}

export function enqueueEvents(tenantId: string, items: IngestItem[]): EnqueueResult {
  if (!buffer || !limiter) {
    return { ok: false, reason: 'buffer_full', retryAfterSec: 1 };
  }
  // Check the buffer first so a rejected request does not spend quota.
  if (!buffer.canAccept(items.length)) {
    return {
      ok: false,
      reason: 'buffer_full',
      retryAfterSec: Math.max(1, Math.ceil(flushIntervalMs / 1000)),
    };
  }
  const quota = limiter.tryConsume(tenantId, items.length);
  if (!quota.ok) {
    return { ok: false, reason: 'quota_exceeded', retryAfterSec: quota.retryAfterSec };
  }

  const receivedAt = new Date();
  const entries: BufferedEvent[] = items.map((i) => ({
    tenantId,
    idempotencyKey: i.idempotencyKey,
    event: i.event,
    receivedAt,
    attempts: 0,
  }));
  // canAccept() was true and offer() is synchronous, so this cannot fail.
  const res = buffer.offer(entries);
  return res.ok ? { ok: true } : { ok: false, reason: 'buffer_full', retryAfterSec: 1 };
}
