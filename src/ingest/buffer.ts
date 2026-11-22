import { RingBuffer } from './ring-buffer.js';
import type { EventInput } from './service.js';

/**
 * Bounded in-memory buffer between the HTTP handlers and the database.
 * Handlers offer() events and return immediately. A flush runs when the batch
 * size is reached or the interval elapses, never more than one at a time.
 */

export interface BufferedEvent {
  tenantId: string;
  idempotencyKey: string | null;
  event: EventInput;
  /** Receipt time, so ingested_at reflects arrival and not flush time. */
  receivedAt: Date;
  attempts: number;
}

/** Writes a batch and returns the entries that failed and may be retried. */
export type FlushFn = (batch: BufferedEvent[]) => Promise<BufferedEvent[]>;

export interface IngestBufferOptions {
  capacity: number;
  batchSize: number;
  flushIntervalMs: number;
  /** Fill ratio at or above which offers are rejected. */
  highWatermark: number;
  maxAttempts: number;
  flush: FlushFn;
  log?: (msg: string) => void;
}

export type OfferResult = { ok: true } | { ok: false; reason: 'buffer_full' };

export class IngestBuffer {
  private readonly ring: RingBuffer<BufferedEvent>;
  private timer: NodeJS.Timeout | null = null;
  private inFlight: Promise<void> | null = null;
  private closed = false;
  private dropped = 0;

  constructor(private readonly opts: IngestBufferOptions) {
    this.ring = new RingBuffer(opts.capacity);
  }

  get depth(): number {
    return this.ring.size;
  }

  get droppedCount(): number {
    return this.dropped;
  }

  get fillRatio(): number {
    return this.ring.size / this.opts.capacity;
  }

  /** True when `n` more events would be accepted right now. */
  canAccept(n: number): boolean {
    return (
      !this.closed &&
      this.fillRatio < this.opts.highWatermark &&
      this.ring.size + n <= this.opts.capacity
    );
  }

  /** All-or-nothing: either every entry is buffered or none is. */
  offer(entries: BufferedEvent[]): OfferResult {
    if (!this.canAccept(entries.length)) return { ok: false, reason: 'buffer_full' };
    for (const e of entries) this.ring.push(e);
    if (this.ring.size >= this.opts.batchSize) void this.flushNow();
    return { ok: true };
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.flushNow(), this.opts.flushIntervalMs);
    this.timer.unref();
  }

  /** Flush one batch. Concurrent calls share the in-flight flush. */
  flushNow(): Promise<void> {
    if (this.inFlight) return this.inFlight;
    if (this.ring.size === 0) return Promise.resolve();
    const batch = this.ring.drain(this.opts.batchSize);
    this.inFlight = this.runFlush(batch).finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async runFlush(batch: BufferedEvent[]): Promise<void> {
    let failed: BufferedEvent[];
    try {
      failed = await this.opts.flush(batch);
    } catch (err) {
      this.opts.log?.(`ingest flush error: ${err instanceof Error ? err.message : String(err)}`);
      failed = batch;
    }
    for (const e of failed) {
      e.attempts += 1;
      if (e.attempts >= this.opts.maxAttempts || !this.ring.push(e)) {
        this.dropped += 1;
      }
    }
    if (failed.length > 0) {
      this.opts.log?.(`ingest flush: ${failed.length} event(s) failed, dropped so far: ${this.dropped}`);
    }
  }

  /**
   * Stop accepting events and flush everything still buffered. Gives up on
   * entries that keep failing (bounded by maxAttempts) so shutdown terminates.
   */
  async close(): Promise<void> {
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    while (this.inFlight || this.ring.size > 0) {
      await this.flushNow();
    }
  }
}
