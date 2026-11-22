import { describe, it, expect } from 'vitest';
import { RingBuffer } from '../src/ingest/ring-buffer.js';
import { TokenBucket, TenantLimiter } from '../src/ingest/token-bucket.js';
import { encodeBinaryCopy } from '../src/ingest/binary-copy.js';
import { IngestBuffer, type BufferedEvent } from '../src/ingest/buffer.js';
import { findEventProblem } from '../src/ingest/event-checks.js';

const entry = (n = 0): BufferedEvent => ({
  tenantId: 't',
  idempotencyKey: null,
  event: { endpoint: `/e${n}`, method: 'GET', status_code: 200, latency_ms: 1 },
  receivedAt: new Date(0),
  attempts: 0,
});

describe('RingBuffer', () => {
  it('is FIFO and wraps around', () => {
    const r = new RingBuffer<number>(3);
    r.push(1); r.push(2); r.push(3);
    expect(r.drain(2)).toEqual([1, 2]);
    r.push(4); r.push(5);
    expect(r.drain(10)).toEqual([3, 4, 5]);
    expect(r.size).toBe(0);
  });

  it('rejects pushes when full instead of growing', () => {
    const r = new RingBuffer<number>(2);
    expect(r.push(1)).toBe(true);
    expect(r.push(2)).toBe(true);
    expect(r.push(3)).toBe(false);
    expect(r.size).toBe(2);
  });

  it('rejects an invalid capacity', () => {
    expect(() => new RingBuffer(0)).toThrow(RangeError);
  });
});

describe('TokenBucket', () => {
  it('allows a burst, then reports Retry-After, then refills', () => {
    let now = 0;
    const b = new TokenBucket(10, 20, () => now);
    expect(b.tryConsume(20)).toEqual({ ok: true });
    expect(b.tryConsume(5)).toEqual({ ok: false, retryAfterSec: 1 });
    expect(b.tryConsume(100)).toEqual({ ok: false, retryAfterSec: 10 });
    now += 1000; // +10 tokens
    expect(b.tryConsume(10)).toEqual({ ok: true });
    expect(b.tryConsume(1).ok).toBe(false);
  });

  it('never refills above burst', () => {
    let now = 0;
    const b = new TokenBucket(10, 5, () => now);
    now += 60_000;
    expect(b.tryConsume(6).ok).toBe(false);
    expect(b.tryConsume(5).ok).toBe(true);
  });

  it('keeps tenants independent and prunes idle buckets', () => {
    let now = 0;
    const l = new TenantLimiter(1, 2, () => now);
    expect(l.tryConsume('a', 2).ok).toBe(true);
    expect(l.tryConsume('a', 1).ok).toBe(false);
    expect(l.tryConsume('b', 2).ok).toBe(true);
    l.prune();
    expect(l.size).toBe(2);
    now += 10_000;
    l.prune();
    expect(l.size).toBe(0);
  });
});

describe('encodeBinaryCopy', () => {
  it('writes the exact binary COPY layout', () => {
    const buf = encodeBinaryCopy(['int2', 'int4', 'text', 'timestamptz'], [
      [7, -1, 'hi', new Date(Date.UTC(2000, 0, 1, 0, 0, 1))],
      [null, 2, 'é', new Date(Date.UTC(1999, 11, 31, 23, 59, 59))],
    ]);
    expect(buf.subarray(0, 11).toString('latin1')).toBe('PGCOPY\n\xff\r\n\0');
    expect(buf.readInt32BE(11)).toBe(0);
    expect(buf.readInt32BE(15)).toBe(0);
    let o = 19;
    expect(buf.readInt16BE(o)).toBe(4); o += 2;
    expect(buf.readInt32BE(o)).toBe(2); o += 4;
    expect(buf.readInt16BE(o)).toBe(7); o += 2;
    expect(buf.readInt32BE(o)).toBe(4); o += 4;
    expect(buf.readInt32BE(o)).toBe(-1); o += 4;
    expect(buf.readInt32BE(o)).toBe(2); o += 4;
    expect(buf.subarray(o, o + 2).toString()).toBe('hi'); o += 2;
    expect(buf.readInt32BE(o)).toBe(8); o += 4;
    expect(buf.readBigInt64BE(o)).toBe(1_000_000n); o += 8;
    // row 2: NULL int2, utf8 length counts bytes, pre-2000 timestamps are negative
    expect(buf.readInt16BE(o)).toBe(4); o += 2;
    expect(buf.readInt32BE(o)).toBe(-1); o += 4;
    o += 4 + 4; // int4 length + value
    expect(buf.readInt32BE(o)).toBe(2); o += 4; // 'é' is 2 bytes
    o += 2;
    o += 4;
    expect(buf.readBigInt64BE(o)).toBe(-1_000_000n); o += 8;
    expect(buf.readInt16BE(o)).toBe(-1);
    expect(o + 2).toBe(buf.length);
  });

  it('rejects out-of-range and mistyped values', () => {
    expect(() => encodeBinaryCopy(['int2'], [[40000]])).toThrow(RangeError);
    expect(() => encodeBinaryCopy(['int4'], [[1.5]])).toThrow(RangeError);
    expect(() => encodeBinaryCopy(['text'], [[5]])).toThrow(TypeError);
    expect(() => encodeBinaryCopy(['timestamptz'], [[new Date('x')]])).toThrow(TypeError);
    expect(() => encodeBinaryCopy(['int2', 'int2'], [[1]])).toThrow(RangeError);
  });
});

describe('findEventProblem', () => {
  const ok = { endpoint: '/a', method: 'GET' as const, status_code: 200, latency_ms: 1 };
  it('accepts a normal event', () => {
    expect(findEventProblem(ok)).toBeNull();
  });
  it('rejects NUL characters anywhere, including nested metadata', () => {
    expect(findEventProblem({ ...ok, endpoint: 'a\u0000b' })).not.toBeNull();
    expect(findEventProblem({ ...ok, metadata: { a: [{ b: 'x\u0000' }] } })).not.toBeNull();
  });
  it('rejects malformed IPs and timestamps', () => {
    expect(findEventProblem({ ...ok, ip_address: 'nope' })).not.toBeNull();
    expect(findEventProblem({ ...ok, ip_address: '10.0.0.1' })).toBeNull();
    expect(findEventProblem({ ...ok, ingested_at: 'garbage' })).not.toBeNull();
  });
});

describe('IngestBuffer', () => {
  const make = (over: Partial<ConstructorParameters<typeof IngestBuffer>[0]> = {}) => {
    const flushed: BufferedEvent[][] = [];
    const buf = new IngestBuffer({
      capacity: 10,
      batchSize: 4,
      flushIntervalMs: 20,
      highWatermark: 0.85,
      maxAttempts: 2,
      flush: async (b) => {
        flushed.push(b);
        return [];
      },
      ...over,
    });
    return { buf, flushed };
  };

  it('flushes when the batch size is reached', async () => {
    const { buf, flushed } = make();
    buf.offer([entry(1), entry(2), entry(3), entry(4)]);
    await buf.flushNow();
    expect(flushed.map((b) => b.length)).toEqual([4]);
  });

  it('flushes on the interval', async () => {
    const { buf, flushed } = make();
    buf.start();
    buf.offer([entry(1)]);
    await new Promise((r) => setTimeout(r, 80));
    await buf.close();
    expect(flushed.flat().length).toBe(1);
  });

  it('rejects at >= 85% fill and when a request would not fit', () => {
    const { buf } = make({ batchSize: 100 });
    expect(buf.offer(Array.from({ length: 8 }, (_, i) => entry(i))).ok).toBe(true); // 80%
    expect(buf.canAccept(1)).toBe(true);
    expect(buf.offer([entry(9)]).ok).toBe(true); // 90%
    expect(buf.offer([entry(10)])).toEqual({ ok: false, reason: 'buffer_full' });
    expect(buf.depth).toBe(9);
  });

  it('offer is all-or-nothing', () => {
    const { buf } = make({ batchSize: 100 });
    expect(buf.offer(Array.from({ length: 11 }, (_, i) => entry(i))).ok).toBe(false);
    expect(buf.depth).toBe(0);
  });

  it('close() drains everything and then rejects new events', async () => {
    const { buf, flushed } = make({ batchSize: 3 });
    buf.offer([entry(1), entry(2)]);
    buf.offer([entry(3)]);
    await buf.close();
    expect(flushed.flat().length).toBe(3);
    expect(buf.depth).toBe(0);
    expect(buf.offer([entry(4)]).ok).toBe(false);
  });

  it('re-queues failed entries and drops them after maxAttempts', async () => {
    let calls = 0;
    const { buf } = make({
      maxAttempts: 2,
      flush: async (b) => {
        calls += 1;
        return b;
      },
    });
    buf.offer([entry(1)]);
    await buf.close(); // terminates because attempts are bounded
    expect(calls).toBe(2);
    expect(buf.droppedCount).toBe(1);
  });

  it('never runs two flushes at once', async () => {
    let active = 0;
    let max = 0;
    const { buf } = make({
      batchSize: 1,
      flush: async () => {
        active += 1;
        max = Math.max(max, active);
        await new Promise((r) => setTimeout(r, 10));
        active -= 1;
        return [];
      },
    });
    buf.offer([entry(1), entry(2), entry(3)]);
    await buf.close();
    expect(max).toBe(1);
  });
});
