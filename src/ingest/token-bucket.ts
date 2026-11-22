/**
 * Token-bucket rate limiting, one bucket per tenant. Tokens are events, not
 * requests, so a 1000-event batch costs 1000 tokens. The clock is injectable
 * for deterministic tests.
 */

export type ConsumeResult = { ok: true } | { ok: false; retryAfterSec: number };

export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly ratePerSec: number,
    private readonly burst: number,
    private readonly now: () => number = Date.now,
  ) {
    this.tokens = burst;
    this.last = now();
  }

  private refill(): void {
    const t = this.now();
    const elapsedSec = Math.max(0, t - this.last) / 1000;
    this.tokens = Math.min(this.burst, this.tokens + elapsedSec * this.ratePerSec);
    this.last = t;
  }

  tryConsume(n: number): ConsumeResult {
    this.refill();
    if (n <= this.tokens) {
      this.tokens -= n;
      return { ok: true };
    }
    const missing = n - this.tokens;
    return { ok: false, retryAfterSec: Math.max(1, Math.ceil(missing / this.ratePerSec)) };
  }

  /** True when the bucket is full, i.e. the tenant has been idle long enough. */
  isIdle(): boolean {
    this.refill();
    return this.tokens >= this.burst;
  }
}

export class TenantLimiter {
  private readonly buckets = new Map<string, TokenBucket>();

  constructor(
    private readonly ratePerSec: number,
    private readonly burst: number,
    private readonly now: () => number = Date.now,
  ) {}

  tryConsume(tenantId: string, n: number): ConsumeResult {
    let b = this.buckets.get(tenantId);
    if (!b) {
      b = new TokenBucket(this.ratePerSec, this.burst, this.now);
      this.buckets.set(tenantId, b);
    }
    return b.tryConsume(n);
  }

  /** Drop buckets of idle tenants so the map does not grow without bound. */
  prune(): void {
    for (const [id, b] of this.buckets) {
      if (b.isIdle()) this.buckets.delete(id);
    }
  }

  get size(): number {
    return this.buckets.size;
  }
}
