/**
 * Fixed-capacity FIFO ring buffer. push() on a full buffer returns false
 * instead of growing, so memory use is bounded by construction.
 */
export class RingBuffer<T> {
  private readonly slots: Array<T | undefined>;
  private head = 0;
  private count = 0;

  constructor(readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.slots = new Array<T | undefined>(capacity);
  }

  get size(): number {
    return this.count;
  }

  push(item: T): boolean {
    if (this.count === this.capacity) return false;
    this.slots[(this.head + this.count) % this.capacity] = item;
    this.count += 1;
    return true;
  }

  /** Remove and return up to `max` items, oldest first. */
  drain(max: number): T[] {
    const n = Math.min(max, this.count);
    const out: T[] = new Array<T>(n);
    for (let i = 0; i < n; i++) {
      out[i] = this.slots[this.head] as T;
      this.slots[this.head] = undefined;
      this.head = (this.head + 1) % this.capacity;
    }
    this.count -= n;
    return out;
  }
}
