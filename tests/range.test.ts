import { describe, it, expect } from 'vitest';
import { parseRange } from '../src/query/range.js';

describe('parseRange', () => {
  const now = new Date('2025-06-15T12:30:00.000Z');

  it('defaults to the last 24h when no params given', () => {
    const r = parseRange(undefined, undefined, now);
    expect(r.ok).toBe(true);
    expect(r.range!.to.toISOString()).toBe(now.toISOString());
    expect(r.range!.from.toISOString()).toBe('2025-06-14T12:30:00.000Z');
  });

  it('accepts explicit valid range', () => {
    const r = parseRange('2025-06-01T00:00:00Z', '2025-06-02T00:00:00Z', now);
    expect(r.ok).toBe(true);
    expect(r.range!.from.toISOString()).toBe('2025-06-01T00:00:00.000Z');
  });

  it('rejects invalid timestamps', () => {
    const r = parseRange('not-a-date', undefined, now);
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/invalid/);
  });

  it('rejects inverted range', () => {
    const r = parseRange('2025-06-02T00:00:00Z', '2025-06-01T00:00:00Z', now);
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/before/);
  });

  it('rejects equal from/to', () => {
    const r = parseRange('2025-06-01T00:00:00Z', '2025-06-01T00:00:00Z', now);
    expect(r.ok).toBe(false);
  });

  it('rejects ranges over the max window', () => {
    const r = parseRange('2023-01-01T00:00:00Z', '2025-06-01T00:00:00Z', now);
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/may not exceed/);
  });
});
