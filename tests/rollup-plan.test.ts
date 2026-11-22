import { describe, it, expect } from 'vitest';
import {
  HOUR_MS,
  MINUTE_MS,
  planSegments,
  shouldUseRollups,
  usesRollup,
  type Segment,
} from '../src/rollup/plan.js';

const t = (iso: string): Date => new Date(iso);
const wm = (w1m: string, w1h: string) => ({ w1m: t(w1m).getTime(), w1h: t(w1h).getTime() });
const show = (segs: Segment[]): string[] =>
  segs.map((s) => `${s.source} ${s.from.toISOString().slice(11, 19)}-${s.to.toISOString().slice(11, 19)}`);

describe('shouldUseRollups', () => {
  it('uses rollups only for ranges longer than one hour', () => {
    const from = t('2025-01-01T00:00:00Z');
    expect(shouldUseRollups({ from, to: new Date(from.getTime() + HOUR_MS) })).toBe(false);
    expect(shouldUseRollups({ from, to: new Date(from.getTime() + HOUR_MS + 1) })).toBe(true);
    expect(shouldUseRollups({ from, to: new Date(from.getTime() + 5 * 60_000) })).toBe(false);
  });
});

describe('planSegments', () => {
  const caughtUp = wm('2025-01-01T12:00:00Z', '2025-01-01T12:00:00Z');

  it('aligned range comes entirely from rollup_1h', () => {
    const segs = planSegments({ from: t('2025-01-01T03:00:00Z'), to: t('2025-01-01T09:00:00Z') }, caughtUp);
    expect(show(segs)).toEqual(['rollup_1h 03:00:00-09:00:00']);
  });

  it('unaligned edges use 1m rollups, and sub-minute remainders use raw', () => {
    const segs = planSegments(
      { from: t('2025-01-01T02:30:30Z'), to: t('2025-01-01T09:15:10Z') },
      caughtUp,
    );
    expect(show(segs)).toEqual([
      'raw 02:30:30-02:31:00',
      'rollup_1m 02:31:00-03:00:00',
      'rollup_1h 03:00:00-09:00:00',
      'rollup_1m 09:00:00-09:15:00',
      'raw 09:15:00-09:15:10',
    ]);
  });

  it('segments are contiguous and cover the range exactly', () => {
    const range = { from: t('2025-01-01T02:30:30Z'), to: t('2025-01-01T09:15:10Z') };
    const segs = planSegments(range, caughtUp);
    expect(segs[0].from).toEqual(range.from);
    expect(segs[segs.length - 1].to).toEqual(range.to);
    for (let i = 1; i < segs.length; i++) expect(segs[i].from).toEqual(segs[i - 1].to);
  });

  it('data past the watermark falls back to raw (rollup behind)', () => {
    // 1h rollup is complete to 06:00 only; 1m to 08:30.
    const segs = planSegments(
      { from: t('2025-01-01T03:00:00Z'), to: t('2025-01-01T09:00:00Z') },
      wm('2025-01-01T08:30:00Z', '2025-01-01T06:00:00Z'),
    );
    expect(show(segs)).toEqual([
      'rollup_1h 03:00:00-06:00:00',
      'rollup_1m 06:00:00-08:30:00',
      'raw 08:30:00-09:00:00',
    ]);
  });

  it('watermarks that never ran send everything to raw', () => {
    const segs = planSegments(
      { from: t('2025-01-01T03:00:00Z'), to: t('2025-01-01T09:00:00Z') },
      { w1m: 0, w1h: 0 },
    );
    expect(show(segs)).toEqual(['raw 03:00:00-09:00:00']);
    expect(usesRollup(segs)).toBe(false);
  });

  it('ignores the 1m rollup when useMinute is false (percentile queries)', () => {
    const segs = planSegments(
      { from: t('2025-01-01T02:30:00Z'), to: t('2025-01-01T09:30:00Z') },
      caughtUp,
      { useMinute: false },
    );
    expect(show(segs)).toEqual([
      'raw 02:30:00-03:00:00',
      'rollup_1h 03:00:00-09:00:00',
      'raw 09:00:00-09:30:00',
    ]);
  });

  it('a range with no whole hour uses 1m rollups only', () => {
    const segs = planSegments(
      { from: t('2025-01-01T02:10:00Z'), to: t('2025-01-01T03:40:00Z') },
      caughtUp,
    );
    // 02:10 -> 03:00 and 03:00 -> 03:40 are separate hours but the hour 03:00-04:00
    // is incomplete, so no 1h bucket fits: 02:10-03:40 is covered by minutes.
    expect(segs.every((s) => s.source === 'rollup_1m')).toBe(true);
    expect(segs).toHaveLength(1);
    expect(segs[0].to.getTime() - segs[0].from.getTime()).toBe(90 * MINUTE_MS);
  });

  it('range after the watermark is entirely raw', () => {
    const segs = planSegments(
      { from: t('2025-01-01T13:00:00Z'), to: t('2025-01-01T16:00:00Z') },
      caughtUp,
    );
    expect(show(segs)).toEqual(['raw 13:00:00-16:00:00']);
  });
});
