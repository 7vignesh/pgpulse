/**
 * Pure planning of where each part of a time range is read from - no I/O.
 *
 * rollup_1h holds COMPLETE UTC hours, rollup_1m holds complete minutes, each up
 * to its watermark. Anything a rollup cannot answer exactly (sub-minute edges,
 * the newest data past the watermark) is read from raw `events`, so a stale or
 * missing rollup only makes a query slower, never wrong.
 *
 *   from                                                              to
 *    |raw|--- 1m ---|=========== 1h ===========|--- 1m ---|   raw   |
 *     <1m  <60 min   whole hours                <60 min    past watermark
 */

export type SegmentSource = 'raw' | 'rollup_1m' | 'rollup_1h';

export interface Segment {
  source: SegmentSource;
  from: Date;
  to: Date;
}

/** Epoch-ms watermarks of each rollup; 0 means "never ran". */
export interface Watermarks {
  w1m: number;
  w1h: number;
}

export const MINUTE_MS = 60_000;
export const HOUR_MS = 3_600_000;

const floorTo = (ms: number, unit: number): number => Math.floor(ms / unit) * unit;
const ceilTo = (ms: number, unit: number): number => Math.ceil(ms / unit) * unit;

/** Ranges longer than one hour are served from rollups. */
export function shouldUseRollups(range: { from: Date; to: Date }): boolean {
  return range.to.getTime() - range.from.getTime() > HOUR_MS;
}

/**
 * Split [from, to) into contiguous segments. With `useMinute: false` the 1m
 * rollup is ignored (used by percentile queries, which cannot be merged across
 * minute buckets).
 */
export function planSegments(
  range: { from: Date; to: Date },
  wm: Watermarks,
  opts: { useMinute: boolean } = { useMinute: true },
): Segment[] {
  const from = range.from.getTime();
  const to = range.to.getTime();
  const limit1h = floorTo(wm.w1h, HOUR_MS);
  const limit1m = opts.useMinute ? floorTo(wm.w1m, MINUTE_MS) : -Infinity;

  const out: Segment[] = [];
  const push = (source: SegmentSource, a: number, b: number): void => {
    if (a < b) out.push({ source, from: new Date(a), to: new Date(b) });
  };

  // Cover [a, b) with raw edges and whole minutes from rollup_1m.
  const minuteLevel = (a: number, b: number): void => {
    if (a >= b) return;
    const mStart = ceilTo(a, MINUTE_MS);
    const mEnd = Math.min(floorTo(b, MINUTE_MS), limit1m);
    if (mStart < mEnd) {
      push('raw', a, mStart);
      push('rollup_1m', mStart, mEnd);
      push('raw', mEnd, b);
    } else {
      push('raw', a, b);
    }
  };

  const hStart = ceilTo(from, HOUR_MS);
  const hEnd = Math.min(floorTo(to, HOUR_MS), limit1h);
  if (hStart < hEnd) {
    minuteLevel(from, hStart);
    push('rollup_1h', hStart, hEnd);
    minuteLevel(hEnd, to);
  } else {
    minuteLevel(from, to);
  }
  return out;
}

export function usesRollup(segments: Segment[]): boolean {
  return segments.some((s) => s.source !== 'raw');
}
