/**
 * Pure helpers for analytics request handling — no DB access, fully unit
 * testable. Used by the query service + routes.
 */

export interface TimeRange {
  from: Date;
  to: Date;
}

export interface ParseRangeResult {
  ok: boolean;
  range?: TimeRange;
  error?: string;
}

const MAX_RANGE_DAYS = 366;
const DAY_MS = 86_400_000;

/**
 * Parse and validate from/to query params. Defaults to the last 24h.
 */
export function parseRange(
  fromStr: string | undefined,
  toStr: string | undefined,
  now: Date = new Date(),
): ParseRangeResult {
  const to = toStr ? new Date(toStr) : now;
  const from = fromStr ? new Date(fromStr) : new Date(now.getTime() - DAY_MS);

  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    return { ok: false, error: 'invalid from/to timestamp' };
  }
  if (from >= to) {
    return { ok: false, error: 'from must be before to' };
  }
  if (to.getTime() - from.getTime() > MAX_RANGE_DAYS * DAY_MS) {
    return { ok: false, error: `range may not exceed ${MAX_RANGE_DAYS} days` };
  }
  return { ok: true, range: { from, to } };
}
