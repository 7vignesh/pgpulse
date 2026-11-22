/**
 * Pure helpers for the partition lifecycle - no I/O, fully unit testable.
 * (Daily partition creation lives in SQL: ensure_events_daily_partitions().)
 */

export interface PartitionInfo {
  name: string;
  /** null for the DEFAULT partition */
  from: Date | null;
  to: Date | null;
  isDefault: boolean;
  /** DETACH ... CONCURRENTLY was started but not finished */
  detachPending: boolean;
}

export const DAY_MS = 86_400_000;

const IDENT = /^[a-z_][a-z0-9_]{0,62}$/;

/** Table names are interpolated into DDL, so only plain lowercase identifiers are allowed. */
export function assertIdent(name: string): string {
  if (!IDENT.test(name)) throw new Error(`unsafe SQL identifier: ${JSON.stringify(name)}`);
  return name;
}

/** UTC midnight of the day containing `d`. */
export function utcDay(d: Date): Date {
  return new Date(Math.floor(d.getTime() / DAY_MS) * DAY_MS);
}

const TS = /^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2}))?\+00$/;

function parseUtcTimestamp(s: string): Date {
  const m = TS.exec(s);
  if (!m) throw new Error(`unrecognised partition bound (session time zone must be UTC): ${s}`);
  const [, y, mo, d, h = '0', mi = '0', se = '0'] = m;
  return new Date(Date.UTC(+y, +mo - 1, +d, +h, +mi, +se));
}

/**
 * Parse pg_get_expr(relpartbound, oid), e.g.
 * "FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00')".
 * The session must run with TimeZone = UTC so bounds are printed as +00.
 */
export function parseBound(expr: string): Pick<PartitionInfo, 'from' | 'to' | 'isDefault'> {
  if (expr.trim() === 'DEFAULT') return { from: null, to: null, isDefault: true };
  const m = /^FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)$/.exec(expr.trim());
  if (!m) throw new Error(`unsupported partition bound: ${expr}`);
  return { from: parseUtcTimestamp(m[1]), to: parseUtcTimestamp(m[2]), isDefault: false };
}

/**
 * The ingested_at values the events table can store: from the retention
 * cutoff up to the end of the last pre-created day. There is no DEFAULT
 * partition (DETACH ... CONCURRENTLY forbids one), so anything outside this
 * window would fail the INSERT and must be rejected at the API instead.
 */
export function ingestWindow(
  now: Date,
  retentionDays: number,
  aheadDays: number,
): { min: Date; max: Date } {
  const today = utcDay(now).getTime();
  return {
    min: new Date(today - retentionDays * DAY_MS),
    max: new Date(today + (aheadDays + 1) * DAY_MS), // exclusive
  };
}

/**
 * A partition is expired when its whole range ended before the retention
 * cutoff (start of today minus retentionDays). The DEFAULT partition has no
 * range and is never "expired".
 */
export function isExpired(
  p: Pick<PartitionInfo, 'to' | 'isDefault'>,
  now: Date,
  retentionDays: number,
): boolean {
  if (p.isDefault || p.to === null) return false;
  const cutoff = utcDay(now).getTime() - retentionDays * DAY_MS;
  return p.to.getTime() <= cutoff;
}

/** events/dt=2026-08-01/events_20260801.parquet. `dt` is the partition's start day (UTC). */
export function archiveKey(prefix: string, parent: string, partition: string, from: Date): string {
  const dt = from.toISOString().slice(0, 10);
  const clean = prefix.replace(/^\/+|\/+$/g, '');
  return `${clean ? clean + '/' : ''}${assertIdent(parent)}/dt=${dt}/${assertIdent(partition)}.parquet`;
}

/** Double-quote an identifier for use in SQL. */
export function quoteIdent(name: string): string {
  return `"${assertIdent(name)}"`;
}
