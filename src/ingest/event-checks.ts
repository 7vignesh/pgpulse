import { isIP } from 'node:net';
import type { EventInput } from './service.js';

/**
 * Checks that must run before an event is buffered. Ingest is asynchronous, so
 * a value Postgres would reject (NUL in text/jsonb, malformed inet) would
 * otherwise fail the whole flush transaction for every event in it.
 */

function containsNul(v: unknown): boolean {
  if (typeof v === 'string') return v.includes('\u0000');
  if (Array.isArray(v)) return v.some(containsNul);
  if (v !== null && typeof v === 'object') {
    return Object.entries(v).some(([k, x]) => k.includes('\u0000') || containsNul(x));
  }
  return false;
}

/** Returns a human-readable problem, or null when the event is storable. */
export function findEventProblem(e: EventInput): string | null {
  if (containsNul(e)) return 'event contains a NUL (\u0000) character';
  if (e.ip_address != null && isIP(e.ip_address) === 0) return 'ip_address is not a valid IP';
  if (e.ingested_at != null && Number.isNaN(Date.parse(e.ingested_at))) {
    return 'ingested_at is not a valid timestamp';
  }
  return null;
}
