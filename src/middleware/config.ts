/**
 * Pure config-parsing helpers - no I/O, fully unit testable.
 */

/**
 * Parse a raw string into a positive integer, returning `fallback` for
 * anything that isn't one: missing/empty values, non-numeric strings,
 * fractional values, zero, or negatives.
 */
export function parsePositiveInt(raw: string | undefined, fallback: number): number {
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}
