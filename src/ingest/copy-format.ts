/**
 * Pure helpers for the COPY-based bulk ingest path - no I/O, no DB, fully
 * unit testable.
 */

/**
 * Escape a value for Postgres COPY text format. The text format is tab-
 * delimited and newline-terminated, and uses backslash as its escape
 * character, so tabs, newlines, carriage returns and backslashes in a value
 * must be escaped or they would corrupt the COPY stream.
 *
 * Backslash must be escaped first so the escapes emitted for the other
 * characters are not themselves re-escaped.
 */
export function escCopyText(s: string): string {
  return s
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}
