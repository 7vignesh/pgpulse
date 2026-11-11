/**
 * Pure URL validation helpers - no I/O, no DB, fully unit testable.
 */

/**
 * Strict https-only URL validation. The JSON-schema `format: uri` hint is
 * permissive, so this is the authoritative check used by the alert routes.
 * Rejects non-https schemes and URLs with embedded credentials
 * (https://user:pass@host), which could leak secrets or enable SSRF-style
 * abuse via the userinfo component.
 */
export function isHttpsUrl(value: string): boolean {
  try {
    const u = new URL(value);
    return u.protocol === 'https:' && u.username === '' && u.password === '';
  } catch {
    return false;
  }
}
