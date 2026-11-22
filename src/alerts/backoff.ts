/**
 * Retry delay for webhook delivery: exponential with "equal jitter".
 *
 * `attempt` is the 1-based number of the attempt that just failed. The
 * exponential ceiling is base * 2^(attempt-1), capped at `capMs`. The returned
 * delay is uniform in [ceiling/2, ceiling), so retries from many failing
 * webhooks spread out while still backing off at least half the ceiling.
 */
export function backoffDelayMs(
  attempt: number,
  random: () => number = Math.random,
  baseMs = 1_000,
  capMs = 15 * 60_000,
): number {
  const exponent = Math.max(0, attempt - 1);
  const ceiling = Math.min(capMs, baseMs * 2 ** Math.min(exponent, 30));
  return Math.floor(ceiling / 2 + random() * (ceiling / 2));
}
