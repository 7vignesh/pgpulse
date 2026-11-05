import type { FastifyInstance } from 'fastify';
import rateLimit from '@fastify/rate-limit';
import process from 'node:process';

/**
 * Per-tenant rate limiting.
 *
 * Keyed by the x-api-key header when present (so limits are per customer),
 * falling back to client IP for unauthenticated requests. The header is used
 * rather than req.tenant because the tenant is only resolved by a route-level
 * authenticate preHandler, which Fastify runs AFTER plugin hooks (route hooks
 * execute last in their category). Keying off req.tenant would therefore see
 * it undefined and silently fall back to IP. The header is available at every
 * hook stage, so keying is stable regardless of hook ordering.
 *
 * In-memory store is fine for a single app instance; swap to the Redis store
 * for multi-instance deployments (see @fastify/rate-limit docs).
 */

function envInt(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

export async function registerRateLimit(app: FastifyInstance): Promise<void> {
  await app.register(rateLimit, {
    max: envInt('RATE_LIMIT_MAX', 1000),
    timeWindow: process.env.RATE_LIMIT_WINDOW ?? '1 minute',
    keyGenerator: (req) => {
      const apiKey = req.headers['x-api-key'];
      return typeof apiKey === 'string' && apiKey.trim() !== '' ? apiKey : req.ip;
    },
    errorResponseBuilder: (_req, context) => ({
      error: 'rate limit exceeded',
      max: context.max,
      windowMs: context.ttl,
    }),
  });
}
