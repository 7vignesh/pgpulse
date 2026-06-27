import type { FastifyRequest, FastifyReply } from 'fastify';
import { primaryPool } from '../db/pool.js';

/**
 * API-key authentication, per tenant.
 *
 * The `x-api-key` header is matched against tenants.api_key. On success the
 * resolved tenant is attached to the request as `request.tenant`. We look up
 * via the primary pool (PgBouncer) because auth is in the write path's
 * critical section and must reflect freshly rotated keys immediately.
 */

export interface AuthedTenant {
  id: string;
  name: string;
  plan: 'free' | 'pro' | 'enterprise';
}

declare module 'fastify' {
  interface FastifyRequest {
    tenant?: AuthedTenant;
  }
}

export async function authenticate(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const apiKey = request.headers['x-api-key'];

  if (!apiKey || typeof apiKey !== 'string') {
    await reply.code(401).send({ error: 'missing x-api-key header' });
    return;
  }

  // Parameterized query => no SQL injection from header value.
  const { rows } = await primaryPool.query<AuthedTenant>(
    'SELECT id, name, plan FROM tenants WHERE api_key = $1',
    [apiKey],
  );

  if (rows.length === 0) {
    await reply.code(401).send({ error: 'invalid api key' });
    return;
  }

  request.tenant = rows[0];
}
