import type { FastifyInstance } from 'fastify';
import { createTenant, getTenant, rotateApiKey } from './service.js';

/**
 * Tenant admin routes. Thin handlers: validate input, call service, shape
 * response. No business logic here.
 *
 * NOTE: In production these admin endpoints would sit behind an operator
 * auth layer (e.g. an admin JWT or mTLS). That is intentionally out of scope
 * here; flagged so it is not silently shipped unauthenticated.
 */

const VALID_PLANS = ['free', 'pro', 'enterprise'] as const;

export async function registerTenantRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: { name?: string; plan?: string } }>(
    '/v1/tenants',
    {
      schema: {
        body: {
          type: 'object',
          required: ['name'],
          properties: {
            name: { type: 'string', minLength: 1, maxLength: 200 },
            plan: { type: 'string', enum: VALID_PLANS as unknown as string[] },
          },
        },
      },
    },
    async (request, reply) => {
      const { name, plan } = request.body;
      const tenant = await createTenant(
        name!,
        (plan as (typeof VALID_PLANS)[number]) ?? 'free',
      );
      // Return api_key exactly once on creation.
      return reply.code(201).send(tenant);
    },
  );

  app.get<{ Params: { id: string } }>(
    '/v1/tenants/:id',
    {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
          properties: { id: { type: 'string', format: 'uuid' } },
        },
      },
    },
    async (request, reply) => {
      const tenant = await getTenant(request.params.id);
      if (!tenant) {
        return reply.code(404).send({ error: 'tenant not found' });
      }
      return reply.send(tenant);
    },
  );

  app.post<{ Params: { id: string } }>(
    '/v1/tenants/:id/rotate-key',
    {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
          properties: { id: { type: 'string', format: 'uuid' } },
        },
      },
    },
    async (request, reply) => {
      const result = await rotateApiKey(request.params.id);
      if (!result) {
        return reply.code(404).send({ error: 'tenant not found' });
      }
      return reply.send(result);
    },
  );
}
