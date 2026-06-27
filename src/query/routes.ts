import type { FastifyInstance, FastifyReply } from 'fastify';
import { authenticate } from '../middleware/auth.js';
import {
  overview,
  endpoints,
  latency,
  errors,
  timeseries,
} from './service.js';
import { parseRange as parseRangePure, type TimeRange } from './range.js';

/**
 * Analytics routes. Thin handlers: authenticate, parse + validate the time
 * range, delegate to the service, return JSON. All reads are tenant-scoped
 * and routed to the replica inside the service layer.
 */

interface RangeQuery {
  from?: string;
  to?: string;
}

/**
 * Parse + validate the range, sending a 400 on failure. Delegates the actual
 * logic to the pure helper so it stays unit-tested.
 */
function parseRange(
  q: RangeQuery,
  reply: FastifyReply,
): TimeRange | null {
  const result = parseRangePure(q.from, q.to);
  if (!result.ok || !result.range) {
    void reply.code(400).send({ error: result.error });
    return null;
  }
  return result.range;
}

const rangeSchema = {
  type: 'object',
  properties: {
    from: { type: 'string', format: 'date-time' },
    to: { type: 'string', format: 'date-time' },
  },
} as const;

export async function registerQueryRoutes(app: FastifyInstance): Promise<void> {
  // Auth applies to every analytics route.
  app.addHook('preHandler', async (request, reply) => {
    if (request.url.startsWith('/v1/analytics')) {
      await authenticate(request, reply);
    }
  });

  app.get<{ Querystring: RangeQuery }>(
    '/v1/analytics/overview',
    { schema: { querystring: rangeSchema } },
    async (request, reply) => {
      const range = parseRange(request.query, reply);
      if (!range) return;
      return reply.send(await overview(request.tenant!.id, range));
    },
  );

  app.get<{ Querystring: RangeQuery & { limit?: number } }>(
    '/v1/analytics/endpoints',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            ...rangeSchema.properties,
            limit: { type: 'integer', minimum: 1, maximum: 1000, default: 50 },
          },
        },
      },
    },
    async (request, reply) => {
      const range = parseRange(request.query, reply);
      if (!range) return;
      const limit = request.query.limit ?? 50;
      return reply.send(await endpoints(request.tenant!.id, range, limit));
    },
  );

  app.get<{ Querystring: RangeQuery & { endpoint?: string } }>(
    '/v1/analytics/latency',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            ...rangeSchema.properties,
            endpoint: { type: 'string', maxLength: 2048 },
          },
        },
      },
    },
    async (request, reply) => {
      const range = parseRange(request.query, reply);
      if (!range) return;
      return reply.send(
        await latency(request.tenant!.id, range, request.query.endpoint),
      );
    },
  );

  app.get<{ Querystring: RangeQuery }>(
    '/v1/analytics/errors',
    { schema: { querystring: rangeSchema } },
    async (request, reply) => {
      const range = parseRange(request.query, reply);
      if (!range) return;
      return reply.send(await errors(request.tenant!.id, range));
    },
  );

  app.get<{ Querystring: RangeQuery & { granularity?: 'hour' | 'day' } }>(
    '/v1/analytics/timeseries',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            ...rangeSchema.properties,
            granularity: { type: 'string', enum: ['hour', 'day'], default: 'hour' },
          },
        },
      },
    },
    async (request, reply) => {
      const range = parseRange(request.query, reply);
      if (!range) return;
      const granularity = request.query.granularity ?? 'hour';
      return reply.send(
        await timeseries(request.tenant!.id, range, granularity),
      );
    },
  );
}
