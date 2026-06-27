import type { FastifyInstance } from 'fastify';
import { authenticate } from '../middleware/auth.js';
import { insertEvent, insertEventsBatch, type EventInput } from './service.js';

/**
 * Ingest routes. Auth + validation in the route; the actual write logic lives
 * in the service. All routes require x-api-key and are tenant-scoped.
 */

const METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'] as const;

const eventSchema = {
  type: 'object',
  required: ['endpoint', 'method', 'status_code', 'latency_ms'],
  additionalProperties: false,
  properties: {
    endpoint: { type: 'string', minLength: 1, maxLength: 2048 },
    method: { type: 'string', enum: METHODS as unknown as string[] },
    status_code: { type: 'integer', minimum: 100, maximum: 599 },
    latency_ms: { type: 'integer', minimum: 0, maximum: 3_600_000 },
    user_agent: { type: 'string', maxLength: 1024, nullable: true },
    ip_address: { type: 'string', maxLength: 45, nullable: true },
    metadata: { type: 'object', nullable: true },
    ingested_at: { type: 'string', format: 'date-time', nullable: true },
  },
} as const;

export async function registerIngestRoutes(app: FastifyInstance): Promise<void> {
  // Single event.
  app.post<{ Body: EventInput }>(
    '/v1/events',
    {
      preHandler: authenticate,
      schema: { body: eventSchema },
    },
    async (request, reply) => {
      const { id } = await insertEvent(request.tenant!.id, request.body);
      return reply.code(201).send({ id });
    },
  );

  // Batch: up to 1000 events in a single transaction.
  app.post<{ Body: { events: EventInput[] } }>(
    '/v1/events/batch',
    {
      preHandler: authenticate,
      schema: {
        body: {
          type: 'object',
          required: ['events'],
          properties: {
            events: {
              type: 'array',
              minItems: 1,
              maxItems: 1000,
              items: eventSchema,
            },
          },
        },
      },
    },
    async (request, reply) => {
      const { inserted } = await insertEventsBatch(
        request.tenant!.id,
        request.body.events,
      );
      return reply.code(201).send({ inserted });
    },
  );
}
