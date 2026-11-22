import type { FastifyInstance, FastifyReply } from 'fastify';
import { authenticate } from '../middleware/auth.js';
import { enqueueEvents, type IngestItem } from './pipeline.js';
import { findEventProblem } from './event-checks.js';
import type { EventInput } from './service.js';
import { ingestWindow } from '../partitions/plan.js';
import { aheadDays, retentionDays } from '../partitions/config.js';

/**
 * Ingest routes. Auth + validation in the route; events are handed to the
 * buffered pipeline and acknowledged with 202 (accepted, not yet committed).
 * All routes require x-api-key and are tenant-scoped.
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
    idempotency_key: { type: 'string', minLength: 1, maxLength: 255 },
  },
} as const;

/**
 * events has no DEFAULT partition (DETACH ... CONCURRENTLY forbids one), so a
 * client-supplied ingested_at outside the pre-created window would fail the
 * INSERT with a 500. Reject it up front with a 400 instead.
 */
function timestampProblem(events: EventInput[]): string | null {
  const { min, max } = ingestWindow(new Date(), retentionDays(), aheadDays());
  for (const e of events) {
    if (e.ingested_at == null) continue;
    const t = new Date(e.ingested_at);
    if (Number.isNaN(t.getTime()) || t < min || t >= max) {
      return `ingested_at must be between ${min.toISOString()} and ${max.toISOString()}`;
    }
  }
  return null;
}

type EventBody = EventInput & { idempotency_key?: string };

function toItem(body: EventBody): IngestItem {
  const { idempotency_key, ...event } = body;
  return { event, idempotencyKey: idempotency_key ?? null };
}

async function accept(
  reply: FastifyReply,
  tenantId: string,
  bodies: EventBody[],
): Promise<FastifyReply> {
  for (const b of bodies) {
    const problem = findEventProblem(b);
    if (problem) return reply.code(400).send({ error: 'validation failed', details: problem });
  }
  // Checked before buffering: an event the partitions cannot hold would fail
  // its whole tenant batch at flush time and take valid events down with it.
  const window = timestampProblem(bodies);
  if (window) return reply.code(400).send({ error: 'validation failed', details: window });
  const res = enqueueEvents(tenantId, bodies.map(toItem));
  if (!res.ok) {
    return reply
      .code(429)
      .header('Retry-After', String(res.retryAfterSec))
      .send({
        error:
          res.reason === 'buffer_full'
            ? 'ingest buffer near capacity, retry later'
            : 'tenant ingest quota exceeded, retry later',
        retry_after_sec: res.retryAfterSec,
      });
  }
  return reply.code(202).send({ accepted: bodies.length });
}

export async function registerIngestRoutes(app: FastifyInstance): Promise<void> {
  // Single event. The Idempotency-Key header is an alternative to the body field.
  app.post<{ Body: EventBody }>(
    '/v1/events',
    {
      preHandler: authenticate,
      schema: { body: eventSchema },
    },
    async (request, reply) => {
      const header = request.headers['idempotency-key'];
      const body: EventBody = { ...request.body };
      if (body.idempotency_key === undefined && typeof header === 'string' && header !== '') {
        if (header.length > 255) {
          return reply.code(400).send({ error: 'Idempotency-Key must be at most 255 characters' });
        }
        body.idempotency_key = header;
      }
      return accept(reply, request.tenant!.id, [body]);
    },
  );

  // Batch: up to 1000 events per request.
  app.post<{ Body: { events: EventBody[] } }>(
    '/v1/events/batch',
    {
      preHandler: authenticate,
      schema: {
        body: {
          type: 'object',
          required: ['events'],
          additionalProperties: false,
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
    async (request, reply) => accept(reply, request.tenant!.id, request.body.events),
  );
}
