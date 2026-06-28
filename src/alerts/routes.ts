import type { FastifyInstance } from 'fastify';
import { authenticate } from '../middleware/auth.js';
import {
  createRule,
  listRules,
  deleteRule,
  listEvents,
  type CreateRuleInput,
} from './service.js';

/**
 * Alert routes. All require x-api-key and are tenant-scoped: every service
 * call passes request.tenant!.id, and rule/event queries filter by tenant_id,
 * so no tenant can see or mutate another's alerts.
 */

const METRICS = ['error_rate', 'p95_latency_ms', 'request_volume'] as const;
const OPERATORS = ['>', '<'] as const;
const WINDOWS = [5, 10, 15, 30, 60] as const;

const createRuleSchema = {
  type: 'object',
  required: ['name', 'metric', 'operator', 'threshold', 'window_minutes', 'webhook_url'],
  additionalProperties: false,
  properties: {
    name: { type: 'string', minLength: 1, maxLength: 200 },
    metric: { type: 'string', enum: METRICS as unknown as string[] },
    operator: { type: 'string', enum: OPERATORS as unknown as string[] },
    // Positive number. exclusiveMinimum keeps 0 and negatives out.
    threshold: { type: 'number', exclusiveMinimum: 0 },
    window_minutes: { type: 'integer', enum: WINDOWS as unknown as number[] },
    endpoint_filter: { type: 'string', maxLength: 2048, nullable: true },
    // https-only, validated strictly below in addition to the format hint.
    webhook_url: { type: 'string', maxLength: 2048, format: 'uri' },
  },
} as const;

/** Strict https-only URL validation (format:uri is permissive). */
function isHttpsUrl(value: string): boolean {
  try {
    const u = new URL(value);
    return u.protocol === 'https:';
  } catch {
    return false;
  }
}

export async function registerAlertRoutes(app: FastifyInstance): Promise<void> {
  // Auth for the whole group.
  app.addHook('preHandler', async (request, reply) => {
    if (request.url.startsWith('/v1/alerts')) {
      await authenticate(request, reply);
    }
  });

  app.post<{ Body: CreateRuleInput }>(
    '/v1/alerts/rules',
    { schema: { body: createRuleSchema } },
    async (request, reply) => {
      if (!isHttpsUrl(request.body.webhook_url)) {
        return reply.code(400).send({ error: 'webhook_url must be a valid https:// URL' });
      }
      const rule = await createRule(request.tenant!.id, request.body);
      return reply.code(201).send(rule);
    },
  );

  app.get('/v1/alerts/rules', async (request, reply) => {
    const rules = await listRules(request.tenant!.id);
    return reply.send({ rules });
  });

  app.delete<{ Params: { id: string } }>(
    '/v1/alerts/rules/:id',
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
      const deleted = await deleteRule(request.tenant!.id, request.params.id);
      if (!deleted) {
        return reply.code(404).send({ error: 'alert rule not found' });
      }
      return reply.code(204).send();
    },
  );

  app.get<{ Querystring: { limit?: number } }>(
    '/v1/alerts/events',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            limit: { type: 'integer', minimum: 1, maximum: 500, default: 50 },
          },
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? 50;
      const events = await listEvents(request.tenant!.id, limit);
      return reply.send({ events });
    },
  );
}
