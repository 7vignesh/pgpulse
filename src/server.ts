import Fastify, { type FastifyInstance } from 'fastify';
import process from 'node:process';
import { registerRateLimit } from './middleware/ratelimit.js';
import { registerTenantRoutes } from './tenants/routes.js';
import { registerIngestRoutes } from './ingest/routes.js';
import { registerQueryRoutes } from './query/routes.js';
import { registerHealthRoute } from './health/route.js';
import { PoolExhaustedError, closePools } from './db/pool.js';

/**
 * Fastify server assembly. Builds the app, registers plugins + route groups,
 * and wires graceful shutdown. Exported buildServer() so integration tests can
 * spin up an instance without binding a port.
 */

export async function buildServer(): Promise<FastifyInstance> {
  const app = Fastify({
    logger: {
      level: process.env.LOG_LEVEL ?? 'info',
      // Don't log api keys.
      redact: ['req.headers["x-api-key"]'],
    },
    // Trust proxy so req.ip reflects X-Forwarded-For behind PgBouncer/LB.
    trustProxy: true,
  });

  await registerRateLimit(app);

  // Map known error types to clean HTTP responses.
  app.setErrorHandler((err, request, reply) => {
    if (err instanceof PoolExhaustedError) {
      request.log.error({ err }, 'connection pool exhausted');
      return reply.code(503).send({ error: 'service unavailable: database busy' });
    }
    // Fastify validation errors carry a statusCode of 400.
    if ((err as { validation?: unknown }).validation) {
      return reply.code(400).send({ error: 'validation failed', details: err.message });
    }
    request.log.error({ err }, 'unhandled error');
    return reply.code(500).send({ error: 'internal server error' });
  });

  await app.register(registerHealthRoute);
  await app.register(registerTenantRoutes);
  await app.register(registerIngestRoutes);
  await app.register(registerQueryRoutes);

  return app;
}

async function main(): Promise<void> {
  const app = await buildServer();
  const port = Number(process.env.PORT ?? 3000);

  const shutdown = async (signal: string): Promise<void> => {
    app.log.info(`received ${signal}, shutting down`);
    try {
      await app.close();
      await closePools();
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, 'error during shutdown');
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  try {
    await app.listen({ port, host: '0.0.0.0' });
  } catch (err) {
    app.log.error({ err }, 'failed to start');
    process.exit(1);
  }
}

// Only auto-start when run directly (not when imported by tests).
const isMain = process.argv[1]?.endsWith('server.ts') || process.argv[1]?.endsWith('server.js');
if (isMain) {
  void main();
}
