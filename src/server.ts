import Fastify, { type FastifyInstance } from 'fastify';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import process from 'node:process';
import { registerRateLimit } from './middleware/ratelimit.js';
import { registerTenantRoutes } from './tenants/routes.js';
import { registerIngestRoutes } from './ingest/routes.js';
import { registerQueryRoutes } from './query/routes.js';
import { registerAlertRoutes } from './alerts/routes.js';
import { startAlertWorkers } from './alerts/workers.js';
import { startPartitionManager } from './partitions/manager.js';
import { registerHealthRoute } from './health/route.js';
import { registerDbHealthRoute } from './health/db-health.js';
import { startIngest, stopIngest } from './ingest/pipeline.js';
import { PoolExhaustedError, closePools, primaryPool } from './db/pool.js';
import { readRouter } from './db/router.js';
import { startRollupWorker } from './rollup/worker.js';

/**
 * Fastify server assembly. Builds the app, registers plugins + route groups,
 * and wires graceful shutdown. Exported buildServer() so integration tests can
 * spin up an instance without binding a port.
 */

export async function buildServer(): Promise<FastifyInstance> {
  const app = Fastify({
    logger: {
      level: process.env.LOG_LEVEL ?? 'info',
      // Don't log credentials.
      redact: ['req.headers["x-api-key"]', 'req.headers.authorization', 'req.headers.cookie'],
    },
    // Trust proxy so req.ip reflects X-Forwarded-For behind PgBouncer/LB.
    trustProxy: true,
  });

  await registerRateLimit(app);

  // Buffered ingest: start once the app is ready; on close (SIGTERM path and
  // tests) the server has stopped accepting requests, so drain what is buffered
  // before the caller closes the DB pools.
  app.addHook('onReady', async () => startIngest((msg) => app.log.info(msg)));
  app.addHook('onClose', async () => stopIngest());

  // Lag-aware read routing: probe the replica; analytics reads fall back to the
  // primary whenever it is lagging or unreachable.
  app.addHook('onReady', async () => readRouter.start());
  app.addHook('onClose', async () => readRouter.stop());

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
    // Honor explicit client-error status codes (e.g. 404 not-found) instead of
    // masking them as 500. 5xx still falls through to the generic handler so
    // internal error detail is never leaked.
    const statusCode = (err as { statusCode?: number }).statusCode;
    if (typeof statusCode === 'number' && statusCode >= 400 && statusCode < 500) {
      return reply.code(statusCode).send({ error: err.message });
    }
    request.log.error({ err }, 'unhandled error');
    return reply.code(500).send({ error: 'internal server error' });
  });

  await app.register(registerHealthRoute);
  await app.register(registerDbHealthRoute);
  await app.register(registerTenantRoutes);
  await app.register(registerIngestRoutes);
  await app.register(registerQueryRoutes);
  await app.register(registerAlertRoutes);

  return app;
}

async function main(): Promise<void> {
  const app = await buildServer();
  const rawPort = Number(process.env.PORT ?? 3000);
  const port = Number.isFinite(rawPort) && rawPort > 0 && rawPort <= 65535
    ? rawPort
    : 3000;

  // Alert evaluator + webhook dispatcher loops. Multiple instances are safe
  // (SKIP LOCKED + leases). Only runs in the long-lived server process, never
  // under tests that import buildServer().
  const stopAlertWorkers = startAlertWorkers((msg) => app.log.info(msg));
  // Incremental rollups (watermark cursors); safe to run on several instances.
  const stopRollupWorker = startRollupWorker((msg) => app.log.info(msg), primaryPool);
  // Partition lifecycle: pre-create daily partitions, archive + drop expired
  // ones. Leader-elected with an advisory lock, so every instance may run it.
  const stopPartitionManager = startPartitionManager((msg) => app.log.info(msg));

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    app.log.info(`received ${signal}, shutting down`);
    // Hard deadline: if a graceful close hangs (e.g. an in-flight query won't
    // drain), force-exit so orchestrators don't have to escalate to SIGKILL.
    const forceExit = setTimeout(() => {
      app.log.error('graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, 10_000);
    forceExit.unref();
    try {
      await stopAlertWorkers();
      await stopRollupWorker();
      await stopPartitionManager();
      await app.close();
      await closePools();
      clearTimeout(forceExit);
      process.exit(0);
    } catch (err) {
      clearTimeout(forceExit);
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
const currentFile = fileURLToPath(import.meta.url);
const entryFile = process.argv[1] ? resolve(process.argv[1]) : '';
if (currentFile === entryFile) {
  void main();
}
