import { createHash, timingSafeEqual } from 'node:crypto';
import process from 'node:process';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { primaryPool } from '../db/pool.js';
import { replicaLag } from './route.js';
import { ingestStats } from '../ingest/pipeline.js';

/**
 * GET /internal/db-health - operator diagnostics.
 *
 * Kept separate from /health, which is a cheap unauthenticated liveness probe.
 * This endpoint runs heavier catalog queries and returns normalized SQL text,
 * so it requires `Authorization: Bearer <INTERNAL_API_TOKEN>` and answers 404
 * when no token is configured (the endpoint does not exist unless enabled).
 *
 * Each section is computed independently: one failing query (for example
 * pg_stat_statements not installed) reports its own error and does not hide
 * the other sections.
 */

const SLOW_QUERY_LIMIT = 10;
const MAX_QUERY_TEXT = 500;
const SECTION_TIMEOUT_MS = 2_000;

// ---------------------------------------------------------------------------
// Pure helpers (unit tested)
// ---------------------------------------------------------------------------

const digest = (s: string): Buffer => createHash('sha256').update(s, 'utf8').digest();

/**
 * Constant-time check of an `Authorization: Bearer <token>` header. Both sides
 * are hashed first so the comparison takes the same time whatever the length
 * of the supplied value.
 */
export function isAuthorized(header: string | undefined, expected: string | undefined): boolean {
  if (!expected) return false;
  if (typeof header !== 'string') return false;
  const m = /^Bearer\s+(.+)$/i.exec(header.trim());
  if (!m) return false;
  return timingSafeEqual(digest(m[1]), digest(expected));
}

/** blks_hit / (blks_hit + blks_read), or null when nothing has been read yet. */
export function cacheHitRatio(hit: number, read: number): number | null {
  const total = hit + read;
  return total > 0 ? Number((hit / total).toFixed(4)) : null;
}

export interface ConnectionRow {
  state: string | null;
  count: number;
}

export function shapeConnections(
  rows: ConnectionRow[],
  maxConnections: number,
): { total: number; active: number; idle: number; idle_in_transaction: number; max_connections: number; utilization: number } {
  const by = (s: string): number => rows.find((r) => r.state === s)?.count ?? 0;
  const total = rows.reduce((n, r) => n + r.count, 0);
  return {
    total,
    active: by('active'),
    idle: by('idle'),
    idle_in_transaction: by('idle in transaction') + by('idle in transaction (aborted)'),
    max_connections: maxConnections,
    utilization: maxConnections > 0 ? Number((total / maxConnections).toFixed(4)) : 0,
  };
}

/** Collapse whitespace and cap the length of normalized SQL text. */
export function truncateQuery(q: string, max = MAX_QUERY_TEXT): string {
  const flat = q.replace(/\s+/g, ' ').trim();
  return flat.length > max ? `${flat.slice(0, max - 3)}...` : flat;
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

type Section<T> = ({ ok: true } & T) | { ok: false; error: string };

async function section<T>(fn: () => Promise<T>): Promise<Section<T>> {
  let timer: NodeJS.Timeout | undefined;
  try {
    const value = await Promise.race([
      fn(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out after ${SECTION_TIMEOUT_MS}ms`)), SECTION_TIMEOUT_MS);
      }),
    ]);
    return { ok: true, ...value };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  } finally {
    clearTimeout(timer);
  }
}

async function cacheSection(): Promise<{ ratio: number | null; blks_hit: number; blks_read: number }> {
  const { rows } = await primaryPool.query<{ blks_hit: string; blks_read: string }>(
    `SELECT blks_hit, blks_read FROM pg_stat_database WHERE datname = current_database()`,
  );
  const hit = Number(rows[0]?.blks_hit ?? 0);
  const read = Number(rows[0]?.blks_read ?? 0);
  return { ratio: cacheHitRatio(hit, read), blks_hit: hit, blks_read: read };
}

async function connectionsSection(): Promise<ReturnType<typeof shapeConnections>> {
  const [{ rows }, max] = await Promise.all([
    primaryPool.query<{ state: string | null; count: number }>(
      `SELECT state, COUNT(*)::int AS count
         FROM pg_stat_activity
        WHERE datname = current_database() AND backend_type = 'client backend'
        GROUP BY state`,
    ),
    primaryPool.query<{ max: string }>(`SELECT current_setting('max_connections') AS max`),
  ]);
  return shapeConnections(rows, Number(max.rows[0].max));
}

async function replicationSection(): Promise<Awaited<ReturnType<typeof replicaLag>>> {
  return replicaLag();
}

/**
 * In-memory ingest buffer (src/ingest/pipeline.ts): events waiting to be
 * flushed, fill ratio (429s start at 0.85), and events dropped after repeated
 * flush failures. Unavailable before the server is ready or after shutdown.
 */
async function bufferSection(): Promise<
  { available: true; depth: number; fill_ratio: number; dropped: number } | { available: false; reason: string }
> {
  const s = ingestStats();
  if (!s) return { available: false, reason: 'ingest pipeline not running' };
  return { available: true, depth: s.depth, fill_ratio: Number(s.fill.toFixed(4)), dropped: s.dropped };
}

interface SlowQuery {
  query: string;
  calls: number;
  mean_exec_ms: number;
  total_exec_ms: number;
  rows: number;
}

async function slowQueriesSection(): Promise<{ queries: SlowQuery[] }> {
  const { rows } = await primaryPool.query<{
    query: string; calls: string; mean: string; total: string; rows: string;
  }>(
    `SELECT query, calls, mean_exec_time AS mean, total_exec_time AS total, rows
       FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      ORDER BY mean_exec_time DESC
      LIMIT $1`,
    [SLOW_QUERY_LIMIT],
  );
  return {
    queries: rows.map((r) => ({
      query: truncateQuery(r.query),
      calls: Number(r.calls),
      mean_exec_ms: Number(Number(r.mean).toFixed(3)),
      total_exec_ms: Number(Number(r.total).toFixed(3)),
      rows: Number(r.rows),
    })),
  };
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

export async function registerDbHealthRoute(app: FastifyInstance): Promise<void> {
  app.get('/internal/db-health', async (request: FastifyRequest, reply) => {
    // Read per request so the token can be rotated by restarting with a new env,
    // and so the endpoint is simply absent when it is not configured.
    const expected = process.env.INTERNAL_API_TOKEN;
    if (!expected) return reply.code(404).send({ error: 'not found' });
    if (!isAuthorized(request.headers.authorization, expected)) {
      return reply.code(401).header('WWW-Authenticate', 'Bearer').send({ error: 'unauthorized' });
    }

    const [cache, connections, replication, buffer, slow] = await Promise.all([
      section(cacheSection),
      section(connectionsSection),
      section(replicationSection),
      section(bufferSection),
      section(slowQueriesSection),
    ]);

    return reply.header('cache-control', 'no-store').send({
      timestamp: new Date().toISOString(),
      cache_hit_ratio: cache,
      connections,
      replication_lag: replication,
      ingest_buffer: buffer,
      slow_queries: slow,
    });
  });
}
