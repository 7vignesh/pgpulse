import type { FastifyInstance } from 'fastify';
import { primaryPool, replicaPool, poolStats } from '../db/pool.js';

/**
 * Health + observability endpoint.
 *
 * Reports:
 *   - DB connectivity (primary + replica)
 *   - app-side pool stats (total/idle/waiting) for exhaustion monitoring
 *   - replica lag in bytes/seconds (from pg_stat_replication on primary, or
 *     pg_last_wal_receive/replay on the replica)
 *   - top slow queries from pg_stat_statements
 *
 * No auth: standard for liveness/readiness probes. Does NOT leak tenant data;
 * pg_stat_statements text is normalized SQL. If you consider query text
 * sensitive, gate this behind operator auth.
 */

interface SlowQuery {
  query: string;
  calls: number;
  mean_exec_time: number;
  total_exec_time: number;
}

async function replicaLag(): Promise<{
  configured: boolean;
  bytes: number | null;
  seconds: number | null;
  note?: string;
}> {
  if (replicaPool === primaryPool) {
    return { configured: false, bytes: null, seconds: null, note: 'no replica configured' };
  }
  try {
    // On the replica: compute lag from received vs replayed LSN and the
    // timestamp of the last replayed transaction.
    const { rows } = await replicaPool.query<{
      lag_bytes: string | null;
      lag_seconds: number | null;
    }>(
      `SELECT
         COALESCE(
           pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()),
           0)::bigint                                            AS lag_bytes,
         EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float AS lag_seconds`,
    );
    const r = rows[0];
    return {
      configured: true,
      bytes: r?.lag_bytes != null ? Number(r.lag_bytes) : null,
      seconds: r?.lag_seconds != null ? Number(r.lag_seconds) : null,
    };
  } catch (err) {
    return {
      configured: true,
      bytes: null,
      seconds: null,
      note: err instanceof Error ? err.message : 'replica lag query failed',
    };
  }
}

async function slowQueries(): Promise<{ available: boolean; rows: SlowQuery[]; note?: string }> {
  try {
    const { rows } = await primaryPool.query<SlowQuery>(
      `SELECT query, calls,
              ROUND(mean_exec_time::numeric, 2)  AS mean_exec_time,
              ROUND(total_exec_time::numeric, 2) AS total_exec_time
       FROM pg_stat_statements
       ORDER BY mean_exec_time DESC
       LIMIT 10`,
    );
    return {
      available: true,
      rows: rows.map((r) => ({
        query: r.query,
        calls: Number(r.calls),
        mean_exec_time: Number(r.mean_exec_time),
        total_exec_time: Number(r.total_exec_time),
      })),
    };
  } catch (err) {
    return {
      available: false,
      rows: [],
      note:
        err instanceof Error
          ? `pg_stat_statements unavailable: ${err.message}`
          : 'pg_stat_statements unavailable',
    };
  }
}

async function pingPrimary(): Promise<boolean> {
  try {
    await primaryPool.query('SELECT 1');
    return true;
  } catch {
    return false;
  }
}

export async function registerHealthRoute(app: FastifyInstance): Promise<void> {
  app.get('/health', async (_request, reply) => {
    const [primaryOk, lag, slow] = await Promise.all([
      pingPrimary(),
      replicaLag(),
      slowQueries(),
    ]);

    const body = {
      status: primaryOk ? 'ok' : 'degraded',
      timestamp: new Date().toISOString(),
      pools: {
        primary: poolStats(primaryPool),
        replica: replicaPool === primaryPool ? 'shared-with-primary' : poolStats(replicaPool),
      },
      replica_lag: lag,
      slow_queries: slow,
    };

    return reply.code(primaryOk ? 200 : 503).send(body);
  });
}
