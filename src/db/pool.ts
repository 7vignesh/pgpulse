import pg from 'pg';
import process from 'node:process';

const { Pool } = pg;

/**
 * PgBouncer-aware connection pools.
 *
 * Two logical pools:
 *   - primaryPool  -> writes + recent/realtime reads, via PgBouncer (txn mode)
 *   - replicaPool  -> analytics reads, via the streaming replica
 *
 * IMPORTANT (PgBouncer transaction mode constraints):
 *   - Prepared statements are disabled (`pg` sends unnamed/simple where it can;
 *     we additionally never name statements). Server-side prepared statements
 *     would bind to a backend that PgBouncer may hand to another client.
 *   - No LISTEN/NOTIFY, no session-level state that outlives a transaction.
 *   - All per-transaction state (e.g. `SET LOCAL app.current_tenant_id`) must
 *     use SET LOCAL so it is scoped to the transaction PgBouncer keeps pinned.
 *   See docs/SCALING.md for the full rationale.
 */

const num = (v: string | undefined, fallback: number): number => {
  const n = v ? Number(v) : NaN;
  return Number.isFinite(n) ? n : fallback;
};

const baseConfig = {
  max: num(process.env.PG_POOL_MAX, 20),
  idleTimeoutMillis: num(process.env.PG_POOL_IDLE_TIMEOUT_MS, 30_000),
  connectionTimeoutMillis: num(process.env.PG_POOL_CONNECT_TIMEOUT_MS, 5_000),
  // Disable libpq-level statement naming; keep us safe under txn pooling.
  // `pg` only uses server-prepared statements when you pass a `name`, which we
  // never do, but we make the intent explicit.
  allowExitOnIdle: false,
};

function makePool(connectionString: string | undefined, label: string): pg.Pool {
  if (!connectionString) {
    throw new Error(`Missing connection string for ${label} pool`);
  }
  const pool = new Pool({ ...baseConfig, connectionString });

  // Pool errors on idle clients must never crash the process.
  pool.on('error', (err) => {
    // eslint-disable-next-line no-console
    console.error(`[pool:${label}] idle client error: ${err.message}`);
  });

  return pool;
}

export const primaryPool = makePool(process.env.PRIMARY_DATABASE_URL, 'primary');

// Replica is optional; fall back to primary if not configured.
export const replicaPool = process.env.REPLICA_DATABASE_URL
  ? makePool(process.env.REPLICA_DATABASE_URL, 'replica')
  : primaryPool;

export interface PoolStats {
  total: number;
  idle: number;
  waiting: number;
}

export function poolStats(pool: pg.Pool): PoolStats {
  return {
    total: pool.totalCount,
    idle: pool.idleCount,
    waiting: pool.waitingCount,
  };
}

/**
 * Run a function inside a transaction on the given pool, with optional tenant
 * scoping for RLS. When `tenantId` is provided we issue
 * `SET LOCAL app.current_tenant_id` so RLS policies on `events` apply.
 *
 * Handles pool exhaustion explicitly: connectionTimeoutMillis causes
 * `pool.connect()` to reject; we surface a typed PoolExhaustedError so callers
 * can map it to HTTP 503.
 */
export async function withTransaction<T>(
  pool: pg.Pool,
  fn: (client: pg.PoolClient) => Promise<T>,
  opts: { tenantId?: string; readOnly?: boolean } = {},
): Promise<T> {
  let client: pg.PoolClient;
  try {
    client = await pool.connect();
  } catch (err) {
    throw new PoolExhaustedError(
      err instanceof Error ? err.message : 'failed to acquire connection',
    );
  }

  try {
    await client.query(opts.readOnly ? 'BEGIN READ ONLY' : 'BEGIN');
    if (opts.tenantId) {
      // Parameterized SET LOCAL is not allowed; validate + inject safely.
      // tenantId comes from a trusted UUID column, but we still re-validate.
      if (!/^[0-9a-f-]{36}$/i.test(opts.tenantId)) {
        throw new Error('invalid tenantId for RLS scope');
      }
      await client.query(
        `SET LOCAL app.current_tenant_id = '${opts.tenantId}'`,
      );
    }
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore rollback failure */
    }
    throw err;
  } finally {
    client.release();
  }
}

export class PoolExhaustedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PoolExhaustedError';
  }
}

export async function closePools(): Promise<void> {
  await primaryPool.end();
  if (replicaPool !== primaryPool) {
    await replicaPool.end();
  }
}
