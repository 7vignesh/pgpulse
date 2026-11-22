import type { Pool, PoolClient } from 'pg';
import process from 'node:process';
import { primaryPool, replicaPool, withTransaction, PoolExhaustedError } from './pool.js';
import { parsePositiveInt } from '../middleware/config.js';

/**
 * Lag-aware read router.
 *
 * Analytics reads go to the streaming replica while it is healthy and fall
 * back to the primary when replication lag exceeds REPLICA_MAX_LAG_MS (default
 * 2000) or the replica cannot be probed. A background probe samples the lag;
 * going unhealthy is immediate, coming back requires several consecutive good
 * probes so a replica hovering around the limit does not flap.
 *
 * Writes, auth and alerting always use the primary pool and never pass
 * through here.
 */

/** Row returned by the probe query that runs ON THE REPLICA. */
export interface LagProbeRow {
  in_recovery: boolean;
  streaming: boolean;
  caught_up: boolean | null;
  since_replay: number | null;
}

/**
 * Replication lag in seconds, or Infinity when the replica cannot be trusted
 * (not a standby, WAL receiver not streaming, or behind with no replay
 * timestamp to measure from).
 *
 * "Caught up" (received LSN == replayed LSN) counts as zero lag even if the
 * last replayed transaction is old, so an idle primary does not look like a
 * lagging replica.
 */
export function lagSecondsFromProbe(r: LagProbeRow): number {
  if (!r.in_recovery || !r.streaming) return Infinity;
  if (r.caught_up) return 0;
  return r.since_replay === null ? Infinity : Math.max(0, r.since_replay);
}

// pg_stat_wal_receiver is visible to superusers and members of pg_monitor /
// pg_read_all_stats. Without that privilege `streaming` is false and the
// router (safely) never uses the replica.
const PROBE_SQL = `
  SELECT pg_is_in_recovery() AS in_recovery,
         EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming') AS streaming,
         pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() AS caught_up,
         EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float8 AS since_replay`;

export function makeReplicaSampler(pool: Pool): () => Promise<number> {
  return async () => {
    const { rows } = await pool.query<LagProbeRow>(PROBE_SQL);
    return lagSecondsFromProbe(rows[0]);
  };
}

export interface RouterOptions {
  primary: Pool;
  /** null = no replica configured; every read goes to the primary. */
  replica: Pool | null;
  /** Returns replication lag in seconds (Infinity = unusable). Throws on failure. */
  sample: () => Promise<number>;
  maxLagMs: number;
  intervalMs: number;
  /** Consecutive good probes required to return to the replica. */
  recoveryProbes: number;
  probeTimeoutMs: number;
  now?: () => number;
  log?: (msg: string) => void;
}

export interface RouterStatus {
  target: 'replica' | 'primary';
  replica_configured: boolean;
  lag_ms: number | null;
  max_lag_ms: number;
  last_probe_at: string | null;
  reason: string | null;
  failovers: number;
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`probe timed out after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

export class ReplicaRouter {
  private healthy = false;
  private everHealthy = false;
  private goodStreak = 0;
  private lagMs: number | null = null;
  private lastProbeAt: number | null = null;
  private reason: string | null = 'not probed yet';
  private failovers = 0;
  private probing: Promise<void> | null = null;
  private timer: NodeJS.Timeout | null = null;

  constructor(private readonly opts: RouterOptions) {}

  getReadPool(): Pool {
    return this.opts.replica !== null && this.healthy ? this.opts.replica : this.opts.primary;
  }

  /** Run one probe. Concurrent calls share the in-flight probe. */
  probeOnce(): Promise<void> {
    if (this.opts.replica === null) return Promise.resolve();
    if (this.probing) return this.probing;
    this.probing = this.doProbe().finally(() => {
      this.probing = null;
    });
    return this.probing;
  }

  private async doProbe(): Promise<void> {
    const { maxLagMs, probeTimeoutMs, recoveryProbes } = this.opts;
    let lag: number | null = null;
    let error: string | null = null;
    try {
      lag = (await withTimeout(this.opts.sample(), probeTimeoutMs)) * 1000;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    }

    this.lastProbeAt = (this.opts.now ?? Date.now)();
    this.lagMs = lag !== null && Number.isFinite(lag) ? lag : null;

    if (lag === null || lag > maxLagMs) {
      this.goodStreak = 0;
      this.setHealthy(
        false,
        error ?? (Number.isFinite(lag) ? `replication lag ${Math.round(lag!)}ms > ${maxLagMs}ms` : 'replica is not streaming'),
      );
      return;
    }

    this.goodStreak += 1;
    // The first good probe is enough at startup; after a failover require a streak.
    const needed = this.everHealthy ? recoveryProbes : 1;
    if (!this.healthy && this.goodStreak >= needed) this.setHealthy(true, null);
    else if (this.healthy) this.reason = null;
  }

  private setHealthy(next: boolean, reason: string | null): void {
    this.reason = reason;
    if (next === this.healthy) return;
    this.healthy = next;
    if (next) {
      this.everHealthy = true;
      this.opts.log?.('read router: replica healthy, routing analytics reads to replica');
    } else {
      this.failovers += 1;
      this.opts.log?.(`read router: failing over to primary (${reason ?? 'unknown'})`);
    }
  }

  /** Probe once (awaited) and keep probing in the background. */
  async start(): Promise<void> {
    if (this.timer || this.opts.replica === null) return;
    await this.probeOnce();
    this.timer = setInterval(() => void this.probeOnce(), this.opts.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  status(): RouterStatus {
    return {
      target: this.opts.replica !== null && this.healthy ? 'replica' : 'primary',
      replica_configured: this.opts.replica !== null,
      lag_ms: this.lagMs === null ? null : Math.round(this.lagMs),
      max_lag_ms: this.opts.maxLagMs,
      last_probe_at: this.lastProbeAt === null ? null : new Date(this.lastProbeAt).toISOString(),
      reason: this.reason,
      failovers: this.failovers,
    };
  }
}

const NETWORK_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND']);

/**
 * True for errors where re-running a read-only transaction on the primary is
 * the right response: replica unreachable or shutting down, connection class
 * errors (08xxx), admin shutdown / crash (57P01-57P03), and recovery
 * conflicts, where the replica cancels a long query to apply WAL (40001, 40P01).
 */
export function isReplicaFailure(err: unknown): boolean {
  if (err instanceof PoolExhaustedError) return true;
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code !== 'string') return false;
  return (
    NETWORK_CODES.has(code) ||
    code.startsWith('08') ||
    code === '57P01' ||
    code === '57P02' ||
    code === '57P03' ||
    code === '40001' ||
    code === '40P01'
  );
}

const env = (key: string, fallback: number): number => parsePositiveInt(process.env[key], fallback);

const hasReplica = replicaPool !== primaryPool;

export const readRouter = new ReplicaRouter({
  primary: primaryPool,
  replica: hasReplica ? replicaPool : null,
  sample: hasReplica ? makeReplicaSampler(replicaPool) : async () => 0,
  maxLagMs: env('REPLICA_MAX_LAG_MS', 2000),
  intervalMs: env('REPLICA_PROBE_INTERVAL_MS', 1000),
  recoveryProbes: env('REPLICA_RECOVERY_PROBES', 3),
  probeTimeoutMs: env('REPLICA_PROBE_TIMEOUT_MS', 1000),
  log: (msg) => console.log(msg), // eslint-disable-line no-console
});

/**
 * Run a read-only, tenant-scoped transaction on the replica when it is
 * healthy, otherwise on the primary. If the replica fails mid-request with a
 * connection-level error the whole transaction is re-run on the primary,
 * which is safe because it is read-only. `fn` may therefore run twice.
 */
export async function readTransaction<T>(
  fn: (client: PoolClient) => Promise<T>,
  opts: { tenantId?: string } = {},
): Promise<T> {
  const pool = readRouter.getReadPool();
  const txOpts = { ...opts, readOnly: true };
  if (pool === primaryPool) return withTransaction(primaryPool, fn, txOpts);
  try {
    return await withTransaction(pool, fn, txOpts);
  } catch (err) {
    if (!isReplicaFailure(err)) throw err;
    void readRouter.probeOnce(); // refresh health now instead of waiting for the interval
    return withTransaction(primaryPool, fn, txOpts);
  }
}
