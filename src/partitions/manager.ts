import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import process from 'node:process';
import { parsePositiveInt } from '../middleware/config.js';
import { aheadDays, retentionDays } from './config.js';
import { exportTableToParquet } from './export.js';
import {
  archiveKey,
  isExpired,
  parseBound,
  quoteIdent,
  type PartitionInfo,
} from './plan.js';
import { parquetRowCount, storeFromEnv, type ObjectStore } from './store.js';

/**
 * Partition lifecycle manager.
 *
 * One run, on a DIRECT (non-PgBouncer) session, because it needs a session
 * advisory lock and DETACH ... CONCURRENTLY, which cannot run in a transaction
 * block or through transaction pooling:
 *
 *   0. pg_try_advisory_lock: only one instance works at a time; the others skip.
 *   1. Pre-create daily partitions for today .. today + PARTITION_AHEAD_DAYS.
 *   2. For every partition that ended before the retention cutoff (and any
 *      earlier run left unfinished), driven by partition_archive_log:
 *        detaching -> ALTER TABLE events DETACH PARTITION p CONCURRENTLY
 *                     (or ... FINALIZE if a previous attempt was interrupted)
 *        detached  -> export to compressed Parquet, upload to S3/MinIO
 *        verified  -> object exists, size > 0, Parquet footer row count equals
 *                     count(*) of the detached table
 *        dropped   -> DROP TABLE, only after verification passed in this run
 *   A failure in one partition is logged and retried on the next run; the
 *   table simply stays detached. Nothing is dropped unless an object store is
 *   configured, because without one there is nowhere to archive to.
 */

export interface ManagerDeps {
  adminUrl: string;
  /** null disables detach/archive/drop (partitions are still pre-created). */
  store: ObjectStore | null;
  now?: () => Date;
  aheadDays: number;
  retentionDays: number;
  /** Key prefix inside the bucket. */
  prefix?: string;
  log?: (msg: string) => void;
}

export interface RunSummary {
  skipped: boolean;
  archiveEnabled: boolean;
  created: number;
  archived: string[];
  failed: { partition: string; error: string }[];
}

const LOCK_NAME = 'pgpulse:partition-manager';

interface LogRow {
  partition_name: string;
  range_from: Date;
  range_to: Date;
  status: 'detaching' | 'detached' | 'verified' | 'dropped';
  attempts: number;
}

async function listPartitions(c: pg.Client): Promise<(PartitionInfo)[]> {
  const { rows } = await c.query<{ name: string; bound: string; pending: boolean }>(
    `SELECT c.relname AS name,
            pg_get_expr(c.relpartbound, c.oid) AS bound,
            i.inhdetachpending AS pending
       FROM pg_inherits i
       JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = 'events'::regclass
      ORDER BY c.relname`,
  );
  return rows.map((r) => ({ name: r.name, ...parseBound(r.bound), detachPending: r.pending }));
}

async function tableExists(c: pg.Client, name: string): Promise<boolean> {
  const { rows } = await c.query<{ r: string | null }>(`SELECT to_regclass($1)::text AS r`, [quoteIdent(name)]);
  return rows[0].r !== null;
}

async function setStatus(
  c: pg.Client,
  name: string,
  status: LogRow['status'],
  extra: { rows?: number; key?: string | null; bytes?: number; sha?: string } = {},
): Promise<void> {
  await c.query(
    `UPDATE partition_archive_log
        SET status = $2,
            row_count = COALESCE($3, row_count),
            s3_key = COALESCE($4, s3_key),
            bytes = COALESCE($5, bytes),
            sha256 = COALESCE($6, sha256),
            last_error = NULL,
            detached_at = CASE WHEN $2 = 'detached' THEN now() ELSE detached_at END,
            verified_at = CASE WHEN $2 = 'verified' THEN now() ELSE verified_at END,
            dropped_at = CASE WHEN $2 = 'dropped' THEN now() ELSE dropped_at END
      WHERE partition_name = $1`,
    [name, status, extra.rows ?? null, extra.key ?? null, extra.bytes ?? null, extra.sha ?? null],
  );
}

async function detach(c: pg.Client, p: Pick<PartitionInfo, 'name' | 'detachPending'>): Promise<void> {
  const q = quoteIdent(p.name);
  // Autocommit statements: CONCURRENTLY refuses to run inside a transaction block.
  if (p.detachPending) await c.query(`ALTER TABLE events DETACH PARTITION ${q} FINALIZE`);
  else await c.query(`ALTER TABLE events DETACH PARTITION ${q} CONCURRENTLY`);
}

/** Throws unless the archived object provably holds every row of the table. */
async function verifyArchive(
  c: pg.Client,
  store: ObjectStore,
  name: string,
  key: string,
  expectedBytes: number | null,
): Promise<number> {
  const head = await store.head(key);
  if (!head || head.size <= 0) throw new Error(`archive object missing or empty: ${key}`);
  if (expectedBytes !== null && head.size !== expectedBytes) {
    throw new Error(`archive size ${head.size} != uploaded ${expectedBytes}`);
  }
  const inFooter = await parquetRowCount(store, key);
  const { rows } = await c.query<{ n: string }>(`SELECT count(*) AS n FROM ${quoteIdent(name)}`);
  const inTable = Number(rows[0].n);
  if (inFooter !== inTable) {
    throw new Error(`row count mismatch: parquet has ${inFooter}, table has ${inTable}`);
  }
  return inTable;
}

async function archiveOne(
  c: pg.Client,
  deps: ManagerDeps & { store: ObjectStore },
  p: { name: string; from: Date; to: Date; attached: boolean; detachPending: boolean },
  log: LogRow | undefined,
): Promise<void> {
  const name = p.name;
  const say = deps.log ?? (() => {});

  if (!log) {
    await c.query(
      `INSERT INTO partition_archive_log (partition_name, range_from, range_to)
       VALUES ($1, $2, $3) ON CONFLICT (partition_name) DO NOTHING`,
      [name, p.from, p.to],
    );
  }
  let status: LogRow['status'] = log?.status ?? 'detaching';

  if (status === 'detaching') {
    if (p.attached) {
      await detach(c, p);
      say(`partition ${name}: detached (concurrently)`);
    }
    await setStatus(c, name, 'detached');
    status = 'detached';
  }

  let key: string;
  let bytes: number | null = null;
  if (status === 'detached') {
    key = archiveKey(deps.prefix ?? 'archive', 'events', name, p.from);
    const dir = await mkdtemp(join(tmpdir(), 'pgpulse-archive-'));
    try {
      const file = join(dir, `${name}.parquet`);
      const exported = await exportTableToParquet(c as unknown as pg.Client, name, file);
      if (exported.rows === 0) {
        // Nothing to preserve: skip the upload and drop the empty table.
        await setStatus(c, name, 'verified', { rows: 0 });
        key = '';
      } else {
        await deps.store.put(key, file, {
          row_count: String(exported.rows),
          sha256: exported.sha256,
          source_table: name,
        });
        bytes = exported.bytes;
        await c.query(
          `UPDATE partition_archive_log SET row_count = $2, s3_key = $3, bytes = $4, sha256 = $5 WHERE partition_name = $1`,
          [name, exported.rows, key, exported.bytes, exported.sha256],
        );
      }
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
    if (key !== '') {
      await verifyArchive(c, deps.store, name, key, bytes);
      await setStatus(c, name, 'verified');
    }
    status = 'verified';
  } else {
    // Resumed at 'verified': re-verify right before dropping.
    const { rows } = await c.query<{ s3_key: string | null; bytes: string | null; row_count: string | null }>(
      `SELECT s3_key, bytes, row_count FROM partition_archive_log WHERE partition_name = $1`,
      [name],
    );
    const r = rows[0];
    if (r.s3_key) {
      await verifyArchive(c, deps.store, name, r.s3_key, r.bytes === null ? null : Number(r.bytes));
    } else if (Number(r.row_count ?? 0) !== 0) {
      throw new Error('verified partition has no archive key');
    } else {
      const { rows: n } = await c.query<{ n: string }>(`SELECT count(*) AS n FROM ${quoteIdent(name)}`);
      if (Number(n[0].n) !== 0) throw new Error('partition recorded as empty now has rows');
    }
  }

  await c.query(`DROP TABLE ${quoteIdent(name)}`);
  await setStatus(c, name, 'dropped');
  say(`partition ${name}: archived and dropped`);
}

export async function runPartitionMaintenance(deps: ManagerDeps): Promise<RunSummary> {
  const now = deps.now?.() ?? new Date();
  const say = deps.log ?? (() => {});
  const summary: RunSummary = {
    skipped: false,
    archiveEnabled: deps.store !== null,
    created: 0,
    archived: [],
    failed: [],
  };

  const c = new pg.Client({ connectionString: deps.adminUrl });
  await c.connect();
  try {
    // Bounds from pg_get_expr are printed in the session time zone.
    await c.query(`SET TIME ZONE 'UTC'`);
    await c.query(`SET lock_timeout = '10s'`);

    const got = await c.query<{ ok: boolean }>(`SELECT pg_try_advisory_lock(hashtext($1)) AS ok`, [LOCK_NAME]);
    if (!got.rows[0].ok) {
      summary.skipped = true;
      return summary;
    }
    try {
      const created = await c.query<{ n: number }>(
        `SELECT ensure_events_daily_partitions($1) AS n`,
        [deps.aheadDays],
      );
      summary.created = created.rows[0].n;
      if (summary.created > 0) say(`partition manager: created ${summary.created} daily partition(s)`);

      if (deps.store === null) return summary;
      const store = deps.store;

      const parts = await listPartitions(c);
      const logs = (
        await c.query<LogRow>(
          `SELECT partition_name, range_from, range_to, status, attempts
             FROM partition_archive_log WHERE status <> 'dropped'`,
        )
      ).rows;
      const logByName = new Map(logs.map((l) => [l.partition_name, l]));

      type Work = { name: string; from: Date; to: Date; attached: boolean; detachPending: boolean };
      const work = new Map<string, Work>();
      for (const p of parts) {
        if (p.from && p.to && isExpired(p, now, deps.retentionDays)) {
          work.set(p.name, { name: p.name, from: p.from, to: p.to, attached: true, detachPending: p.detachPending });
        }
      }
      // Unfinished earlier runs: already detached, or detach interrupted.
      for (const l of logs) {
        if (work.has(l.partition_name)) continue;
        const attached = parts.find((p) => p.name === l.partition_name);
        if (!attached && !(await tableExists(c, l.partition_name))) {
          // Dropped by hand: nothing left to archive.
          await setStatus(c, l.partition_name, 'dropped');
          continue;
        }
        work.set(l.partition_name, {
          name: l.partition_name,
          from: l.range_from,
          to: l.range_to,
          attached: attached !== undefined,
          detachPending: attached?.detachPending ?? false,
        });
      }

      for (const w of work.values()) {
        try {
          await archiveOne(c, { ...deps, store }, w, logByName.get(w.name));
          summary.archived.push(w.name);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          summary.failed.push({ partition: w.name, error: msg });
          say(`partition ${w.name}: archive failed, will retry: ${msg}`);
          // Make sure a failed export did not leave an aborted transaction open.
          await c.query('ROLLBACK').catch(() => {});
          await c
            .query(
              `UPDATE partition_archive_log SET attempts = attempts + 1, last_error = $2 WHERE partition_name = $1`,
              [w.name, msg.slice(0, 500)],
            )
            .catch(() => {});
        }
      }
    } finally {
      await c.query(`SELECT pg_advisory_unlock(hashtext($1))`, [LOCK_NAME]).catch(() => {});
    }
    return summary;
  } finally {
    await c.end().catch(() => {});
  }
}

/** Background loop for the server process. Returns an async stop(). */
export function startPartitionManager(log: (msg: string) => void): () => Promise<void> {
  const adminUrl = process.env.ADMIN_DATABASE_URL;
  if (!adminUrl) {
    log('partition manager disabled: ADMIN_DATABASE_URL is not set');
    return async () => {};
  }
  const store = storeFromEnv();
  if (!store) {
    log('partition manager: S3_BUCKET not set, creating partitions only (no detach/archive/drop)');
  }
  const intervalMs = parsePositiveInt(process.env.PARTITION_INTERVAL_MS, 3_600_000);

  let inFlight: Promise<void> | null = null;
  let stopped = false;
  const tick = (): void => {
    if (inFlight || stopped) return;
    inFlight = (async () => {
      try {
        await runPartitionMaintenance({
          adminUrl,
          store,
          aheadDays: aheadDays(),
          retentionDays: retentionDays(),
          prefix: process.env.S3_PREFIX || 'archive',
          log,
        });
      } catch (err) {
        log(`partition manager error: ${err instanceof Error ? err.message : String(err)}`);
      }
    })().finally(() => {
      inFlight = null;
    });
  };
  const first = setTimeout(tick, 10_000);
  const timer = setInterval(tick, intervalMs);
  first.unref();
  timer.unref();
  return async () => {
    stopped = true;
    clearTimeout(first);
    clearInterval(timer);
    await inFlight;
  };
}
