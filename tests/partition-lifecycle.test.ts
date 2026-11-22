import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import process from 'node:process';
import zlib from 'node:zlib';
import { S3Client } from '@aws-sdk/client-s3';
import { parquetReadObjects } from 'hyparquet';
import { runPartitionMaintenance } from '../src/partitions/manager.js';
import { parseBound } from '../src/partitions/plan.js';
import { S3Store, parquetRowCount, type ObjectStore } from '../src/partitions/store.js';
import { parquetDecompressors } from '../src/partitions/export.js';

/**
 * Partition lifecycle against a real Postgres and a real S3-compatible store
 * (MinIO). Needs both: ADMIN_DATABASE_URL and S3_ENDPOINT + S3_ACCESS_KEY +
 * S3_SECRET_KEY + S3_BUCKET. Skipped when either is unavailable.
 */

const { Client } = pg;

async function dbReachable(): Promise<boolean> {
  const cs = process.env.ADMIN_DATABASE_URL;
  if (!cs) return false;
  const c = new Client({ connectionString: cs, connectionTimeoutMillis: 2000 });
  try {
    await c.connect();
    await c.query('SELECT to_regclass($1)', ['partition_archive_log']);
    await c.end();
    return true;
  } catch {
    try { await c.end(); } catch { /* ignore */ }
    return false;
  }
}

async function s3Reachable(): Promise<boolean> {
  if (!process.env.S3_ENDPOINT || !process.env.S3_BUCKET) return false;
  try {
    const r = await fetch(new URL('/minio/health/live', process.env.S3_ENDPOINT), { signal: AbortSignal.timeout(2000) });
    return r.ok;
  } catch {
    return false;
  }
}

const reachable = (await dbReachable()) && (await s3Reachable());
const d = reachable ? describe : describe.skip;

const DAY = 86_400_000;
const dayStart = (offsetDays: number): Date =>
  new Date(Math.floor(Date.now() / DAY) * DAY + offsetDays * DAY);
const name = (day: Date): string => `events_${day.toISOString().slice(0, 10).replace(/-/g, '')}`;

d('integration: partition lifecycle + Parquet archival (MinIO)', () => {
  let admin: pg.Client;
  let store: S3Store;
  let s3: S3Client;
  let tenantId: string;
  const RETENTION = 30;
  const base = { aheadDays: 7, retentionDays: RETENTION, prefix: `itest-${Date.now()}` };
  const run = (over: Partial<Parameters<typeof runPartitionMaintenance>[0]> = {}) =>
    runPartitionMaintenance({ adminUrl: process.env.ADMIN_DATABASE_URL!, store, ...base, ...over });

  const q = async <T extends pg.QueryResultRow = Record<string, unknown>>(sql: string, p: unknown[] = []) =>
    (await admin.query<T>(sql, p)).rows;
  const exists = async (t: string): Promise<boolean> =>
    (await q<{ r: string | null }>('SELECT to_regclass($1)::text AS r', [t]))[0].r !== null;
  const attached = async (t: string): Promise<boolean> =>
    (await q('SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass AND c.relname = $2', ['events', t])).length > 0;
  const logRow = async (t: string) =>
    (await q<{ status: string; row_count: string | null; s3_key: string | null; attempts: number; last_error: string | null }>(
      'SELECT status, row_count, s3_key, attempts, last_error FROM partition_archive_log WHERE partition_name = $1', [t]))[0];

  /** A standalone daily partition `ago` days in the past, with n rows. */
  const makeOldPartition = async (ago: number, n: number): Promise<{ table: string; from: Date }> => {
    const from = dayStart(-ago);
    const table = name(from);
    const to = new Date(from.getTime() + DAY);
    await admin.query(`DROP TABLE IF EXISTS ${table}`);
    await admin.query(`CREATE TABLE ${table} (LIKE events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`);
    await admin.query(
      `INSERT INTO ${table} (tenant_id, endpoint, method, status_code, latency_ms, user_agent, ip_address, metadata, ingested_at)
       SELECT $1, '/old/' || (g % 7), (ARRAY['GET','POST','PUT'])[1 + g % 3], 200 + (g % 5) * 100, g,
              CASE WHEN g % 4 = 0 THEN NULL ELSE 'ua ' || g END,
              CASE WHEN g % 3 = 0 THEN NULL WHEN g % 3 = 1 THEN '10.0.0.' || (g % 250) ELSE '10.0.0.0/8' END::inet,
              jsonb_build_object('n', g, 'note', 'tab' || E'\\t' || 'nl' || E'\\n'),
              $2::timestamptz + g * interval '1 second' + (g % 1000) * interval '1 microsecond'
         FROM generate_series(1, $3::int) g`,
      [tenantId, from, n],
    );
    await admin.query(`ALTER TABLE events ATTACH PARTITION ${table} FOR VALUES FROM ('${from.toISOString()}') TO ('${to.toISOString()}')`);
    return { table, from };
  };

  beforeAll(async () => {
    admin = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await admin.connect();
    await admin.query(`SET TIME ZONE 'UTC'`);
    s3 = new S3Client({
      region: 'us-east-1',
      endpoint: process.env.S3_ENDPOINT,
      forcePathStyle: true,
      credentials: { accessKeyId: process.env.S3_ACCESS_KEY!, secretAccessKey: process.env.S3_SECRET_KEY! },
    });
    store = new S3Store(s3, process.env.S3_BUCKET!);
    const t = await admin.query<{ id: string }>(`INSERT INTO tenants (name) VALUES ('partition-itest') RETURNING id`);
    tenantId = t.rows[0].id;
  });

  afterAll(async () => {
    for (const ago of [200, 150, 120, 100, 95]) await admin?.query(`DROP TABLE IF EXISTS ${name(dayStart(-ago))}`).catch(() => {});
    await admin?.query(`DELETE FROM partition_archive_log WHERE partition_name LIKE 'events_2%' AND partition_name NOT IN (SELECT relname FROM pg_class)`).catch(() => {});
    await admin?.query('DELETE FROM tenants WHERE id = $1', [tenantId]).catch(() => {});
    await admin?.end();
    s3?.destroy();
  });

  it('pre-creates daily partitions through day + 7 and is idempotent', async () => {
    const first = await run();
    expect(first.skipped).toBe(false);
    const bounds = (
      await q<{ b: string }>(
        `SELECT pg_get_expr(c.relpartbound, c.oid) AS b
           FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
          WHERE i.inhparent = 'events'::regclass`,
      )
    ).map((r) => parseBound(r.b));
    // Every day from today through today + 7 is covered by some partition.
    for (let i = 0; i <= 7; i++) {
      const start = dayStart(i).getTime();
      const covered = bounds.some((p) => p.from && p.to && p.from.getTime() <= start && p.to.getTime() >= start + DAY);
      expect(covered).toBe(true);
    }
    const second = await run();
    expect(second.created).toBe(0);
  });

  it('archives an expired partition: detach -> parquet -> verify -> drop, with every row intact', async () => {
    const { table, from } = await makeOldPartition(200, 2500);
    const expected = await q<{ id: string; endpoint: string; method: string; status_code: number; latency_ms: number; user_agent: string | null; ip: string | null; metadata: unknown; micros: string }>(
      `SELECT id::text AS id, endpoint, method, status_code, latency_ms, user_agent,
              CASE WHEN ip_address IS NULL THEN NULL WHEN masklen(ip_address) IN (32, 128) THEN host(ip_address) ELSE text(ip_address) END AS ip,
              metadata, (extract(epoch FROM ingested_at) * 1000000)::bigint::text AS micros
         FROM ${table} t ORDER BY t.id`,
    );
    expect(expected).toHaveLength(2500);

    const summary = await run();
    expect(summary.failed).toEqual([]);
    expect(summary.archived).toContain(table);

    expect(await exists(table)).toBe(false); // dropped
    const log = await logRow(table);
    expect(log.status).toBe('dropped');
    expect(Number(log.row_count)).toBe(2500);

    // The object is a readable, compressed Parquet file with the same rows.
    const key = log.s3_key!;
    expect(key).toBe(`${base.prefix}/events/dt=${from.toISOString().slice(0, 10)}/${table}.parquet`);
    expect(await parquetRowCount(store, key)).toBe(2500);

    const head = await store.head(key);
    const raw = await s3.send(new (await import('@aws-sdk/client-s3')).GetObjectCommand({ Bucket: process.env.S3_BUCKET!, Key: key }));
    const bytes = await raw.Body!.transformToByteArray();
    expect(head!.size).toBe(bytes.length);
    const file = { byteLength: bytes.length, slice: (a: number, b?: number) => bytes.buffer.slice(bytes.byteOffset + a, bytes.byteOffset + (b ?? bytes.length)) as ArrayBuffer };
    const rows = (await parquetReadObjects({ file, utf8: true, compressors: { GZIP: (b: Uint8Array) => new Uint8Array(zlib.gunzipSync(b)) } })) as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(2500);
    rows.sort((a, b) => Number(a.id) - Number(b.id));
    for (const i of [0, 1, 7, 999, 2499]) {
      const r = rows[i];
      const e = expected[i];
      expect(String(r.id)).toBe(e.id);
      expect(r.tenant_id).toBe(tenantId);
      expect(r.endpoint).toBe(e.endpoint);
      expect(r.method).toBe(e.method);
      expect(r.status_code).toBe(e.status_code);
      expect(r.latency_ms).toBe(e.latency_ms);
      expect(r.user_agent).toBe(e.user_agent);
      expect(r.ip_address).toBe(e.ip);
      expect(JSON.parse(r.metadata as string)).toEqual(e.metadata);
      // exact microseconds
      expect((r.ingested_at as Date).getTime()).toBe(Math.floor(Number(BigInt(e.micros) / 1000n)));
    }
    // compression applied: well under the raw text size
    expect(bytes.length).toBeLessThan(2500 * 120);
    void parquetDecompressors;
  });

  it('detaches without blocking inserts into events while it waits on a reader', async () => {
    const { table } = await makeOldPartition(150, 3000);
    // A reader holds the old partition (ACCESS SHARE) for 1.5s. A plain DETACH needs
    // ACCESS EXCLUSIVE on the parent, so it would queue behind the reader and every
    // insert and query behind it. DETACH CONCURRENTLY waits only on the partition.
    const holder = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await holder.connect();
    await holder.query('BEGIN');
    await holder.query(`SELECT count(*) FROM ${table}`);

    const writer = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await writer.connect();
    const stamps: number[] = [];
    let stop = false;
    const loop = (async () => {
      while (!stop) {
        await writer.query(
          `INSERT INTO events (tenant_id, endpoint, method, status_code, latency_ms) VALUES ($1, '/live', 'GET', 200, 1)`,
          [tenantId],
        );
        stamps.push(Date.now());
        await new Promise((r) => setTimeout(r, 5));
      }
    })();

    const t0 = Date.now();
    let done = false;
    const running = run().then((r) => { done = true; return r; });
    await new Promise((r) => setTimeout(r, 1500));
    expect(done).toBe(false); // the detach really is waiting on the reader
    await holder.query('ROLLBACK');
    await holder.end();
    const summary = await running;
    stop = true;
    await loop;
    await writer.end();

    const waiting = stamps.filter((t) => t >= t0 + 300 && t <= t0 + 1500);
    // Baseline here is ~16 inserts/s (timer granularity + round trip), so only require
    // that inserts kept flowing; the max-gap check below is what proves nothing blocked.
    expect(waiting.length).toBeGreaterThan(8);
    let maxGap = 0;
    for (let i = 1; i < waiting.length; i++) maxGap = Math.max(maxGap, waiting[i] - waiting[i - 1]);
    // A plain DETACH would stall every insert for the full 1.5s the reader holds the lock.
    expect(maxGap).toBeLessThan(500);
    expect(summary.archived).toContain(table);
    expect(await exists(table)).toBe(false);
  });

  it('keeps the table when the upload fails, then resumes and completes on the next run', async () => {
    const { table } = await makeOldPartition(120, 400);
    const failing: ObjectStore = {
      put: async () => { throw new Error('simulated S3 outage'); },
      head: (k) => store.head(k),
      readRange: (k, a, b) => store.readRange(k, a, b),
    };
    const failed = await run({ store: failing });
    expect(failed.failed.map((f) => f.partition)).toContain(table);
    expect(await exists(table)).toBe(true); // NOT dropped
    expect(await attached(table)).toBe(false); // but detached
    const mid = await logRow(table);
    expect(mid.status).toBe('detached');
    expect(mid.attempts).toBeGreaterThanOrEqual(1);
    expect(mid.last_error).toMatch(/simulated S3 outage/);

    const ok = await run();
    expect(ok.archived).toContain(table);
    expect(await exists(table)).toBe(false);
    expect((await logRow(table)).status).toBe('dropped');
  });

  it('refuses to drop on a row-count mismatch (corrupt or truncated archive)', async () => {
    const { table } = await makeOldPartition(100, 300);
    // Uploads a Parquet file that is valid but for a different (smaller) table.
    const lying: ObjectStore = {
      put: async (key, _file, meta) => {
        void meta;
        const { exportTableToParquet } = await import('../src/partitions/export.js');
        const { mkdtemp } = await import('node:fs/promises');
        const { tmpdir } = await import('node:os');
        const { join } = await import('node:path');
        const c = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
        await c.connect();
        try {
          await c.query(`CREATE TEMP TABLE liar AS SELECT * FROM ${table} LIMIT 10`);
          const f = join(await mkdtemp(join(tmpdir(), 'liar-')), 'x.parquet');
          await exportTableToParquet(c, 'liar', f);
          await store.put(key, f, {});
        } finally {
          await c.end();
        }
      },
      head: (k) => store.head(k),
      readRange: (k, a, b) => store.readRange(k, a, b),
    };
    const res = await run({ store: lying });
    expect(res.failed.find((f) => f.partition === table)?.error).toMatch(/mismatch|size/);
    expect(await exists(table)).toBe(true);
    expect((await logRow(table)).status).not.toBe('dropped');
    // Honest retry repairs it (the object is overwritten).
    const ok = await run();
    expect(ok.archived).toContain(table);
    expect(await exists(table)).toBe(false);
  });

  it('finalizes a detach that was interrupted (detach-pending state)', async () => {
    const { table } = await makeOldPartition(95, 120);
    // Start DETACH CONCURRENTLY and cancel it after it has marked the partition pending:
    // an open transaction holding the partition forces the detach to wait in phase 2.
    const holder = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await holder.connect();
    await holder.query('BEGIN');
    await holder.query(`SELECT count(*) FROM ${table}`);
    const detacher = new Client({ connectionString: process.env.ADMIN_DATABASE_URL });
    await detacher.connect();
    const pending = detacher.query(`ALTER TABLE events DETACH PARTITION ${table} CONCURRENTLY`).catch(() => {});
    let sawPending = false;
    for (let i = 0; i < 50 && !sawPending; i++) {
      await new Promise((r) => setTimeout(r, 100));
      sawPending = (await q<{ p: boolean }>(
        `SELECT inhdetachpending AS p FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = $1`, [table],
      ))[0]?.p === true;
      if (sawPending) break;
    }
    // Interrupt the waiting detach (what a crash/cancel leaves behind).
    await admin.query(`SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE 'ALTER TABLE events DETACH PARTITION%' AND pid <> pg_backend_pid()`);
    await pending;
    await holder.query('ROLLBACK');
    await holder.end();
    await detacher.end();
    expect(sawPending).toBe(true);
    const state = await q<{ p: boolean }>(`SELECT inhdetachpending AS p FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = $1`, [table]);
    expect(state[0]?.p).toBe(true); // still attached, detach pending

    const res = await run();
    expect(res.failed).toEqual([]);
    expect(res.archived).toContain(table);
    expect(await exists(table)).toBe(false);
  });

  it('two managers at once: only one works, the other skips (advisory lock)', async () => {
    const { table } = await makeOldPartition(100, 200);
    const [a, b] = await Promise.all([run(), run()]);
    expect([a.skipped, b.skipped].filter(Boolean).length).toBeGreaterThanOrEqual(1);
    const worked = [a, b].filter((r) => !r.skipped);
    const archivedTimes = worked.flatMap((r) => r.archived).filter((t) => t === table).length;
    expect(archivedTimes).toBe(1);
    expect(await exists(table)).toBe(false);
    expect((await logRow(table)).status).toBe('dropped');
  });

  it('with no object store it only creates partitions and never detaches or drops', async () => {
    const { table } = await makeOldPartition(100, 50);
    const res = await run({ store: null });
    expect(res.archiveEnabled).toBe(false);
    expect(res.archived).toEqual([]);
    expect(await attached(table)).toBe(true);
    expect(await exists(table)).toBe(true);
    // clean up through the normal path
    await run();
    expect(await exists(table)).toBe(false);
  });
});
