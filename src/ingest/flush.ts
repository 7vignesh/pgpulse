import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import type { Pool } from 'pg';
import { withTransaction } from '../db/pool.js';
import { encodeBinaryCopy, type ColumnType, type CopyValue } from './binary-copy.js';
import type { BufferedEvent } from './buffer.js';

/**
 * Database side of the buffer. Per tenant, one transaction:
 *   1. COPY the batch (binary format) into a temp staging table
 *   2. claim idempotency keys, then insert only the first occurrence of each
 *      newly claimed key (plus all events without a key) into `events`
 *
 * Everything happens on one backend inside one transaction, so it is safe
 * under PgBouncer transaction pooling: the temp table is created, used and
 * dropped (ON COMMIT DROP) before the backend is handed to another client.
 * The tenant is set with SET LOCAL, so RLS WITH CHECK on `events` rejects any
 * row whose tenant_id differs from the one the transaction is scoped to.
 */

const STAGE_TYPES: readonly ColumnType[] = [
  'int4', // seq
  'text', // idempotency_key
  'text', // endpoint
  'text', // method
  'int2', // status_code
  'int4', // latency_ms
  'text', // user_agent
  'text', // ip_address (cast to inet on insert)
  'text', // metadata (cast to jsonb on insert)
  'timestamptz', // ingested_at
];

const CREATE_STAGE = `
  CREATE TEMP TABLE ingest_stage (
    seq             int4        NOT NULL,
    idempotency_key text,
    endpoint        text        NOT NULL,
    method          text        NOT NULL,
    status_code     int2        NOT NULL,
    latency_ms      int4        NOT NULL,
    user_agent      text,
    ip_address      text,
    metadata        text        NOT NULL,
    ingested_at     timestamptz NOT NULL
  ) ON COMMIT DROP`;

const COPY_STAGE = 'COPY ingest_stage FROM STDIN WITH (FORMAT binary)';

const INSERT_FROM_STAGE = `
  WITH cand AS (
    SELECT DISTINCT ON (idempotency_key) seq, idempotency_key
      FROM ingest_stage
     WHERE idempotency_key IS NOT NULL
     ORDER BY idempotency_key, seq
  ), claimed AS (
    INSERT INTO ingest_keys (tenant_id, idempotency_key)
    SELECT $1::uuid, idempotency_key FROM cand
    ON CONFLICT DO NOTHING
    RETURNING idempotency_key
  )
  INSERT INTO events
    (tenant_id, endpoint, method, status_code, latency_ms,
     user_agent, ip_address, metadata, ingested_at)
  SELECT $1::uuid, s.endpoint, s.method, s.status_code, s.latency_ms,
         s.user_agent, s.ip_address::inet, s.metadata::jsonb, s.ingested_at
    FROM ingest_stage s
   WHERE s.idempotency_key IS NULL
      OR s.seq IN (SELECT c.seq FROM cand c JOIN claimed k USING (idempotency_key))
   ORDER BY s.seq`;

function toRow(e: BufferedEvent, seq: number): CopyValue[] {
  const ev = e.event;
  return [
    seq,
    e.idempotencyKey,
    ev.endpoint,
    ev.method,
    ev.status_code,
    ev.latency_ms,
    ev.user_agent ?? null,
    ev.ip_address ?? null,
    JSON.stringify(ev.metadata ?? {}),
    ev.ingested_at ? new Date(ev.ingested_at) : e.receivedAt,
  ];
}

/** Write one tenant's events. Returns the number of rows inserted. */
export async function writeTenantBatch(
  pool: Pool,
  tenantId: string,
  entries: BufferedEvent[],
): Promise<number> {
  const payload = encodeBinaryCopy(
    STAGE_TYPES,
    entries.map((e, i) => toRow(e, i)),
  );
  return withTransaction(
    pool,
    async (client) => {
      await client.query(CREATE_STAGE);
      const copy = client.query(copyFrom(COPY_STAGE));
      await pipeline(Readable.from([payload]), copy);
      const res = await client.query(INSERT_FROM_STAGE, [tenantId]);
      return res.rowCount ?? 0;
    },
    { tenantId },
  );
}

/**
 * FlushFn implementation: groups by tenant (RLS needs one tenant per
 * transaction) and isolates failures so one tenant's bad batch cannot block
 * the others. Returns the entries of the tenants whose transaction failed.
 */
export function makeFlush(
  pool: Pool,
  log: (msg: string) => void = () => {},
): (batch: BufferedEvent[]) => Promise<BufferedEvent[]> {
  return async (batch) => {
    const byTenant = new Map<string, BufferedEvent[]>();
    for (const e of batch) {
      const list = byTenant.get(e.tenantId);
      if (list) list.push(e);
      else byTenant.set(e.tenantId, [e]);
    }

    const failed: BufferedEvent[] = [];
    await Promise.all(
      [...byTenant].map(async ([tenantId, entries]) => {
        try {
          await writeTenantBatch(pool, tenantId, entries);
        } catch (err) {
          log(
            `ingest write failed for tenant ${tenantId}: ${
              err instanceof Error ? err.message : String(err)
            }`,
          );
          failed.push(...entries);
        }
      }),
    );
    return failed;
  };
}
