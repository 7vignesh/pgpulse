import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { primaryPool, withTransaction } from '../db/pool.js';

/**
 * Ingest service. Writes go to the primary via PgBouncer. Every insert runs
 * inside a tenant-scoped transaction (SET LOCAL app.current_tenant_id) so RLS
 * on `events` enforces tenant isolation even at write time.
 */

export interface EventInput {
  endpoint: string;
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  status_code: number;
  latency_ms: number;
  user_agent?: string | null;
  ip_address?: string | null;
  metadata?: Record<string, unknown>;
  ingested_at?: string; // ISO; defaults to now() in DB
}

const COLUMNS = [
  'tenant_id',
  'endpoint',
  'method',
  'status_code',
  'latency_ms',
  'user_agent',
  'ip_address',
  'metadata',
  'ingested_at',
] as const;

/**
 * Insert a single event. Uses a parameterized multi-column INSERT.
 */
export async function insertEvent(tenantId: string, e: EventInput): Promise<{ id: string }> {
  return withTransaction(
    primaryPool,
    async (client) => {
      const { rows } = await client.query<{ id: string }>(
        `INSERT INTO events
           (tenant_id, endpoint, method, status_code, latency_ms,
            user_agent, ip_address, metadata, ingested_at)
         VALUES
           ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, COALESCE($9::timestamptz, now()))
         RETURNING id`,
        [
          tenantId,
          e.endpoint,
          e.method,
          e.status_code,
          e.latency_ms,
          e.user_agent ?? null,
          e.ip_address ?? null,
          JSON.stringify(e.metadata ?? {}),
          e.ingested_at ?? null,
        ],
      );
      return rows[0];
    },
    { tenantId },
  );
}

/**
 * Batch insert via a single multi-row INSERT inside one transaction.
 * Builds one parameterized statement with N value tuples — no per-row round
 * trips, no string interpolation of values.
 *
 * For very large batches COPY is faster; see insertEventsCopy.
 */
export async function insertEventsBatch(
  tenantId: string,
  events: EventInput[],
): Promise<{ inserted: number }> {
  if (events.length === 0) return { inserted: 0 };

  return withTransaction(
    primaryPool,
    async (client) => {
      const params: unknown[] = [];
      const tuples: string[] = [];

      events.forEach((e, i) => {
        const base = i * 9;
        tuples.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, ` +
            `$${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}::jsonb, ` +
            `COALESCE($${base + 9}::timestamptz, now()))`,
        );
        params.push(
          tenantId,
          e.endpoint,
          e.method,
          e.status_code,
          e.latency_ms,
          e.user_agent ?? null,
          e.ip_address ?? null,
          JSON.stringify(e.metadata ?? {}),
          e.ingested_at ?? null,
        );
      });

      const sql =
        `INSERT INTO events (${COLUMNS.join(', ')}) VALUES ${tuples.join(', ')}`;
      const res = await client.query(sql, params);
      return { inserted: res.rowCount ?? events.length };
    },
    { tenantId },
  );
}

/**
 * COPY-based bulk load for high-throughput ingest. Streams TSV into the
 * server's COPY machinery — the fastest path for large batches. Tenant scope
 * is still set via SET LOCAL for RLS.
 *
 * Exposed as an alternative; the batch route uses multi-row INSERT by default
 * for its richer error reporting, but COPY is wired here for benchmarking.
 */
export async function insertEventsCopy(
  tenantId: string,
  events: EventInput[],
): Promise<{ inserted: number }> {
  if (events.length === 0) return { inserted: 0 };

  return withTransaction(
    primaryPool,
    async (client) => {
      const stream = client.query(
        copyFrom(
          `COPY events (${COLUMNS.join(', ')})
           FROM STDIN WITH (FORMAT text, NULL '\\N')`,
        ),
      );

      const tsv = Readable.from(
        (function* () {
          for (const e of events) {
            const cols = [
              tenantId,
              esc(e.endpoint),
              e.method,
              String(e.status_code),
              String(e.latency_ms),
              e.user_agent != null ? esc(e.user_agent) : '\\N',
              e.ip_address != null ? e.ip_address : '\\N',
              esc(JSON.stringify(e.metadata ?? {})),
              e.ingested_at ?? new Date().toISOString(),
            ];
            yield cols.join('\t') + '\n';
          }
        })(),
      );

      await pipeline(tsv, stream);
      return { inserted: events.length };
    },
    { tenantId },
  );
}

// Escape tabs/newlines/backslashes for COPY text format.
function esc(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
}
