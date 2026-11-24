import { from as copyFrom } from 'pg-copy-streams';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { primaryPool, withTransaction } from '../db/pool.js';
import { escCopyText as esc } from './copy-format.js';

/**
 * Ingest types, plus a text-format COPY helper. The HTTP path is the buffered
 * binary COPY pipeline (pipeline.ts, flush.ts).
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
              e.ip_address != null ? esc(e.ip_address) : '\\N',
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
