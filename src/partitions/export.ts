import zlib from 'node:zlib';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import type { Client } from 'pg';
import { ParquetWriter, fileWriter } from 'hyparquet-writer';
import { quoteIdent } from './plan.js';

/**
 * Export one detached partition to a compressed Parquet file.
 *
 * Rows are read through a server-side cursor in fixed-size chunks and each
 * chunk becomes one Parquet row group, so memory stays bounded however large
 * the partition is. The caller must give an idle session (no open transaction)
 * on a direct connection; this function opens and closes its own transaction.
 *
 * Column mapping (lossless):
 *   bigint -> INT64, uuid -> UUID (FLBA16), text -> UTF8, int2/int4 -> INT32,
 *   inet -> UTF8 (bare address for host entries, address/mask otherwise),
 *   jsonb -> JSON, timestamptz -> TIMESTAMP(MICROS, UTC) with exact microseconds.
 */

export const PARQUET_CODEC = 'GZIP';
export const parquetCompressors = {
  GZIP: (input: Uint8Array): Uint8Array => new Uint8Array(zlib.gzipSync(input)),
};
export const parquetDecompressors = {
  GZIP: (input: Uint8Array, outputLength: number): Uint8Array => {
    void outputLength;
    return new Uint8Array(zlib.gunzipSync(input));
  },
};

export const PARQUET_SCHEMA = [
  { name: 'root', num_children: 10 },
  { name: 'id', type: 'INT64', repetition_type: 'REQUIRED' },
  { name: 'tenant_id', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, repetition_type: 'REQUIRED', logical_type: { type: 'UUID' } },
  { name: 'endpoint', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
  { name: 'method', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
  { name: 'status_code', type: 'INT32', repetition_type: 'REQUIRED' },
  { name: 'latency_ms', type: 'INT32', repetition_type: 'REQUIRED' },
  { name: 'user_agent', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' },
  { name: 'ip_address', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' },
  { name: 'metadata', type: 'BYTE_ARRAY', converted_type: 'JSON', repetition_type: 'OPTIONAL' },
  { name: 'ingested_at', type: 'INT64', repetition_type: 'REQUIRED', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } },
] as const;

/** Row as returned by SELECT_SQL (everything as text so int8/numeric keep full precision). */
export interface ExportRow {
  id: string;
  tenant_id: string;
  endpoint: string;
  method: string;
  status_code: number;
  latency_ms: number;
  user_agent: string | null;
  ip_address: string | null;
  metadata: string | null;
  ingested_micros: string;
}

const SELECT_SQL = (table: string): string => `
  SELECT id::text AS id,
         tenant_id::text AS tenant_id,
         endpoint,
         method,
         status_code::int AS status_code,
         latency_ms,
         user_agent,
         CASE WHEN ip_address IS NULL THEN NULL
              WHEN masklen(ip_address) = CASE family(ip_address) WHEN 4 THEN 32 ELSE 128 END
                THEN host(ip_address)
              ELSE text(ip_address) END AS ip_address,
         metadata::text AS metadata,
         (extract(epoch FROM ingested_at) * 1000000)::bigint::text AS ingested_micros
    FROM ${table}`;

/** Convert fetched rows to Parquet column arrays. Pure. */
export function toColumns(rows: ExportRow[]): { name: string; data: unknown[] }[] {
  return [
    { name: 'id', data: rows.map((r) => BigInt(r.id)) },
    { name: 'tenant_id', data: rows.map((r) => r.tenant_id) },
    { name: 'endpoint', data: rows.map((r) => r.endpoint) },
    { name: 'method', data: rows.map((r) => r.method) },
    { name: 'status_code', data: rows.map((r) => r.status_code) },
    { name: 'latency_ms', data: rows.map((r) => r.latency_ms) },
    { name: 'user_agent', data: rows.map((r) => r.user_agent) },
    { name: 'ip_address', data: rows.map((r) => r.ip_address) },
    { name: 'metadata', data: rows.map((r) => r.metadata) },
    { name: 'ingested_at', data: rows.map((r) => BigInt(r.ingested_micros)) },
  ];
}

export interface ExportResult {
  rows: number;
  bytes: number;
  sha256: string;
}

async function sha256File(path: string): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path)) h.update(chunk as Buffer);
  return h.digest('hex');
}

export async function exportTableToParquet(
  client: Client,
  table: string,
  filePath: string,
  chunkRows = 50_000,
): Promise<ExportResult> {
  const writer = fileWriter(filePath);
  const pw = new ParquetWriter({
    writer,
    // The library's SchemaElement type is stricter than a literal array of const objects.
    schema: PARQUET_SCHEMA as unknown as ConstructorParameters<typeof ParquetWriter>[0]['schema'],
    codec: PARQUET_CODEC,
    compressors: parquetCompressors,
    kvMetadata: [{ key: 'source_table', value: table }],
    createdBy: 'pgpulse-partition-manager',
  });

  let total = 0;
  await client.query('BEGIN READ ONLY');
  try {
    await client.query(`DECLARE pgpulse_export NO SCROLL CURSOR FOR ${SELECT_SQL(quoteIdent(table))}`);
    for (;;) {
      const { rows } = await client.query<ExportRow>(`FETCH ${chunkRows} FROM pgpulse_export`);
      if (rows.length === 0) break;
      pw.write({
        columnData: toColumns(rows) as Parameters<typeof pw.write>[0]['columnData'],
        rowGroupSize: rows.length,
      });
      total += rows.length;
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
  pw.finish();

  const { statSync } = await import('node:fs');
  return { rows: total, bytes: statSync(filePath).size, sha256: await sha256File(filePath) };
}
