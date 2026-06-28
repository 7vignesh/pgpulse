import pg from 'pg';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import process from 'node:process';

const { Client } = pg;

/**
 * Benchmark script: captures EXPLAIN (ANALYZE, BUFFERS) for the key analytics
 * queries WITHOUT and WITH the secondary indexes, saves the raw plans to
 * benchmarks/explain-analyze/, and prints a before/after summary table.
 *
 * Connects directly via PRIMARY_DATABASE_URL (session mode; DDL such as
 * DROP/CREATE INDEX must not go through PgBouncer transaction mode).
 *
 * Index definitions are PARSED from migrations/003_create_indexes.sql so the
 * recreated indexes match the real schema exactly (not hardcoded here).
 */

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');
const OUT_DIR = join(ROOT, 'benchmarks', 'explain-analyze');
const INDEX_MIGRATION = join(ROOT, 'migrations', '003_create_indexes.sql');

const SECONDARY_INDEXES = [
  'idx_events_ingested_brin',
  'idx_events_errors',
  'idx_events_dashboard',
  'idx_events_metadata',
] as const;

interface BenchQuery {
  name: string;
  sql: string;
}

const QUERIES: BenchQuery[] = [
  {
    name: 'overview_last_24h',
    sql: `SELECT
  COUNT(*) AS total_requests,
  COUNT(*) FILTER (WHERE status_code >= 400) AS error_count,
  ROUND(100.0 * COUNT(*) FILTER (WHERE status_code >= 400) / NULLIF(COUNT(*), 0), 2) AS error_rate,
  ROUND(AVG(latency_ms)) AS avg_latency_ms
FROM events
WHERE tenant_id = $1
  AND ingested_at > now() - INTERVAL '24 hours'`,
  },
  {
    name: 'endpoint_breakdown_7d',
    sql: `SELECT
  endpoint,
  COUNT(*) AS total_requests,
  COUNT(*) FILTER (WHERE status_code >= 400) AS error_count,
  ROUND(AVG(latency_ms)) AS avg_latency_ms
FROM events
WHERE tenant_id = $1
  AND ingested_at > now() - INTERVAL '7 days'
GROUP BY endpoint
ORDER BY total_requests DESC`,
  },
  {
    name: 'error_scan_30d',
    sql: `SELECT
  endpoint,
  status_code,
  COUNT(*) AS occurrences
FROM events
WHERE tenant_id = $1
  AND status_code >= 400
  AND ingested_at > now() - INTERVAL '30 days'
GROUP BY endpoint, status_code
ORDER BY occurrences DESC`,
  },
  {
    name: 'p95_latency_per_endpoint',
    sql: `SELECT
  endpoint,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency_ms
FROM events
WHERE tenant_id = $1
  AND ingested_at > now() - INTERVAL '7 days'
GROUP BY endpoint`,
  },
  {
    name: 'timeseries_hourly_30d',
    sql: `SELECT
  date_trunc('hour', ingested_at) AS hour,
  COUNT(*) AS total_requests
FROM events
WHERE tenant_id = $1
  AND ingested_at > now() - INTERVAL '30 days'
GROUP BY date_trunc('hour', ingested_at)
ORDER BY hour ASC`,
  },
];

/**
 * Parse CREATE INDEX statements from the migration file so re-creation matches
 * the real schema. Splits on semicolons and keeps statements that create one of
 * the known secondary indexes.
 */
function loadIndexCreateStatements(): string[] {
  const raw = readFileSync(INDEX_MIGRATION, 'utf8');
  // Strip line comments, then split into statements.
  const noComments = raw
    .split('\n')
    .filter((l) => !l.trimStart().startsWith('--'))
    .join('\n');
  return noComments
    .split(';')
    .map((s) => s.trim())
    .filter(
      (s) =>
        /create\s+index/i.test(s) &&
        SECONDARY_INDEXES.some((idx) => s.includes(idx)),
    );
}

interface Timing {
  planning: number | null;
  execution: number | null;
  text: string;
}

function parseTimings(planText: string): Timing {
  const plan = /Planning Time:\s+([\d.]+)\s*ms/.exec(planText);
  const exec = /Execution Time:\s+([\d.]+)\s*ms/.exec(planText);
  return {
    planning: plan ? Number(plan[1]) : null,
    execution: exec ? Number(exec[1]) : null,
    text: planText,
  };
}

async function explain(
  client: pg.Client,
  query: BenchQuery,
  tenantId: string,
): Promise<Timing> {
  const { rows } = await client.query<{ 'QUERY PLAN': string }>(
    `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ${query.sql}`,
    [tenantId],
  );
  const text = rows.map((r) => r['QUERY PLAN']).join('\n');
  return parseTimings(text);
}

function writePlanFile(
  query: BenchQuery,
  tenantId: string,
  withIndexes: boolean,
  planText: string,
): void {
  const suffix = withIndexes ? 'WITH_INDEXES' : 'WITHOUT_INDEXES';
  const header =
    `-- Query: ${query.name}\n` +
    `-- Tenant: ${tenantId}\n` +
    `-- Run at: ${new Date().toISOString()}\n` +
    `-- Indexes: ${withIndexes ? 'WITH' : 'WITHOUT'}\n\n`;
  writeFileSync(join(OUT_DIR, `${query.name}_${suffix}.txt`), header + planText + '\n');
}

function fmt(ms: number | null): string {
  return ms === null ? 'n/a' : `${ms.toFixed(2)}ms`;
}

async function main(): Promise<void> {
  const connectionString = process.env.PRIMARY_DATABASE_URL;
  if (!connectionString) {
    throw new Error('PRIMARY_DATABASE_URL is required to benchmark');
  }
  mkdirSync(OUT_DIR, { recursive: true });

  const client = new Client({ connectionString });
  await client.connect();

  try {
    // Use the first seeded tenant (most recently created "acme" group). Pick
    // the tenant with the most events so plans are meaningful.
    const { rows: trows } = await client.query<{ tenant_id: string }>(
      `SELECT tenant_id FROM events
        GROUP BY tenant_id
        ORDER BY COUNT(*) DESC
        LIMIT 1`,
    );
    if (trows.length === 0) {
      throw new Error('no events found — run `npm run seed` first');
    }
    const tenantId = trows[0].tenant_id;
    console.log(`Benchmarking against tenant ${tenantId}\n`);

    const createStatements = loadIndexCreateStatements();
    if (createStatements.length !== SECONDARY_INDEXES.length) {
      throw new Error(
        `expected ${SECONDARY_INDEXES.length} index statements, parsed ${createStatements.length}`,
      );
    }

    // --- Phase 1: WITHOUT indexes ---
    console.log('Dropping secondary indexes...');
    for (const idx of SECONDARY_INDEXES) {
      await client.query(`DROP INDEX IF EXISTS ${idx}`);
    }
    await client.query('ANALYZE events');

    const without: Record<string, Timing> = {};
    for (const q of QUERIES) {
      console.log(`  EXPLAIN (WITHOUT) ${q.name}`);
      const t = await explain(client, q, tenantId);
      without[q.name] = t;
      writePlanFile(q, tenantId, false, t.text);
    }

    // --- Phase 2: WITH indexes ---
    console.log('\nRe-creating secondary indexes...');
    for (const stmt of createStatements) {
      await client.query(stmt);
    }
    await client.query('ANALYZE events');

    const withIdx: Record<string, Timing> = {};
    for (const q of QUERIES) {
      console.log(`  EXPLAIN (WITH) ${q.name}`);
      const t = await explain(client, q, tenantId);
      withIdx[q.name] = t;
      writePlanFile(q, tenantId, true, t.text);
    }

    // --- Summary table ---
    console.log('\n\nBENCHMARK SUMMARY');
    const head = ['Query', 'Without Indexes', 'With Indexes', 'Speedup'];
    const rows: string[][] = QUERIES.map((q) => {
      const wo = without[q.name];
      const wi = withIdx[q.name];
      const speedup =
        wo.execution != null && wi.execution != null && wi.execution > 0
          ? `${(wo.execution / wi.execution).toFixed(1)}x`
          : 'n/a';
      return [
        q.name,
        `plan: ${fmt(wo.planning)} exec: ${fmt(wo.execution)}`,
        `plan: ${fmt(wi.planning)} exec: ${fmt(wi.execution)}`,
        speedup,
      ];
    });

    const widths = head.map((h, i) =>
      Math.max(h.length, ...rows.map((r) => r[i].length)),
    );
    const line = (cols: string[]): string =>
      cols.map((c, i) => c.padEnd(widths[i])).join('  ');

    console.log(line(head));
    console.log(widths.map((w) => '-'.repeat(w)).join('  '));
    for (const r of rows) console.log(line(r));

    console.log(`\nPlans written to ${OUT_DIR}`);
  } finally {
    await client.end();
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
