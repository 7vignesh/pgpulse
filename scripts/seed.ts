import pg from 'pg';
import process from 'node:process';

const { Client } = pg;

/**
 * Seed script: inserts 1,000,000 realistic events across 3 fresh tenants.
 *
 * Connects directly via PRIMARY_DATABASE_URL (session mode is fine for
 * seeding; no PgBouncer needed). Uses multi-row INSERT in batches of 1000,
 * each batch wrapped in a transaction. No ORM, no row-by-row inserts.
 */

const TOTAL_EVENTS = 1_000_000;
const BATCH_SIZE = 1000;
const BATCHES = TOTAL_EVENTS / BATCH_SIZE;

const TENANT_NAMES = ['acme', 'stripe-clone', 'devtools-co'] as const;

// --- weighted pickers --------------------------------------------------------

type Weighted<T> = [T, number][];

function makePicker<T>(weighted: Weighted<T>): () => T {
  const total = weighted.reduce((s, [, w]) => s + w, 0);
  // Precompute cumulative thresholds for O(n) pick (n is tiny).
  const cum: [T, number][] = [];
  let acc = 0;
  for (const [val, w] of weighted) {
    acc += w / total;
    cum.push([val, acc]);
  }
  return () => {
    const r = Math.random();
    for (const [val, threshold] of cum) {
      if (r <= threshold) return val;
    }
    return cum[cum.length - 1][0];
  };
}

const pickEndpoint = makePicker<string>([
  ['/api/login', 25],
  ['/api/checkout', 15],
  ['/api/products', 20],
  ['/api/search', 20],
  ['/api/user/profile', 10],
  ['/api/orders', 10],
]);

const pickMethod = makePicker<string>([
  ['GET', 60],
  ['POST', 30],
  ['PUT', 5],
  ['DELETE', 5],
]);

const pickStatus = makePicker<number>([
  [200, 70],
  [201, 10],
  [400, 8],
  [401, 5],
  [404, 4],
  [500, 3],
]);

const METADATA_POOL = [
  {},
  { region: 'us-east-1' },
  { region: 'eu-west-1', version: 'v2' },
  { user_tier: 'enterprise' },
];

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
  'curl/8.4.0',
  'PostmanRuntime/7.36.0',
];

const IP_POOL = [
  '203.0.113.10', '203.0.113.42', '198.51.100.7', '198.51.100.88', '192.0.2.15',
  '192.0.2.200', '203.0.113.77', '198.51.100.150', '192.0.2.33', '203.0.113.5',
  '198.51.100.21', '192.0.2.99', '203.0.113.130', '198.51.100.44', '192.0.2.61',
  '203.0.113.201', '198.51.100.99', '192.0.2.4', '203.0.113.250', '198.51.100.180',
];

const NINETY_DAYS_MS = 90 * 24 * 60 * 60 * 1000;

function randomItem<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomLatency(): number {
  // Long-tail: squared uniform biases toward low values.
  return Math.floor(Math.pow(Math.random(), 2) * 1990) + 10;
}

function randomIngestedAt(): Date {
  return new Date(Date.now() - Math.random() * NINETY_DAYS_MS);
}

// --- main --------------------------------------------------------------------

async function main(): Promise<void> {
  const connectionString = process.env.PRIMARY_DATABASE_URL;
  if (!connectionString) {
    throw new Error('PRIMARY_DATABASE_URL is required to seed');
  }

  const client = new Client({ connectionString });
  await client.connect();

  const start = Date.now();

  try {
    // Create 3 fresh tenants (do not hardcode UUIDs).
    const tenantIds: string[] = [];
    for (const name of TENANT_NAMES) {
      const { rows } = await client.query<{ id: string }>(
        `INSERT INTO tenants (name, plan) VALUES ($1, 'pro') RETURNING id`,
        [name],
      );
      tenantIds.push(rows[0].id);
      console.log(`created tenant ${name} -> ${rows[0].id}`);
    }

    // Build the parameterized multi-row INSERT template once: 9 cols x 1000.
    const COLS = 9;
    const tuples: string[] = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
      const b = i * COLS;
      tuples.push(
        `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},` +
          `$${b + 6},$${b + 7},$${b + 8}::jsonb,$${b + 9})`,
      );
    }
    const insertSql =
      `INSERT INTO events
         (tenant_id, endpoint, method, status_code, latency_ms,
          user_agent, ip_address, metadata, ingested_at)
       VALUES ${tuples.join(',')}`;

    let inserted = 0;
    for (let batch = 0; batch < BATCHES; batch++) {
      const params: unknown[] = [];
      for (let i = 0; i < BATCH_SIZE; i++) {
        params.push(
          randomItem(tenantIds),
          pickEndpoint(),
          pickMethod(),
          pickStatus(),
          randomLatency(),
          randomItem(USER_AGENTS),
          randomItem(IP_POOL),
          JSON.stringify(randomItem(METADATA_POOL)),
          randomIngestedAt(),
        );
      }

      await client.query('BEGIN');
      await client.query(insertSql, params);
      await client.query('COMMIT');

      inserted += BATCH_SIZE;
      if ((batch + 1) % 100 === 0) {
        console.log(
          `Seeded ${inserted.toLocaleString()} / ${TOTAL_EVENTS.toLocaleString()} events`,
        );
      }
    }

    // Update planner stats so the benchmark uses realistic estimates.
    await client.query('ANALYZE events');

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);

    // Per-tenant counts.
    const { rows: perTenant } = await client.query<{ name: string; count: string }>(
      `SELECT t.name, COUNT(*)::bigint AS count
         FROM events e JOIN tenants t ON t.id = e.tenant_id
        WHERE t.id = ANY($1::uuid[])
        GROUP BY t.name
        ORDER BY count DESC`,
      [tenantIds],
    );

    console.log('\n=== Seed complete ===');
    console.log(`Total events inserted: ${inserted.toLocaleString()}`);
    console.log('Events per tenant:');
    for (const r of perTenant) {
      console.log(`  ${r.name.padEnd(14)} ${Number(r.count).toLocaleString()}`);
    }
    console.log(`Time taken: ${elapsed}s`);
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
