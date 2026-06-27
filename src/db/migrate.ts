import pg from 'pg';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';

const { Client } = pg;

/**
 * Sequential raw-SQL migration runner.
 *
 * Runs against ADMIN_DATABASE_URL — a DIRECT (session-mode) connection to
 * Postgres, NOT through PgBouncer. DDL such as CREATE INDEX, multi-statement
 * transactional DDL, and pg_cron scheduling require a stable session that
 * PgBouncer transaction mode cannot guarantee.
 *
 * Each migration file runs inside its own transaction and is recorded in
 * schema_migrations so re-runs are idempotent.
 */

const __dirname = dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = resolve(__dirname, '..', '..', 'migrations');

interface Migration {
  filename: string;
  sql: string;
}

function loadMigrations(): Migration[] {
  return readdirSync(MIGRATIONS_DIR)
    .filter((f) => f.endsWith('.sql'))
    .sort()
    .map((filename) => ({
      filename,
      sql: readFileSync(join(MIGRATIONS_DIR, filename), 'utf8'),
    }));
}

async function connect(): Promise<pg.Client> {
  const connectionString = process.env.ADMIN_DATABASE_URL;
  if (!connectionString) {
    throw new Error('ADMIN_DATABASE_URL is required for migrations');
  }
  const client = new Client({ connectionString });
  await client.connect();
  return client;
}

async function ensureMigrationsTable(client: pg.Client): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename   TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function appliedSet(client: pg.Client): Promise<Set<string>> {
  const { rows } = await client.query<{ filename: string }>(
    'SELECT filename FROM schema_migrations',
  );
  return new Set(rows.map((r) => r.filename));
}

async function up(): Promise<void> {
  const client = await connect();
  try {
    await ensureMigrationsTable(client);
    const applied = await appliedSet(client);
    const migrations = loadMigrations();

    let ran = 0;
    for (const m of migrations) {
      if (applied.has(m.filename)) {
        continue;
      }
      process.stdout.write(`-> applying ${m.filename} ... `);
      try {
        await client.query('BEGIN');
        await client.query(m.sql);
        await client.query(
          'INSERT INTO schema_migrations (filename) VALUES ($1)',
          [m.filename],
        );
        await client.query('COMMIT');
        process.stdout.write('ok\n');
        ran += 1;
      } catch (err) {
        await client.query('ROLLBACK');
        process.stdout.write('FAILED\n');
        throw new Error(
          `migration ${m.filename} failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    }

    if (ran === 0) {
      console.log('No pending migrations. Schema is up to date.');
    } else {
      console.log(`Applied ${ran} migration(s).`);
    }
  } finally {
    await client.end();
  }
}

async function status(): Promise<void> {
  const client = await connect();
  try {
    await ensureMigrationsTable(client);
    const applied = await appliedSet(client);
    const migrations = loadMigrations();
    console.log('Migration status:');
    for (const m of migrations) {
      console.log(`  [${applied.has(m.filename) ? 'x' : ' '}] ${m.filename}`);
    }
  } finally {
    await client.end();
  }
}

const cmd = process.argv[2] ?? 'up';

const run = cmd === 'status' ? status : up;
run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
