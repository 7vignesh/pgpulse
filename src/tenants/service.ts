import { primaryPool } from '../db/pool.js';

export interface Tenant {
  id: string;
  name: string;
  api_key: string;
  plan: 'free' | 'pro' | 'enterprise';
  created_at: string;
}

export type PublicTenant = Omit<Tenant, 'api_key'>;

/**
 * Tenant administration. All logic lives here; routes stay thin.
 */

export async function createTenant(
  name: string,
  plan: Tenant['plan'] = 'free',
): Promise<Tenant> {
  // api_key + id are DB-generated defaults; RETURNING gives us the new row.
  const { rows } = await primaryPool.query<Tenant>(
    `INSERT INTO tenants (name, plan)
     VALUES ($1, $2)
     RETURNING id, name, api_key, plan, created_at`,
    [name, plan],
  );
  return normalizeTimestamps(rows[0]);
}

export async function getTenant(id: string): Promise<PublicTenant | null> {
  const { rows } = await primaryPool.query<PublicTenant>(
    `SELECT id, name, plan, created_at FROM tenants WHERE id = $1`,
    [id],
  );
  return rows[0] ? normalizeTimestamps(rows[0]) : null;
}

// created_at is a timestamptz, which pg returns as a Date object; normalize it
// to an ISO string so the value matches the declared string type.
function normalizeTimestamps<T extends { created_at: string }>(row: T): T {
  const createdAt = row.created_at as unknown;
  return createdAt instanceof Date
    ? { ...row, created_at: createdAt.toISOString() }
    : row;
}

/**
 * Rotate a tenant's API key atomically. The new key is generated server-side
 * via gen_random_bytes so the plaintext only ever exists in the DB + response.
 * A single UPDATE is already atomic, so no explicit transaction is needed.
 */
export async function rotateApiKey(id: string): Promise<{ api_key: string } | null> {
  const { rows } = await primaryPool.query<{ api_key: string }>(
    `UPDATE tenants
       SET api_key = encode(gen_random_bytes(32), 'hex')
     WHERE id = $1
     RETURNING api_key`,
    [id],
  );
  return rows[0] ?? null;
}
