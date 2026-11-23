# 0002. Tenant isolation with RLS + `SET LOCAL`

Status: Accepted

## Context

All tenants share the `events` table. A missing `WHERE tenant_id = ...` in any
one query would leak another customer's data.

## Decision

Enable and force Row-Level Security on `events` with a policy comparing
`tenant_id` to `current_setting('app.current_tenant_id', true)`. Every tenant
query runs inside `withTransaction(..., { tenantId })` (`src/db/pool.ts`),
which validates the id as a UUID and issues `SET LOCAL app.current_tenant_id`.
Queries also keep their explicit `tenant_id` filter.

## Consequences

- Defence in depth: a query that forgets its filter still sees only its own
  tenant's rows.
- Fails closed: with the setting unset the policy evaluates to NULL and no
  rows match.
- `SET LOCAL` is scoped to the transaction, which makes it safe under PgBouncer
  transaction pooling (ADR 0001). Every tenant query must run in a transaction.
- `FORCE` applies the policy to the table owner too; superusers and roles with
  `BYPASSRLS` still bypass it, so maintenance jobs that need all tenants must
  use such a role knowingly.
