#!/bin/sh
# ---------------------------------------------------------------------------
# Build the PgBouncer userlist.txt with a SCRAM-SHA-256 secret that matches
# the Postgres role, then exec pgbouncer.
#
# We connect to Postgres once to read the role's stored SCRAM verifier from
# pg_shadow so the auth secret is guaranteed identical. This avoids hand-
# maintaining a hash.
# ---------------------------------------------------------------------------
set -eu

USERLIST=/etc/pgbouncer/userlist.txt

echo "pgbouncer: waiting for postgres to be ready..."
until pg_isready -h postgres -p 5432 -U "$POSTGRES_USER" >/dev/null 2>&1; do
  sleep 2
done

export PGPASSWORD="$POSTGRES_PASSWORD"
SECRET=$(psql -h postgres -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT passwd FROM pg_shadow WHERE usename = '$POSTGRES_USER'")

echo "\"$POSTGRES_USER\" \"$SECRET\"" > "$USERLIST"
echo "pgbouncer: userlist written for $POSTGRES_USER"

exec pgbouncer /etc/pgbouncer/pgbouncer.ini
