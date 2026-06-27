#!/bin/bash
# ---------------------------------------------------------------------------
# Runs once on PRIMARY first init (docker-entrypoint-initdb.d).
# Creates the replication role used by the streaming replica.
# ---------------------------------------------------------------------------
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${REPLICATION_USER}') THEN
      CREATE ROLE ${REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
    END IF;
  END
  \$\$;
EOSQL

echo "primary init: replication role '${REPLICATION_USER}' ready"
