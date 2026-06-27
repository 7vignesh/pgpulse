#!/bin/bash
# ---------------------------------------------------------------------------
# Entrypoint for the REPLICA. If the data dir is empty, base-backup from the
# primary and configure standby mode; otherwise start normally.
# ---------------------------------------------------------------------------
set -euo pipefail

PGDATA="/var/lib/postgresql/data"

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
  echo "replica: empty data dir, running pg_basebackup from primary..."

  # Wait for primary to accept connections.
  until pg_isready -h postgres -p 5432 -U "$REPLICATION_USER"; do
    echo "replica: waiting for primary..."
    sleep 2
  done

  export PGPASSWORD="$REPLICATION_PASSWORD"
  pg_basebackup \
    --host=postgres \
    --port=5432 \
    --username="$REPLICATION_USER" \
    --pgdata="$PGDATA" \
    --wal-method=stream \
    --write-recovery-conf \
    --progress \
    --verbose

  # standby.signal + primary_conninfo written by --write-recovery-conf.
  chmod 0700 "$PGDATA"
  echo "replica: base backup complete, starting standby"
fi

exec docker-entrypoint.sh postgres
