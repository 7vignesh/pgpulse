#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Run the k6 load suite with the official grafana/k6 image (no local install).
#
#   scripts/k6.sh [scenario]        scenario: mixed (default) | ingest | analytics
#
# Env passed through: BASE_URL, TENANTS, DURATION, THINK_S.
# The summary JSON is written to benchmarks/k6/results/<scenario>.json.
#
# BASE_URL defaults to the app on the host. From inside the container the host
# is host.docker.internal (Docker Desktop; on Linux this script adds the
# host-gateway mapping). To target the compose network instead, run with
# DOCKER_NETWORK=pgpulse_pgpulse BASE_URL=http://app:3000.
# ---------------------------------------------------------------------------
set -euo pipefail

SCENARIO="${1:-mixed}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/benchmarks/k6/results"

NETWORK_ARGS=(--add-host=host.docker.internal:host-gateway)
if [ -n "${DOCKER_NETWORK:-}" ]; then
  NETWORK_ARGS=(--network "$DOCKER_NETWORK")
fi

# MSYS_NO_PATHCONV keeps Git Bash on Windows from rewriting the container paths.
MSYS_NO_PATHCONV=1 docker run --rm -i \
  "${NETWORK_ARGS[@]}" \
  -v "$ROOT/benchmarks/k6:/k6" \
  -e SCENARIO="$SCENARIO" \
  -e BASE_URL="${BASE_URL:-http://host.docker.internal:3000}" \
  -e TENANTS="${TENANTS:-50}" \
  -e DURATION="${DURATION:-2m}" \
  -e THINK_S="${THINK_S:-0.2}" \
  grafana/k6:latest run \
  --summary-export "/k6/results/${SCENARIO}.json" \
  /k6/load.js
