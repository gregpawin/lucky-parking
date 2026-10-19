#!/bin/bash
# Dev compose healthcheck: Postgres ready AND first-boot init finished.
# Init scripts can take hours (citations CSV); API/web wait on service_healthy.
set -euo pipefail

# The temporary initialization server listens only on a Unix socket. Wait for
# TCP so a healthcheck cannot mark a half-finished import as ready.
pg_isready -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1 || exit 1

MARKER="/var/lib/postgresql/data/.lucky_parking_init_done"
if [[ -f "${MARKER}" ]]; then
  exit 0
fi

# Legacy volumes (loaded before the marker existed): citations present => done.
if psql -v ON_ERROR_STOP=0 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
  "SELECT 1 FROM citations LIMIT 1" 2>/dev/null | grep -q 1; then
  # Healthchecks run as root; writing the marker here can race with the
  # postgres-owned init script and make its final touch fail permissions.
  exit 0
fi

# First boot: boundaries/citations init still running.
exit 1
