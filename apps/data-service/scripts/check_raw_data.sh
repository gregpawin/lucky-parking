#!/bin/bash
# Host-side preflight: ensure raw_data/ and a Parking_Citations_*.csv exist
# before first-boot docker compose (which mounts ./raw_data → /raw_data).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Read only these values, rather than executing an environment file as shell code.
read_setting() {
  if [[ -f "${ROOT}/.env" ]]; then
    sed -n "s/^$1=//p" "${ROOT}/.env" | tail -n 1 | tr -d '\r' | sed "s/^[\"']//; s/[\"']$//"
  fi
}
if [[ "${SKIP_CITATIONS_LOAD:-$(read_setting SKIP_CITATIONS_LOAD)}" == "1" ]]; then
  echo "SKIP_CITATIONS_LOAD=1; CSV check skipped."
  exit 0
fi
RAW_DIR="${CITATIONS_DATA_DIR:-$(read_setting CITATIONS_DATA_DIR)}"
RAW_DIR="${RAW_DIR:-${ROOT}/raw_data}"
if [[ "${RAW_DIR}" != /* && ! "${RAW_DIR}" =~ ^[A-Za-z]:[/\\] ]]; then
  RAW_DIR="${ROOT}/${RAW_DIR}"
fi

if [[ ! -d "${RAW_DIR}" ]]; then
  echo "ERROR: missing directory: ${RAW_DIR}" >&2
  echo "Create it and add a city dump, e.g. Parking_Citations_YYYYMMDD.csv" >&2
  exit 1
fi

shopt -s nullglob
csv_candidates=("${RAW_DIR}"/Parking_Citations_*.csv)
if ((${#csv_candidates[@]} == 0)); then
  echo "ERROR: no Parking_Citations_*.csv in ${RAW_DIR}" >&2
  exit 1
fi

CSV="$(printf '%s\n' "${csv_candidates[@]}" | sort | tail -n 1)"
if [[ ! -s "${CSV}" ]]; then
  echo "ERROR: citations CSV missing or empty: ${CSV}" >&2
  exit 1
fi

echo "OK: ${CSV} ($(du -h "${CSV}" | awk '{print $1}'))"
