#!/usr/bin/env bash
set -uo pipefail

if [ -f ".env.test" ]; then
  set -a
  . ./.env.test
  set +a
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-$PWD/.uv-cache}"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$PWD/.matplotlib-cache}"
mkdir -p "$MPLCONFIGDIR"

: "${UPLOAD_CSV_FILEPATH:?Set UPLOAD_CSV_FILEPATH in .env.test}"
: "${UPLOAD_PARQUET_FILEPATH:?Set UPLOAD_PARQUET_FILEPATH in .env.test}"
: "${DUCKDB_UPLOAD_DB_BACKEND:?Set DUCKDB_UPLOAD_DB_BACKEND in .env.test}"
: "${DUCKDB_UPLOAD_DB_PARAMS:?Set DUCKDB_UPLOAD_DB_PARAMS in .env.test}"
: "${POSTGRES_UPLOAD_DB_BACKEND:?Set POSTGRES_UPLOAD_DB_BACKEND in .env.test}"
: "${POSTGRES_UPLOAD_DB_PARAMS:?Set POSTGRES_UPLOAD_DB_PARAMS in .env.test}"
: "${CLICKHOUSE_UPLOAD_DB_BACKEND:?Set CLICKHOUSE_UPLOAD_DB_BACKEND in .env.test}"
: "${CLICKHOUSE_UPLOAD_DB_PARAMS:?Set CLICKHOUSE_UPLOAD_DB_PARAMS in .env.test}"

COMMIT_CHECK_TMPDIR="${COMMIT_CHECK_TMPDIR:-${TMPDIR:-/tmp}/memframe-commit-checks}"
mkdir -p "$COMMIT_CHECK_TMPDIR"

find . \( -path "./src/*" -o -path "./tests/*" \) -name "__pycache__" -type d -exec rm -rf {} +

echo "==> Running commit checks via tests/run_tests.py"

uv run python tests/run_tests.py \
  --backend duckdb,postgres,clickhouse \
  --upload-type csv,parquet \
  --require-upload-file \
  --tox \
  "${INTEGRATION_MODE:+--warn-integration}"
rc=$?
if [ $rc -ne 0 ]; then
  echo "==> Commit checks FAILED (rc=$rc)" >&2
fi
exit $rc
