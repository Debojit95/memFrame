#!/usr/bin/env bash
set -euo pipefail

if [ -d ".venv/bin" ]; then
  PATH="$PWD/.venv/bin:$PATH"
fi

if [ -f ".env" ]; then
  set -a
  . ./.env
  set +a
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-$PWD/.uv-cache}"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"

: "${UPLOAD_CSV_FILEPATH:?Set UPLOAD_CSV_FILEPATH in .env}"
: "${UPLOAD_PARQUET_FILEPATH:?Set UPLOAD_PARQUET_FILEPATH in .env}"
: "${DUCKDB_UPLOAD_DB_BACKEND:?Set DUCKDB_UPLOAD_DB_BACKEND in .env}"
: "${DUCKDB_UPLOAD_DB_PARAMS:?Set DUCKDB_UPLOAD_DB_PARAMS in .env}"
: "${POSTGRES_UPLOAD_DB_BACKEND:?Set POSTGRES_UPLOAD_DB_BACKEND in .env}"
: "${POSTGRES_UPLOAD_DB_PARAMS:?Set POSTGRES_UPLOAD_DB_PARAMS in .env}"

find . \( -path "./src/*" -o -path "./tests/*" \) -name "__pycache__" -type d -exec rm -rf {} +

run_upload_test() {
  local upload_type="$1"
  local filepath="$2"
  local db_backend="$3"
  local db_params="$4"

  pytest tests/test_upload.py \
    --upload-type "$upload_type" \
    --filepath "$filepath" \
    --db-backend "$db_backend" \
    --db-params "$db_params"
}

for upload_type in csv parquet; do
  filepath_var="UPLOAD_${upload_type^^}_FILEPATH"
  run_upload_test "$upload_type" "${!filepath_var}" "$DUCKDB_UPLOAD_DB_BACKEND" "$DUCKDB_UPLOAD_DB_PARAMS"
  run_upload_test "$upload_type" "${!filepath_var}" "$POSTGRES_UPLOAD_DB_BACKEND" "$POSTGRES_UPLOAD_DB_PARAMS"
done

tox -r -p auto -e py310,py311,py312,py313
