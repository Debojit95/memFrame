#!/usr/bin/env bash
set -uo pipefail

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
export MPLCONFIGDIR="${MPLCONFIGDIR:-$PWD/.matplotlib-cache}"
mkdir -p "$MPLCONFIGDIR"

: "${UPLOAD_CSV_FILEPATH:?Set UPLOAD_CSV_FILEPATH in .env}"
: "${UPLOAD_PARQUET_FILEPATH:?Set UPLOAD_PARQUET_FILEPATH in .env}"
: "${DUCKDB_UPLOAD_DB_BACKEND:?Set DUCKDB_UPLOAD_DB_BACKEND in .env}"
: "${DUCKDB_UPLOAD_DB_PARAMS:?Set DUCKDB_UPLOAD_DB_PARAMS in .env}"
: "${POSTGRES_UPLOAD_DB_BACKEND:?Set POSTGRES_UPLOAD_DB_BACKEND in .env}"
: "${POSTGRES_UPLOAD_DB_PARAMS:?Set POSTGRES_UPLOAD_DB_PARAMS in .env}"
: "${CLICKHOUSE_UPLOAD_DB_BACKEND:?Set CLICKHOUSE_UPLOAD_DB_BACKEND in .env}"
: "${CLICKHOUSE_UPLOAD_DB_PARAMS:?Set CLICKHOUSE_UPLOAD_DB_PARAMS in .env}"

DUCKDB_DB_PARAMS="${DUCKDB_DB_PARAMS:-$DUCKDB_UPLOAD_DB_PARAMS}"
POSTGRES_DB_PARAMS="${POSTGRES_DB_PARAMS:-$POSTGRES_UPLOAD_DB_PARAMS}"

find . \( -path "./src/*" -o -path "./tests/*" \) -name "__pycache__" -type d -exec rm -rf {} +

failures=()

run_check() {
  local label="$1"
  shift

  echo "==> Running ${label}"
  if "$@"; then
    echo "==> Passed ${label}"
  else
    local status=$?
    echo "==> Failed ${label} (exit ${status}); continuing commit checks" >&2
    failures+=("${label} (exit ${status})")
  fi
}

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

run_selection_test() {
  local db_backend="$1"
  local db_params="$2"

  pytest tests/test_selection.py \
    -v \
    --db-backend "$db_backend" \
    --db-params "$db_params"
}

run_inspect_test() {
  local db_backend="$1"
  local db_params="$2"

  pytest tests/test_inspect.py \
    -v \
    --db-backend "$db_backend" \
    --db-params "$db_params"
}

run_cleaning_test() {
  local db_backend="$1"
  local db_params="$2"

  pytest tests/test_cleaning.py \
    -v \
    --db-backend "$db_backend" \
    --db-params "$db_params"
}

run_stats_test() {
  local db_backend="$1"
  local db_params="$2"

  pytest tests/test_stats.py \
    -v \
    --db-backend "$db_backend" \
    --db-params "$db_params"
}

for upload_type in csv parquet; do
  filepath_var="UPLOAD_${upload_type^^}_FILEPATH"
  run_check "upload ${upload_type} duckdb" \
    run_upload_test "$upload_type" "${!filepath_var}" "$DUCKDB_UPLOAD_DB_BACKEND" "$DUCKDB_UPLOAD_DB_PARAMS"
  run_check "upload ${upload_type} postgres" \
    run_upload_test "$upload_type" "${!filepath_var}" "$POSTGRES_UPLOAD_DB_BACKEND" "$POSTGRES_UPLOAD_DB_PARAMS"
  if [ -n "${CLICKHOUSE_UPLOAD_DB_BACKEND:-}" ] && [ -n "${CLICKHOUSE_UPLOAD_DB_PARAMS:-}" ]; then
    run_check "upload ${upload_type} clickhouse" \
      run_upload_test "$upload_type" "${!filepath_var}" "$CLICKHOUSE_UPLOAD_DB_BACKEND" "$CLICKHOUSE_UPLOAD_DB_PARAMS"
  fi
done

run_check "selection duckdb" run_selection_test duckdb "$DUCKDB_DB_PARAMS"
run_check "selection postgres" run_selection_test postgres "$POSTGRES_DB_PARAMS"
run_check "inspect duckdb" run_inspect_test duckdb "$DUCKDB_DB_PARAMS"
run_check "inspect postgres" run_inspect_test postgres "$POSTGRES_DB_PARAMS"
run_check "cleaning duckdb" run_cleaning_test duckdb "$DUCKDB_DB_PARAMS"
run_check "cleaning postgres" run_cleaning_test postgres "$POSTGRES_DB_PARAMS"
run_check "stats duckdb" run_stats_test duckdb "$DUCKDB_DB_PARAMS"
run_check "stats postgres" run_stats_test postgres "$POSTGRES_DB_PARAMS"
run_check "tox py310 py311 py312 py313" tox -r -p auto -e py310,py311,py312,py313

if [ "${#failures[@]}" -gt 0 ]; then
  echo ""
  echo "Commit checks completed with failures, but the pre-commit hook will not block this commit:" >&2
  printf ' - %s\n' "${failures[@]}" >&2
else
  echo ""
  echo "Commit checks completed successfully."
fi

exit 0
