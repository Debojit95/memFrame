# Testing

memFrame uses `pytest`. All tests live under `tests/` and run through a single
entry point: `tests/run_tests.py`.

## Test layout

| Path | What it covers |
|------|----------------|
| `tests/unit/` | Fast, dependency-free unit tests: exceptions, async/sync wrappers, factory, connection layer, datatype detection, context manager, ops mixin, error paths. |
| `tests/integration/ops/` | End-to-end operation tests against a real backend: upload, selection, inspection, cleaning, arithmetic, stats, and all plot types. |
| `tests/integration/` | Cross-cutting lifecycle, parity, and concurrency tests per backend. |
| `tests/run_tests.py` | The single entry point that runs everything, grouped **db-by-db**. |

Integration tests pick a backend from the `--db-backend` / `--db-params`
options; unit tests never touch a database.

## Quick start

```bash
# Install the dev extras
uv sync --extra dev

# Unit tests only (no database needed)
python tests/run_tests.py --scope unit

# Everything against DuckDB (needs nothing else installed)
python tests/run_tests.py --backend duckdb

# Everything against every backend you have credentials for
python tests/run_tests.py --backend all

# Same as above, plus tox across py310-py313
python tests/run_tests.py --backend all --tox
```

### Options

| Option | Meaning |
|--------|---------|
| `--backend` | Comma-separated `duckdb,postgres,clickhouse` or `all`. |
| `--scope` | Comma-separated `unit,ops,integration` or `all`. |
| `--upload-type` | Comma-separated `csv,parquet,df`, `all`, or `none`. |
| `--<backend>-params` | JSON connection params (e.g. `--duckdb-params '{"db_path":"/tmp/x.duckdb"}'`). Falls back to `DUCKDB_DB_PARAMS`, `POSTGRES_DB_PARAMS`, `CLICKHOUSE_DB_PARAMS` env vars. |
| `--upload-csv` / `--upload-parquet` | Source files for upload tests. Falls back to `UPLOAD_CSV_FILEPATH` / `UPLOAD_PARQUET_FILEPATH`. |
| `--schema-prefix` | Schema prefix for Postgres/ClickHouse isolation across concurrent runs. |
| `--require-upload-file` | Fail if an upload source file is missing, instead of skipping that upload test. |
| `-v`, `--verbose` | Pass `-v` to every pytest run for verbose output. |
| `--save-to-file` | Pass `--save-to-file` to integration runs so they save expected-vs-actual PDF reports under `tests/integration/ops/result/` (one PDF per operation test module). |
| `--tox` | Also run `tox -p auto -e py310,py311,py312,py313`. |
| `--tox-recreate` | Recreate tox environments. |
| `--pytest-args ...` | Extra arguments forwarded to every pytest invocation. |
| `--dry-run` | Print the pytest commands without running them. |

```bash
# Preview what a full run would execute
python tests/run_tests.py --backend all --dry-run

# Verbose output plus PDF reports for the integration tests
python tests/run_tests.py --backend duckdb -v --save-to-file
```

### Why "db-by-db"?

The suite is organized by database backend, not by operation. A single backend
run is just a few pytest processes:

- one combined run over `tests/integration/ops` + `tests/integration`
- one upload run per upload type
- one unit run (db-free)

So `--backend duckdb` is **three** pytest invocations, and `--backend all` is
**seven** — instead of one process per operation per backend. DuckDB gets a
fresh temp file per invocation, so runs never see each other's residue.

## `.env.test`

Connection details for the integration tests live in a `.env.test` file at the
repository root (not a plain `.env` — it exists only for testing). `run_tests.py`
loads it automatically when you run the suite, so you do not need to export
anything yourself. Existing environment variables take precedence over the file.

```bash
# Copy the template, then fill in your real credentials
cp .env.test.example .env.test
```

Example `.env.test`:

```dotenv
UPLOAD_CSV_FILEPATH=tests/datasets/sample.csv
UPLOAD_PARQUET_FILEPATH=tests/datasets/sample.parquet

DUCKDB_UPLOAD_DB_BACKEND=duckdb
DUCKDB_UPLOAD_DB_PARAMS='{"db_path":"memFrame_new.duckdb"}'
DUCKDB_DB_PARAMS='{"db_path":"memFrame_new.duckdb"}'

POSTGRES_UPLOAD_DB_BACKEND=postgres
POSTGRES_UPLOAD_DB_PARAMS='{"backend":"postgres","host":"localhost","port":5432,"user":"postgres","password":"secret","database":"memframe_test"}'
POSTGRES_DB_PARAMS='{"backend":"postgres","host":"localhost","port":5432,"user":"postgres","password":"secret","database":"memframe_test"}'

CLICKHOUSE_UPLOAD_DB_BACKEND=clickhouse
CLICKHOUSE_UPLOAD_DB_PARAMS='{"backend":"clickhouse","host":"localhost","port":8123,"user":"default","password":"secret"}'
CLICKHOUSE_DB_PARAMS='{"backend":"clickhouse","host":"localhost","port":8123,"user":"default","password":"secret"}'
```

Two variables per backend:

- `*_UPLOAD_DB_*` — connection details for the upload tests.
- `*_DB_PARAMS` — connection details for the operation/integration tests
  (falls back to the upload values if not set).

With a valid `.env.test`, running every backend is just:

```bash
python tests/run_tests.py --backend all
```

`DUCKDB_DB_PARAMS` (or `DUCKDB_UPLOAD_DB_PARAMS`) disables the automatic
temp-file behavior, so DuckDB tests use the `db_path` you specify. Omit both to
keep the fresh-temp-file-per-run behavior. The file is git-ignored; commit
`UPLOAD_CSV_FILEPATH`/`UPLOAD_PARQUET_FILEPATH` and any shared/example values in
`.env.test.example` instead.

## Backends

### DuckDB (local)

No configuration required; a temp file is created per run.

```bash
python tests/run_tests.py --backend duckdb
```

### PostgreSQL / ClickHouse (remote)

Provide connection params via CLI or env. Postgres and ClickHouse use a
`schema_prefix` to keep parallel runs isolated.

```bash
POSTGRES_DB_PARAMS='{"backend":"postgres","host":"localhost","port":5432,"user":"postgres","password":"secret","database":"memframe_test"}' \
python tests/run_tests.py --backend postgres

python tests/run_tests.py --backend postgres,clickhouse --schema-prefix mf_dev
```

The commit-checks workflow (`.github/workflows/commit-checks.yml`) starts
Postgres and ClickHouse services and exports the connection details as env vars.

## Pre-commit / CI

Local commits run `scripts/run-commit-checks.sh`, which delegates to
`tests/run_tests.py` with the required upload files and tox. The GitHub
workflow runs the same script. It never blocks a commit: failures are printed
and the hook exits `0`. To skip the hook:

```bash
git commit --no-verify
```

### Environment variables

Required by the commit checks (set in `.env.test`):

- `UPLOAD_CSV_FILEPATH`, `UPLOAD_PARQUET_FILEPATH`
- `DUCKDB_UPLOAD_DB_BACKEND`, `DUCKDB_UPLOAD_DB_PARAMS`
- `POSTGRES_UPLOAD_DB_BACKEND`, `POSTGRES_UPLOAD_DB_PARAMS`
- `CLICKHOUSE_UPLOAD_DB_BACKEND`, `CLICKHOUSE_UPLOAD_DB_PARAMS`

Optional:

- `DUCKDB_DB_PARAMS`, `POSTGRES_DB_PARAMS`, `CLICKHOUSE_DB_PARAMS` — override
  per-backend connection params for operation/integration tests.
- `COMMIT_CHECK_TMPDIR` — where temp DuckDB files are written.
- `TOX_RECREATE=1` — recreate tox environments.
