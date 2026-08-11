# Contributing to memFrame

Thanks for contributing! This guide covers the workflow, tooling, and commit conventions so your changes land cleanly.

## Environment

memFrame is managed with [uv](https://docs.astral.sh/uv/). Python 3.10+ is required.

```bash
# install dependencies (dev + the optional AI layer)
uv sync --extra dev --extra ai

# or minimal install if you only work on core memframe
uv sync --extra dev
```

## Development workflow

1. Create a branch off `main` for your change.
2. Make the change, keeping the diff as small and focused as possible.
3. Add or update tests in `tests/unit` for the behavior you changed.
4. Run the checks below.
5. Open a pull request against `main`.

## Commit conventions

The pre-commit hook (`core.hooksPath` → `.githooks`) runs automatically. There are three commit modes:

| Command | What runs |
| --- | --- |
| `git commit -m "msg"` | Pre-commit gate: `ruff check src/` + unit tests. Commit proceeds only if they pass. |
| `git commit -m "msg" --no-verify` | Bypasses the hook entirely. Use for CI-only or docs changes; never to sneak past failing tests. |
| `git release <version> "msg"` | Full release gate: ruff + full suite (unit, ops, integration across DuckDB/Postgres/ClickHouse, tox) + build + `twine check`, then bumps the version, commits, tags `v<version>`, and pushes. CI publishes to TestPyPI. |

Version scheme:

- `0.X.0` — feature addition (bump X, reset Y)
- `0.X.Y` — bug fix only (bump Y)
- `0.X.Y` with both bumped — feature + bug fix in the same release

The first release uses the current `pyproject.toml` version as a bootstrap (no bump needed).

## Running tests

The test suite is orchestrated by `tests/run_tests.py`:

```bash
# fast, dependency-free unit tests
uv run python tests/run_tests.py --scope unit

# integration + ops tests against a backend (duckdb needs no services)
uv run python tests/run_tests.py --scope integration --backend duckdb --upload-type csv,parquet \
  --upload-csv tests/datasets/sample.csv \
  --upload-parquet tests/datasets/sample.parquet

# everything + tox across py310–py313 (the release gate)
bash scripts/run-commit-checks.sh   # requires .env.test with DB credentials
```

Postgres and ClickHouse integration runs need a local `.env.test` with `POSTGRES_*` / `CLICKHOUSE_*` connection variables. A template lives at `scripts/run-commit-checks.sh`.

## Linting

[ruff](https://docs.astral.sh/ruff/) is the linter and runs on `src/` only (tests are excluded from the gate).

```bash
uv run ruff check src/
```

## Release checklist (maintainers)

1. `scripts/release.sh <version> "<message>" --dry-run` to preview.
2. `scripts/release.sh <version> "<message>"` — runs the full gate, bumps, tags, and pushes.
3. Verify the TestPyPI install:
   `pip install --index-url https://test.pypi.org/simple memframe`
4. Promote to PyPI via the `Release to PyPI` workflow (Actions → workflow_dispatch).

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
