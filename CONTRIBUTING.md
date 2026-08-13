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

The first release uses the current `pyproject.toml` version as a bootstrap (no bump needed).

## Commit message format

Use a tag prefix so the history is greppable and the release notes stay
tidy. The full set:

| Tag         | Use for                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------- |
| `[feat]`    | New feature addition. **Fork the repo and open a PR from a feature branch** — never commit `[feat]` directly to `main`. |
| `[fix]`     | Any bug fix — incorrect behavior, broken test, wrong SQL, etc.                           |
| `[upgrade]` | Existing feature rewritten, refactored, or extended into a new shape (e.g. expanding the cleaning tool surface). |
| `[ci]`      | CI / GitHub Actions / pre-commit / hooks / workflow changes only.                        |
| `[docs]`    | Documentation-only changes (README, `docs/`, docstrings).                                |
| `[add]`     | Adding a supporting file that does not introduce a feature (fixture, config, isolated helper). |
| `[remove]`  | Removing a tracked file from the repo (untracking / deletion of source, doc, or config).  |

### Format

```
[<tag>] <short-imperative-summary>

<optional body — wrap at ~72 chars; explain what and why, not how>
```

- Tag is lowercase, bracketed, single space, then the summary.
- Summary is imperative mood, ≤72 chars, no trailing period.
- The seven tags above are exhaustive. If a change genuinely fits none of them, prefer the closest tag and explain in the body. `[update]` is **not** a valid tag — use `[upgrade]` or `[docs]` instead.

### Examples

```bash
git commit -m "[feat] Add ClickHouse adapter for cache reload"
git commit -m "[fix] Avoid double COUNT(*) on empty select_dtypes result"
git commit -m "[upgrade] Stats tool surface to full wrapper parity"
git commit -m "[ci] Remove integration job from ci workflow"
git commit -m "[docs] Slim README and add agent section"
git commit -m "[add] tests/datasets/sample.parquet fixture"
git commit -m "[remove] deprecated commands.md scratchpad"
```

### Branch-required changes

The following commit shapes warrant their own branch (fork + feature
branch + pull request — never commit directly to `main`):

- `[feat]` — any new feature, no matter how small.
- `[upgrade]` — any refactor that touches the public API surface (adding
  or renaming methods on a public wrapper, changing return shapes,
  moving a method between classes, etc.).
- `[fix]` — **multi-file** bug fixes that cross more than one subsystem
  (e.g. core + orchestrator + tool layer, or backend + adapter).
  Single-file, self-contained fixes can land directly on `main`.
- `[ci]` — changes that touch **two or more** workflow files at once,
  or modify the pre-commit hook itself. Single-workflow tweaks can
  land directly on `main`.
- Any commit that **depends on** an unmerged `[feat]` or `[upgrade]`
  PR — keep the dependency on its own branch until the parent lands.

Direct commits to `main` are reserved for the maintainer's small,
self-contained changes that don't need another pair of eyes: single-file
`[fix]`, single-file `[docs]`, single-file `[add]`, single-file
`[remove]`, and small single-workflow `[ci]` tweaks.

When in doubt, open a branch.

### Enforcement

Convention is documentary only. Reviewers enforce in PR review; `scripts/release.sh` does not validate tags. Do not add a `commit-msg` blocking hook without a separate discussion — the current pre-commit hook runs `ruff` + unit tests and that's all.

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

# ops tests individual
uv run python -m pytest tests/integration/ops/test_xxx.py \
  --db-backend postgres \
  --db-params '{"backend":"postgres","host":"localhost","port":xxxx,"user":"postgres","password":"xxx","database":"xxx"}'


uv run python -m pytest tests/integration/ops/test_upload.py --upload-type csv --filepath tests/datasets/sample.csv --db-backend clickhouse --db-params '{"backend":"clickhouse", "host": "localhost", "port": 8123,  "user":"default", "password": "your_clickhouse_password"}'


  
uv run python -m pytest tests/integration/ops/test_xxx.py \
  --db-backend duckdb \
  --db-params '{"db_path":"xxx.duckdb"}'


-v --save-to-file[optional] 

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
