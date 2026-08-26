# Changelog

All notable changes to memFrame are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0rc3] - 2026-08-26

### Changed
- **Schema/table rename**: the three auto-created namespaces and their inner tables are now prefixed `memframe_` to avoid collisions in shared databases — `upload` → `memframe_upload`, `transient` → `memframe_transient`, `registry` → `memframe_csv_registry`; inner tables `csv_registry` → `memframe_csv_registry` and `transient_registry` → `memframe_transient_registry` (the transient-registry table lives in `memframe_csv_registry` — a schema on DuckDB/PostgreSQL, a database on ClickHouse). Existing databases are not auto-migrated; fresh databases pick up the new names.
- `aset_active`/`set_active` now return the dataset `ContextManager` (instead of the bare `data_id` string), so activation flows straight into operations: `ctx = mf.set_active(data_id); ctx.select_dtypes(...)`. `get_active_table()` still returns the active `data_id`.

### Performance
- ClickHouse backend reuses a pooled `httpx` client with connection keep-alive per event loop, replacing per-query client creation that opened a new TCP connection for every statement (much faster integration runs).

## [0.3.0rc2] - 2026-08-24

### Fixed
- **Dashboard now renders in notebooks (Colab/Jupyter)**: the dashboard was composed as one Plotly figure but displayed via `display(HTML(...))`, whose `<script>` is stripped by notebook sanitization (blank dashboard). It now displays the native `go.Figure` via Plotly's mimebundle in a live kernel. Terminal/browser rendering unchanged.

## [0.3.0rc1] - 2026-08-23

### Added
- **AI Dashboard**: one-shot natural-language sentence produces an auto dashboard; renders as a single full-screen Plotly canvas with every DataFrame shown as a table (scalar/dict/list sub-query results now render correctly).
- **Query guardrail**: validates requests and returns a graceful "blocked" message instead of failing on unsupported queries.
- **Opt-in Logfire observability**: bring-your-own-key tracing across all agents (enable via `logfire_enabled`/`logfire_token`); host metrics via the optional `system-metrics` extra; traces flushed at the end of `achat`/`adashboard`.
- **SyncDB**: register pre-existing DuckDB/PostgreSQL/ClickHouse tables into the csv_registry as datasets.
- Unit-test parallelism via `pytest-xdist`.

### Changed
- Renamed the `memframe_ai` logger name to `memframe.ai`.
- `memframe_ai` docs reorganized under `docs/memframe-ai/` (agent, dashboard, observability).
- The `[logfire]` extra now installs the `[ai]` runtime, so `pip install "memframe[logfire]"` pulls in `pydantic-ai` automatically.

### Fixed
- Logfire configuration is resilient when the `system-metrics` extra is absent (host metrics are skipped instead of disabling all tracing).

## [0.2.2] - 2026-08-19

### Changed
- Data Quality Reports (missing-values, completeness, numeric summary, profile report) relocated from the cleaning module to the inspect module.
- Removed bivariate association methods (chi_square, cramers_v, theil_u, mutual_information) and their categorical wrappers.
- CONTRIBUTING.md: added a distinct `[refactor]` commit tag (pure restructuring, no behavior/public-API change) separate from `[upgrade]`.
- README and docs: documented that every call compiles to backend-native SQL; added Open-in-Colab badge.

## [0.2.1] - 2026-08-17

### Fixed
- Postgres `.corr()`/`.cov()` now compute via streaming in-memory numpy for wide
  feature sets, removing the Postgres aggregate-explosion hang and greatly
  speeding up wide correlation/covariance matrices.
- DuckDB `.corr()` recursion-depth failure fixed for large column-pair counts.
- Arithmetic `add`/`subtract`/`multiply`/`divide` now correctly handle
  vector-scalar operands (column ± scalar, scalar ± column, negative and float
  scalars); scalar-scalar operands are now rejected with a clear `OperationError`.
- Fixed `clip` date parsing bug.

## [0.2.0] - 2026-08-16

### Changed
- (BREAKING) `df.chat()` / `achat()` return shape changed: `answer` is now a
  compact `sub_queryN`-style status string; `blocks` / `return_blocks` removed.
- Operation-result DataFrame is now returned in full (all rows, all columns)
  via new `result` and `results` keys.
- Inline notebook display uses pandas/Colab's default truncation instead of
  forcing every row inline (which hung notebooks on large tables).
- LLM tool-return payload stays capped (`max_output_rows`) to bound agent
  context; full data is still returned to the caller.

### Removed
- `src/memframe_ai/format.py` and the block/`return_blocks` machinery
  (`analytics._package`, `ContextManager.achat`, `entrypoints`).

### [0.1.3] - 2026-08-15

### Changed
- Public operation results are now raw values (DataFrame, dict, scalar, or
  streaming async iterator) instead of the internal response envelope.
  Operations that fail raise `OperationError` instead of returning an
  `is_error` dict.
- All `tests/integration` and `tests/integration/ops` tests updated to the
  new raw public API.

### Added
- Unit tests covering the public result boundary (`test_public_results.py`).

### Docs
- `docs/api/{cleaning,arithmetic,stats,inspect,selection}.md` updated to the
  raw return types.

## [0.1.1] - 2026-08-13

### Changed
- Removed two debug `print()` statements from `CleaningOrchestrator`
  (`Inferred :` and `detected_dtype--------- :`) that were leaking into
  agent chat output.

### Notes
- Same `0.1.0` feature set; this is a packaging/cleanup patch.

## [0.1.0] - 2026-08-13

### Added
- Database-backed DataFrame API across DuckDB, PostgreSQL, and ClickHouse.
- Async-first surface with sync equivalents for every operation.
- Upload from CSV, Parquet, or pandas DataFrame.
- Inspection, selection, cleaning, statistics, arithmetic, Plotly charts.
- Two-level cache: lineage audit + replayable result tables.
- Optional AI agent layer (`memframe_ai`) for natural-language data work.
- Conventional commit tags (`[feat]`, `[fix]`, `[upgrade]`, `[ci]`, `[docs]`,
  `[add]`, `[remove]`) documented in `CONTRIBUTING.md`.
- Slim README with Quick Start, AI Agent section, and links to per-domain
  docs in `docs/api/`.