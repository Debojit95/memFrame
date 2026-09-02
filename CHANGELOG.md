# Changelog

All notable changes to memFrame are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.1] - 2026-09-02

### Changed
- **Sync API runs on one shared background event loop**: `async_to_sync` no longer spins a fresh `asyncio.run` per call — the asyncpg pool is no longer torn down/rebuilt on every sync call and the DuckDB connection is used from a single thread. Sync APIs called from a coroutine already running on the shared loop now raise a `RuntimeError` pointing at the async form.
- **Dataset contexts snapshot the active `data_id` at creation**: a later `set_active()` no longer retargets an existing context mid-flight (upload/`aset_active` contexts already behaved this way).

### Fixed
- **SQL-injection hardening**: `fillna` constant fill values are escaped, the `iloc` raw-WHERE escape hatch rejects multi-statement strings, `map` placeholder substitution no longer corrupts function names, identifiers are quote-doubled everywhere, identifier validation rejects trailing newlines, agent credentials use `SecretStr`, and the dashboard guardrail page HTML-escapes model output.
- **Postgres numeric-text casts**: the arithmetic text-cast hook emitted DuckDB-only `TRY_CAST` (syntax error on Postgres); emulated with a numeric-pattern CASE guard, junk text still becomes NULL.
- **CSV typed-stream retries skip already-flushed rows** instead of re-inserting them (duplicated rows on mid-stream conversion failures).
- **Stale agent context**: `domain_context` caches are cleared by `invalidate()`/`advance_table()` — the previous table's schema is no longer served after a table switch.
- **Deep cache**: DataFrame/Series signatures include a content hash (same-shape frames can no longer collide into wrong cache hits).
- **Plot fetches capped at 10k rows**; AI-plot PNG rendering runs off-loop with a timeout so a hung Chromium cannot freeze the event loop.
- **Planner `UnexpectedModelBehavior`**: `SubQueryNode.query` coerces dict → string, planner `retries=3`, heuristic fallback for `value counts`/`correlation` (fixes dashboard one-sentence flake on `gpt-oss:120b-cloud`).

### Refactor / CI
- Remove dead `DB_TO_PANDAS_DTYPE_MAP` 80-line map — zero runtime consumers (`helper.py:75-154`).
- Dedup CI: delete duplicated `tox.yml` workflow (keep `ci.yml:tox`), shrink `tox.ini:deps`.
- Remove `chardet`/`kaleido` from core deps — stdlib `utf-8` decode loop + best-effort PNG (`plot.py` already `except Exception: png=None`).

### Internal
- The four analytix packages (cleaning, arithmetic, selection, stats) are consolidated onto hook bases mirroring `inspection/` — 10.3k → 6.3k lines, with SQL-fingerprint regression harnesses (182 scenarios × 3 backends) proving per-backend SQL unchanged.
- Ops integration assertions are order-insensitive (result samples come from unordered SELECTs; ClickHouse merges reorder rows under load).

## [0.3.1rc1] - 2026-09-02

### Changed
- **Sync API runs on one shared background event loop**: `async_to_sync` no longer spins a fresh `asyncio.run` per call — the asyncpg pool is no longer torn down/rebuilt on every sync call and the DuckDB connection is used from a single thread. Sync APIs called from a coroutine already running on the shared loop now raise a `RuntimeError` pointing at the async form.
- **Dataset contexts snapshot the active `data_id` at creation**: a later `set_active()` no longer retargets an existing context mid-flight (upload/`aset_active` contexts already behaved this way).

### Fixed
- **SQL-injection hardening**: `fillna` constant fill values are escaped, the `iloc` raw-WHERE escape hatch rejects multi-statement strings, `map` placeholder substitution no longer corrupts function names, identifiers are quote-doubled everywhere, identifier validation rejects trailing newlines, agent credentials use `SecretStr`, and the dashboard guardrail page HTML-escapes model output.
- **Postgres numeric-text casts**: the arithmetic text-cast hook emitted DuckDB-only `TRY_CAST` (syntax error on Postgres); emulated with a numeric-pattern CASE guard, junk text still becomes NULL.
- **CSV typed-stream retries skip already-flushed rows** instead of re-inserting them (duplicated rows on mid-stream conversion failures).
- **Stale agent context**: `domain_context` caches are cleared by `invalidate()`/`advance_table()` — the previous table's schema is no longer served after a table switch.
- **Deep cache**: DataFrame/Series signatures include a content hash (same-shape frames can no longer collide into wrong cache hits).
- **Plot fetches capped at 10k rows**; AI-plot PNG rendering runs off-loop with a timeout so a hung Chromium cannot freeze the event loop.

### Internal
- The four analytix packages (cleaning, arithmetic, selection, stats) are consolidated onto hook bases mirroring `inspection/` — 10.3k → 6.3k lines, with SQL-fingerprint regression harnesses (182 scenarios × 3 backends) proving per-backend SQL unchanged.
- Ops integration assertions are order-insensitive (result samples come from unordered SELECTs; ClickHouse merges reorder rows under load).

## [0.3.0] - 2026-08-28

### Added
- **Ollama Cloud support**: the AI gateway now forwards an explicit `base_url`/`api_key` (e.g. `https://ollama.com/v1`) for Ollama Cloud instead of assuming a local daemon; `OLLAMA_BASE_URL` remains a fallback and the `localhost:11434` default is preserved.

### Fixed
- **Plots and dashboards render in Colab/Jupyter**: figures now embed plotly.js **inline**, so `df.bar`/`df.scatter`/`df.line`/`df.pie`/`df.scatter_3d`/`df.bar_polar` and the dashboard API render interactively without fetching plotly.js from the blocked `cdn.plot.ly` CDN. `smart_show()` no longer calls `fig.show()` inside a notebook; the kernel auto-displays the returned figure.
- **AI agent dynamic re-enable**: `AnalyticsAgent` rebuilds when memframe AI settings change, preserving chat history.

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