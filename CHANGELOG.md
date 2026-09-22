# Changelog

All notable changes to memFrame are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres
to [Semantic Versioning](https://semver.org/).

## [0.10.2] - 2026-09-22

### Changed
- **Window core split**: `core/analytix/window.py` (single backend-branching `WindowOps`) is now a `window/` package — `base.py` holds the shared `WindowOps` with DuckDB-flavoured defaults plus dialect hooks (`_row_id_col`, `_std_agg`/`_var_agg`, `_datetime_epoch_expr`, `_median_epoch_sql`, `_from_epoch_expr`, `_avg_fn`/`_count_fn`, `_quantile_cont_sql`, `_ewm_row_types`, `_ewm_uses_stage_table`); `duckdb.py`/`postgres.py`/`clickhouse.py` override the hooks and the structurally divergent operations (PostgreSQL's correlated-subquery quantile and Python `nunique` fallback, ClickHouse's single-pass windows); `factory.make_window_ops(adapter)` dispatches on `isinstance`. The orchestrator builds the ops via the factory. Generated SQL is proven byte-identical by the unchanged `window_sql_fingerprint.json` snapshot.

### Fixed
- **PostgreSQL EWM type names**: restored `BIGINT`/`DOUBLE PRECISION` in the EWM `VALUES` cast (the split initially defaulted to DuckDB's `DOUBLE`); caught by the live window integration suite.

### Docs
- Window page: removed SQL snippets; added examples omitting `order_by`.
- Cumulative page: added examples omitting `order_col`.

## [0.10.1] - 2026-09-21

### Fixed
- **Python 3.10 import**: `core/analytix/window.py` used `datetime.UTC` (added in Python 3.11), which broke importing memFrame on 3.10; switched to `timezone.utc`. Caught by the `tox py310-py313` CI job.

## [0.10.0] - 2026-09-21

### Added
- **Window (new domain)**: rolling, expanding, and exponentially-weighted ops via `ContextManager` — direct calls (`rolling`/`arolling`, `expanding`/`aexpanding`, `ewm`/`aewm`) and a fluent builder (`on(column).rolling(n).mean(...)`, `.expanding(...)`, `.ewm(...)`) — backed by backend-native SQL across DuckDB, PostgreSQL, and ClickHouse. `WindowOps` is a single backend-branching engine (no factory split); the orchestrator detects the column dtype (numeric / datetime / categorical) and maps each requested function to the dtype-appropriate engine method, chaining multi-function requests into one transient table. Datetime rolling supports `min`/`max`/`mean`/`median`/`mode` (epoch conversion); EWM is numeric-only with `com`/`span`/`halflife`/`alpha`, `adjust`, `ignore_na`, and `min_periods`.
- **Window tests**: `tests/unit/test_window_response.py` (10: values, multi-func chaining, dtype/error shapes, datetime rolling, EWM, fluent builder) + `tests/unit/test_window_sql_fingerprint.py` (21 scenarios × 3 backends, fixture `window_sql_fingerprint.json`); `tests/integration/ops/test_window.py` (31 tests × 3 backends: shuffled unordered fixture, no-`order_by` cases, multi-func, null handling, datetime rolling, a `min`/`max`/`count`/`mean`/`sum`/`std`/`sem`/`rank`/`nunique`/`first`/`last` sweep, and edge windows `w=1` / `w>frame`).
- **Window docs**: `docs/api/window.md` (Public API, dtype support matrix, per-op semantics, fluent builder, backend behavior, mkdocstrings reference) + nav entry after `Cumulative`.
- **Wrapper type stubs**: `window.pyi` and `merging.pyi`.
- **Adaptive logos**: dark/light logo variants for the docs site (`palette` light/dark toggle + scheme-aware nav-logo swap) and `<picture>` swaps in `README.md` / `docs/index.md`.

### Fixed
- **PostgreSQL `rolling_quantile`**: ordered-set aggregate `PERCENTILE_CONT(...) WITHIN GROUP (...)` rejects a window frame (`OVER is not supported for ordered-set aggregate percentile_cont`); the PostgreSQL branch now computes the windowed quantile per-row in a correlated-subquery `CREATE TABLE AS`, matching the DuckDB `QUANTILE_CONT` path. SQL fingerprint regenerated.

## [0.9.0] - 2026-09-18

### Added
- **Cumulative (new domain)**: 8 ops via `ContextManager` (`cumsum`/`cumprod`/`cummax`/`cummin`/`cummean`/`cumcount`/`cumstd`/`cumvar` — pandas `expanding()` sense, `(column, order_col=None, target_col=None)`) backed by backend-native SQL across DuckDB, PostgreSQL, and ClickHouse. `CumulativeOps` base holds the shared engine (PostgreSQL/DuckDB share clone → `ADD COLUMN` → `UPDATE … FROM` on `ctid`/`rowid`); ClickHouse overrides hooks plus one structural CTAS (`MergeTree ORDER BY tuple()`, synthetic `_ch_rowid` for default order). Population `STDDEV_POP`/`VAR_POP` (`stddevPop`/`varPop` on ClickHouse); `cumcount` is `BIGINT`/`UInt64`.
- **Cumulative tests**: `tests/unit/test_cumulative_response.py` (11: values, order/target variants, all-ops, error shape) + `tests/unit/test_cumulative_sql_fingerprint.py` (24 scenarios × 3 backends); happy path verified live on PostgreSQL and ClickHouse.
- **Cumulative docs**: `docs/api/cumulative.md` (datetime-style: Public API, per-op params, backend behavior, mkdocstrings reference); nav after `Arithmetic` + README docs line.
- **Cumulative refactor**: split `core/analytix/cumulative.py` (`DataCumulativeOps`) into `cumulative/` (`CumulativeOps` base + `duckdb`/`postgres`/`clickhouse` + `factory.make_cumulative_ops`, responses via shared `ok()`/`fail()`); SQL proven unchanged by the existing fingerprint snapshot.

## [0.8.0] - 2026-09-17

### Added
- **Transform (new umbrella, Tier1+2)**: 7 ops via `ContextManager` (`robust_scale`/`robust`, `maxabs_scale`/`maxabs`, `normalize`, `log_transform`/`log`, `quantile_transform`/`quantile`, `power_transform`/`power` (yeo-johnson λ=0.5, box-cox), `ordinal_encode`/`ordinal` — `0..n-1` alphabetical) backed by backend-native SQL across DuckDB, PostgreSQL, and ClickHouse. `Transform` is `sklearn transform` / `pandas transform` sense.
- **Transform docs**: `docs/api/preprocessing.md` now `# Transform` (title-only, file kept `preprocessing.md`), table 15→22 ops, `Source: src/memframe/...` fix, members 28→44; nav `Clean`/`Merge`/`Transform` + README docs line.
- **Transform tests**: `tests/unit/test_preprocessing_response.py` 4→11 (happy/canonical/error/public API) + `tests/integration/ops/test_preprocessing.py` 16→23 (20→23 on duckdb, pg/ch verified).
- **Docs titles**: `Cleaning`→`Clean`, `Merging`→`Merge` (title-only, filenames kept).

### Fixed
- `numeric_minmax_scale` PG integer division (`CAST DOUBLE PRECISION`).
- `categorical_{label,frequency,target}_encode` ClickHouse `source.*` collision (`_ch_join_key` alias).

### Changed
- None beyond Transform/docs titles.

## [0.7.2] - 2026-09-15

### Fixed
- **DuckDB's notebook progress bar** no longer appears on every operation. Local DuckDB sessions now set `enable_progress_bar=false` (and `enable_progress_bar_print=false`), so the black bar DuckDB renders into the cell output — and its injected JS that errors on Colab — is disabled. Regression-asserted in `tests/unit/test_pool.py`.

## [0.7.1] - 2026-09-15

### Changed
- **Merge/join/concat return a live `ContextManager`** on the new output table instead of a `DataFrame` snapshot. Outputs are persisted as transient tables and are chainable (`merged.head()`, `merged.merge(...)`, or usable as the `right_ops` of another merge); deep-cache replays return a context too. `chunk_size` no longer changes the public return type. Docs updated.
- **Library logging is silent by default** — memFrame no longer installs `StreamHandler`s or sets `INFO` at import, so notebooks (Colab/Kaggle) stop printing per-operation lines to stderr; the plot renderer's `print()` calls became `logger.debug`. Opt in with `memframe.enable_logging()`.

### Fixed
- **Merging across schemas**: each side is qualified with its own schema, so a merged output (transient schema) can be chained with an upload-schema dataset.

## [0.7.0] - 2026-09-14

### Added
- **Merging (new umbrella)**: `merge`/`amerge`, `join`/`ajoin`, and `concat`/`aconcat` via `ContextManager`, joining dataset contexts on shared keys (`on`, or split `left_on`/`right_on`; `join` falls back to common columns) with `inner`/`left`/`right`/`outer`/`cross`/`left_anti`/`right_anti`, `merge` suffixes vs `join` `lsuffix`/`rsuffix`, timestamp↔date auto-cast, and row/column `concat` with `ignore_index`. Backend-native SQL across DuckDB, PostgreSQL, and ClickHouse.
- **Merging tests**: `tests/unit/test_merging_response.py` (28: happy/error/streaming/public API) + `test_merging_sql_fingerprint.py` (40 scenarios × 3 backends, fixture `merging_sql_fingerprint.json`) + `tests/integration/ops/test_merging.py` (9 ops on DuckDB/Postgres/ClickHouse).
- **Merging docs**: `docs/api/merging.md` + nav entry (`mkdocs.yml`) and README/index/getting-started links.

### Changed
- **Merging core split** into `base` plus per-backend modules (`duckdb`/`postgres`/`clickhouse` + factory) via `make_merge_ops`, mirroring `sorting`/`reshape`/`selection`; SQL fingerprints prove byte-identical output. Hooks: `_auto_cast_join_columns`, `_create_table_as`.

## [0.6.0] - 2026-09-12

### Added
- **Reshape (new umbrella)**: 8 ops via `ContextManager` (`explode`/`aexplode`, `melt`/`amelt`, `pivot`/`apivot`, `pivot_table`/`apivot_table`, `crosstab`/`acrosstab`, `transpose`/`atranspose`, `rank`/`arank`, `groupby_rank`/`agroupby_rank`) backed by backend-native SQL across DuckDB, PostgreSQL, and ClickHouse. Coverage includes bracket-list explode, melt unpivot, pivot with duplicate guard, pivot_table aggregation, crosstab frequency/aggregation with `margins`/`normalize`, stateless transpose (ClickHouse single-query vs TEMP tables on DuckDB/Postgres), and `rank`/`groupby_rank` with `average`/`min`/`max`/`dense`/`first` + `pct`.
- **Reshape docs**: `docs/api/reshape.md` under **Reshape** umbrella + nav entry (`mkdocs.yml:62`) and README link; `mkdocs build --strict` passes.
- **Reshape tests**: `tests/unit/test_reshape_response.py` (18: happy/error/chunked/public API) + `test_reshape_sql_fingerprint.py` (11 scenarios × 3 backends, fixture `reshape_sql_fingerprint.json`) + `tests/integration/ops/test_reshape.py` (8 ops × 3 backends, 8/8 on DuckDB/Postgres/ClickHouse).

### Fixed
- `ReshapingOps._generate_transient_table_name`: `fetch_val` → `fetchval` (all 8 ops previously errored with `'DuckDBBackend' has no attribute 'fetch_val'`).
- `crosstab` `margins` SQL: col-name parse `rstrip` → `strip().rstrip` (trailing newline left `"), SUM("` syntax error).
- `transpose` integration: order-insensitive `sort_values("column_name")` + `astype(str)` for ClickHouse `Nullable(String)`; `explode` fixture bracketed strings (uploader cannot ingest real `list<>` columns).
- `_fetch_in_chunks`: transient-schema fallback (`memframe_transient` on `deep_cache` move) — 8 call sites now pass `backend=` (mirrors `sorting`).

### Changed
- **Reshape core split** into `base` plus per-backend modules (`duckdb`/`postgres`/`clickhouse` + factory) via `make_reshaping_ops`, mirroring `sorting`/`selection`; SQL fingerprints prove byte-identical output. Hooks: `_safe_numeric_expr`, `_get_table_columns`, `_build_explode_sql`, `_count_all_expr`/`_filtered_agg_expr`/`_normalize_ratio_expr`, `_transpose_unpivot_source`, `_pct_rank_expr`.

## [0.5.1] - 2026-09-11

### Changed
- **Sorting core split** into `base` plus per-backend modules (`duckdb`/`postgres`/`clickhouse` + factory) via `make_sorting_ops`, mirroring selection/inspection; SQL fingerprints prove byte-identical output. Responses now use the shared `ok()`/`fail()` envelope (payload-identical).
- Sorting docs point at `core/analytix/sorting/` package instead of the removed single file.

## [0.5.0] - 2026-09-09

### Added
- **Sorting**: `sort_values` via `ContextManager` (`sort_values` / `asort_values`) backed by SQL `ORDER BY` with `ascending`, `na_position` (`first`/`last`, ClickHouse sentinel for non-default), `columns` subset handling, and chunked streaming via async iterator. Coverage includes single- and multi-column sorts, null positioning, and `columns` subset on DuckDB, Postgres, and ClickHouse with result-table persistence.

### Fixed
- Sorting integration harness: corrected column-order expectation for `columns` subset (`columns` + `by` order).

### Changed
- None beyond sorting addition.

## [0.4.0] - 2026-09-08

### Added
- **Datetime Wave 1 ops** (`ctx.dt.*`): day/month names, two-column `diff`, `to_datetime` (format, epoch units, raise/coerce), `between`/`before`/`after` filters, `select_year`/`select_month`.
- **Datetime Wave 2**: `resample` moved from inspection to `ctx.dt` with multi-aggregation, `group_by`, and label control; new `asfreq` reindexing with forward/backward fill.
- **Datetime Wave 3**: calendar-aware `add_offset` (years through days plus Mon–Fri business-day mode).
- **Plotly figures embedded as PNG in plot PDF reports** via a shared `_plot_pdf` helper (kaleido raster branch with graceful placeholder fallback); kaleido added to dev extras.

### Fixed
- **Postgres resample/asfreq SQL**: `mean` maps to `AVG` on all backends, `median` to `percentile_cont` on Postgres; asfreq grid uses native `generate_series` on Postgres.
- **ClickHouse datetime**: day/month names via `DATE_FORMAT`, nullable result column for `to_datetime` coerce, proven `add<Unit>` date-math family plus `IGNORE NULLS` fill windows.
- **Plot PDF teardown**: report rendering no longer calls matplotlib-only `suptitle` on plotly Figures.
- **Python 3.10 compat**: `timezone.utc` replaces `datetime.UTC` / `datetime.utcnow`.

### Changed
- **Datetime core split** into `base` plus per-backend modules (`duckdb`/`postgres`/`clickhouse` + factory), mirroring inspection; SQL fingerprints prove byte-identical output.

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