# Changelog

All notable changes to memFrame are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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