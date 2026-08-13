# Changelog

All notable changes to memFrame are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres
to [Semantic Versioning](https://semver.org/).

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