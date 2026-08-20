<div class="memframe-hero">
  <img src="assets/memframe-logo-full.png" alt="memFrame">
</div>

# memFrame

> *memFrame brings a pandas-like DataFrame API to DuckDB, PostgreSQL, and ClickHouse — async-first, with an optional AI agent for natural-language data work.*

## What It Provides

- Database-backed DataFrame API across DuckDB, PostgreSQL, and ClickHouse.
- Each pandas-style call is compiled to backend-native SQL and executed on the engine — your data never leaves the database.
- Async-first surface with sync equivalents for every operation.
- Upload from CSV, Parquet, or pandas DataFrame.
- Inspection, selection, cleaning, statistics, arithmetic, Plotly charts.
- Two-level cache: lineage audit + replayable result tables.
- Optional AI agent layer (`memframe_ai`) for natural-language data work.


## Start Here

1. Read [Architecture](architecture.md) for a map of the call path and subsystems.
2. Follow [Getting Started](getting-started.md) to connect and upload data.
3. Try the [AI Agent](api/agent.md) for natural-language data work.
4. Read [Upload Manager](api/upload-manager.md) for ingestion behavior.
5. Read [Dataset Operations](api/database.md) to manage uploaded datasets.
