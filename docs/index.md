<div class="memframe-hero">
  <img src="assets/memframe-logo-full.png" alt="memFrame">
</div>

# memFrame

memFrame is a database-backed DataFrame processing framework. It uploads tabular
data into DuckDB or PostgreSQL and returns a context that can be used for later
operations.

## What It Provides

- Synchronous and asynchronous APIs.
- Local DuckDB and remote PostgreSQL backends.
- CSV, Parquet, and pandas DataFrame uploads.
- Column-name normalization and data type inference.
- Dataset registry, active-dataset selection, and operation history.
- Backup and restore for file-backed DuckDB databases.

## Start Here

1. Follow [Getting Started](getting-started.md) to connect and upload data.
2. Read [Upload Manager](api/upload-manager.md) for ingestion behavior.
3. Read [Dataset Operations](api/database.md) to manage uploaded datasets.
