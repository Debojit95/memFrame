# Getting Started

memFrame works with database-backed datasets. You connect once, upload a CSV,
Parquet file, or pandas DataFrame, and then use the returned dataset context for
inspection, selection, cleaning, statistics, and plots.

Supported backends:

- DuckDB for local file-backed analytics.
- PostgreSQL for server-backed workflows.
- ClickHouse for HTTP-backed analytical workloads.

## Install

```bash
pip install memframe
```

For local development from this repository:

```bash
pip install -e .
```

## Quick Start

```python
import asyncio
import pandas as pd

from memframe import MemFrame


async def main():
    frame = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "region": ["east", "west", "east"],
            "revenue": [1250.0, 980.5, 1430.0],
        }
    )

    async with MemFrame(
        connection_type="local",
        connection_params={"db_path": "memframe.duckdb"},
    ) as mf:
        dataset = await mf.aupload_df(frame, filename="customers")

        preview = await dataset.ahead(n=5)
        average = await dataset.amean(column="revenue")

        print(preview["result"])
        print(average["result"])


asyncio.run(main())
```

The upload returns a `ContextManager` bound to the new dataset. Most operations
are available directly on that dataset:

```python
dataset.head(n=5)
dataset.select_dtypes(include=["numeric"])
dataset.fillna(value=0)
dataset.mean(column="revenue")
dataset.bar(x="region", y="revenue")
```

## Connect

Choose a backend with `connection_type` and `connection_params`.

### DuckDB

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)
```

Local mode uses DuckDB. If `db_path` is omitted, memFrame uses
`memFrame_new.duckdb`. In-memory DuckDB is disabled for local mode; passing
`:memory:` falls back to a file-backed database.

### PostgreSQL

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="remote",
    connection_params={
        "backend": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "secret",
        "database": "memframe",
    },
)
```

### ClickHouse

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="remote",
    connection_params={
        "backend": "clickhouse",
        "host": "localhost",
        "port": 8123,
        "user": "default",
        "password": "secret",
        "database": "default",
    },
)
```

See the [Connector](api/connector.md) guide for backend-specific connection
parameters.

## Sync Usage

Synchronous methods are available for scripts and notebooks. Connect and close
with `asyncio.run`, then use the sync wrappers:

```python
import asyncio

from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)

asyncio.run(mf.connect())

dataset = mf.upload_csv("data/sales.csv")
print(dataset.head(n=5)["result"])
print(mf.list_tables())

asyncio.run(mf.close())
```

## Upload Data

```python
dataset = await mf.aupload_csv("data/customers.csv")
dataset = await mf.aupload_parquet("data/events.parquet")
dataset = await mf.aupload_df(frame, filename="customers")
```

Sync forms are also available:

```python
dataset = mf.upload_csv("data/customers.csv")
dataset = mf.upload_parquet("data/events.parquet")
dataset = mf.upload_df(frame, filename="customers")
```

Each upload creates backend tables and records a six-character `data_id` in the
registry. Dataset management APIs use that `data_id` when listing, activating,
or deleting datasets.

## Next Steps

- [Connector](api/connector.md): configure DuckDB, PostgreSQL, or ClickHouse.
- [Upload Manager](api/upload-manager.md): understand ingestion behavior.
- [Dataset Operations](api/database.md): list, activate, and delete datasets.
- [Inspect](api/inspect.md), [Cleaning](api/cleaning.md), [Selection](api/selection.md), and [Stats](api/stats.md): work with uploaded data.
- [Bar Plots](api/bar.md): create Plotly-backed bar charts.
