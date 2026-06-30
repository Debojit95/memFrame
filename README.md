<p align="center">
  <img src="docs/assets/memframe-logo-full.png" alt="memFrame logo" width="720">
</p>

# memFrame

memFrame is a Python package for working with database-backed DataFrame operations. It lets you upload CSV files, Parquet files, and pandas DataFrames into a local DuckDB database or a remote PostgreSQL database, then run DataFrame-style inspection, selection, cleaning, and table-management operations through one consistent API.

The package is designed for workflows where data may be larger than what you want to repeatedly pull into memory, while still keeping a familiar pandas-like developer experience.

## Features

- Upload CSV, Parquet, and pandas DataFrame inputs.
- Use local DuckDB for file-backed analytics.
- Use remote PostgreSQL for server-backed workflows.
- Work with uploaded datasets through a context object.
- Inspect data with APIs such as `head`, `tail`, `describe`, `dtypes`, `shape`, `columns`, and null analysis helpers.
- Select data with `loc`, `iloc`, `at`, `iat`, `where`, `take`, `get`, and dtype-based selection.
- Clean data with helpers such as `fillna`, `dropna`, `drop_duplicates`, `clip`, outlier handling, rare-value compression, and value mapping.
- Choose async APIs for event-loop based apps or sync APIs for scripts and notebooks.

## Installation

```bash
pip install memframe
```

For local development from this repository:

```bash
pip install -e .
```

memFrame requires Python 3.10 or newer.

## Quick Start

```python
import asyncio
import pandas as pd

from memframe import MemFrame


async def main():
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": "memFrame.duckdb"},
    )
    await mf.connect()

    df = pd.DataFrame(
        {
            "id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 82.0, None],
            "active": [True, False, True],
        }
    )

    customers = await mf.aupload_df(df, filename="customers")

    preview = await customers.ahead(n=5)
    print(preview["result"])

    await mf.close()


asyncio.run(main())
```

## Connection Examples

### Local DuckDB

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memFrame.duckdb"},
)
```

### Remote PostgreSQL

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="remote",
    connection_params={
        "backend": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "memframe",
    },
)
```

## Upload Data

Each upload returns a dataset context. Use that context to inspect, select, clean, and transform the uploaded data.

```python
dataset = await mf.aupload_csv("data/customers.csv")
dataset = await mf.aupload_parquet("data/events.parquet")
dataset = await mf.aupload_df(df, filename="customers")
```

Sync upload methods are also available:

```python
dataset = mf.upload_csv("data/customers.csv")
dataset = mf.upload_parquet("data/events.parquet")
dataset = mf.upload_df(df, filename="customers")
```

## Inspect Data

```python
head = await dataset.ahead(n=10)
summary = await dataset.adescribe()
types = await dataset.adtypes()
shape = await dataset.ashape()
columns = await dataset.acolumns()

print(head["result"])
print(summary["result"])
```

Sync equivalents use the same names without the `a` prefix:

```python
print(dataset.head(n=10)["result"])
print(dataset.describe()["result"])
print(dataset.dtypes()["result"])
```

## Select Data

```python
rows = await dataset.ailoc(row_indexer="0:100", columns=["id", "name", "score"])
active = await dataset.aloc(row_selector="active = true")
score = await dataset.aat(row_label=101, column_label="score", index_column="id")
filtered = await dataset.awhere(cond="score > 85", other=None)

print(rows["result"])
```

Sync equivalents:

```python
rows = dataset.iloc(row_indexer="0:100", columns=["id", "name", "score"])
active = dataset.loc(row_selector="active = true")
score = dataset.at(row_label=101, column_label="score", index_column="id")
```

## Clean Data

```python
filled = await dataset.afillna(column="score", method="mean")
deduped = await dataset.adrop_duplicates(subset=["id"])
clipped = await dataset.aclip(column="score", lower=0, upper=100)
valid = await dataset.afilter_valid(column="name", valid_values=["Alice", "Bob"])

print(filled["result"])
```

Sync equivalents:

```python
filled = dataset.fillna(column="score", method="mean")
deduped = dataset.drop_duplicates(subset=["id"])
clipped = dataset.clip(column="score", lower=0, upper=100)
```

## Manage Tables

```python
tables = await mf.alist_tables()
active = await mf.aget_active_table()

print(tables)
print(active)
```

Sync equivalents:

```python
print(mf.list_tables())
print(mf.get_active_table())
```

## Async and Sync APIs

memFrame exposes both async and sync methods:

- Async methods are prefixed with `a`, for example `aupload_df`, `ahead`, `ailoc`, and `afillna`.
- Sync methods do not use the prefix, for example `upload_df`, `head`, `iloc`, and `fillna`.
- Do not call sync APIs from inside a running event loop. In async code, use the async variant with `await`.

For a normal sync script, connect and close with `asyncio.run`, then use sync dataset methods outside any async function:

```python
import asyncio
import pandas as pd

from memframe import MemFrame

mf = MemFrame(connection_type="local", connection_params={"db_path": "memFrame.duckdb"})
asyncio.run(mf.connect())

dataset = mf.upload_df(pd.DataFrame({"id": [1, 2], "value": [10, 20]}))
print(dataset.head(n=2)["result"])

asyncio.run(mf.close())
```

## Documentation

The API documentation is available in the `docs/api` directory and can be served locally with MkDocs:

```bash
mkdocs serve
```

## Development

```bash
pip install -e .
python -m pytest
```
