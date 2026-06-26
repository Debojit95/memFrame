# Getting Started

memFrame needs an active database connection before uploading or managing
datasets. Local connections use DuckDB; remote connections use PostgreSQL.

## Local DuckDB

The asynchronous context manager connects on entry and closes the database on
exit:

```python
import asyncio

from src.memframe.main import MemFrame


async def main():
    async with MemFrame(
        connection_type="local",
        connection_params={"db_path": "memframe.duckdb"},
    ) as mf:
        dataset = await mf.aupload_csv("data/sales.csv")
        print(dataset)


asyncio.run(main())
```

For synchronous upload and dataset-management methods, connect first:

```python
import asyncio

from src.memframe.main import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)

asyncio.run(mf.connect())

dataset = mf.upload_csv("data/sales.csv")
tables = mf.list_tables()

print(tables)

asyncio.run(mf.close())
```

!!! note
    Local mode requires a file-backed DuckDB database. If `":memory:"` is
    supplied, memFrame uses `totem_new.duckdb` instead.

## Remote PostgreSQL

```python
import asyncio

from src.memframe.main import MemFrame


async def main():
    async with MemFrame(
        connection_type="remote",
        connection_params={
            "backend": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "secret",
            "database": "memframe",
        },
    ) as mf:
        dataset = await mf.aupload_parquet("data/events.parquet")
        print(dataset)


asyncio.run(main())
```

## What an Upload Returns

`upload_csv`, `upload_parquet`, and `upload_df` return a `ContextManager` bound
to the newly uploaded dataset. Internally, each dataset also receives a unique
six-character `data_id`, which is used by the registry and dataset-operation
APIs.

Continue with the [Upload Manager](api/upload-manager.md) and
[Dataset Operations](api/database.md) guides.
