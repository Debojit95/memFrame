# Connector

Source: `src/memframe/db_manager/connection/connector.py`

The connection lifecycle (pool, backend and uploader) is owned by a
`ConnectorManager` instance; `MemFrame` holds one and delegates to it.
The module-level facade configures one default `MemFrame`; use the class
directly when you need more than one connection. memFrame supports three
database backends:

- DuckDB through `connection_type="local"`.
- PostgreSQL through `connection_type="remote"` and `backend="postgres"`.
- ClickHouse through `connection_type="remote"` and `backend="clickhouse"`.

Call `mf.connect(...)` (or `await mf.aconnect(...)`) before upload or
dataset-management APIs.

## DuckDB

DuckDB is the local backend.

```python
import memframe as mf

mf.connect(
    connection_type="local",
    connection_params={
        "db_path": "memframe.duckdb",
    },
)
```

In async code:

```python
await mf.aconnect(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)
```

Parameters:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `db_path` | No | `memFrame_new.duckdb` | File path for the DuckDB database. Use `:memory:` for a temporary in-memory database (nothing is persisted to disk). |

!!! note
    For a temporary in-memory database, pass `db_path=":memory:"`. The database
    lives and dies with the connection; data is not persisted to disk.

## PostgreSQL

PostgreSQL is configured as a remote backend.

```python
import memframe as mf

mf.connect(
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

Parameters:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `backend` | Yes | None | Must be `postgres`. |
| `host` | Yes | None | PostgreSQL host name or IP address. |
| `port` | No | `5432` | PostgreSQL server port. |
| `user` | Yes | None | Database user. |
| `password` | Yes | None | Database password. |
| `database` | Yes | None | Target database name. memFrame attempts to create it when it is missing and the user has permission. |

## ClickHouse

ClickHouse is configured as a remote backend and uses the HTTP interface.

```python
import memframe as mf

mf.connect(
    connection_type="remote",
    connection_params={
        "backend": "clickhouse",
        "host": "localhost",
        "port": 8123,
        "user": "default",
        "password": "secret",
        "database": "default",
        "secure": False,
        "timeout": 10.0,
    },
)
```

Parameters:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `backend` | Yes | None | Must be `clickhouse`. |
| `host` | Yes | None | ClickHouse host name or IP address. |
| `port` | No | `8123` | ClickHouse HTTP port. |
| `user` | Yes | None | ClickHouse user. |
| `password` | Yes | None | ClickHouse password. |
| `database` | No | server default | Database used for ClickHouse tables. |
| `secure` | No | `False` | Whether to use HTTPS for the backend connection. |
| `timeout` | No | `10.0` | Request timeout in seconds. |

## Connection Lifecycle

Connect and close explicitly:

```python
import memframe as mf

await mf.aconnect(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)
try:
    dataset = await mf.aupload_csv("data/sales.csv")
finally:
    await mf.aclose()
```

Or manage an explicit instance with an async context manager:

```python
from memframe import MemFrame

async with MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
) as frame:
    dataset = await frame.aupload_csv("data/sales.csv")
```

Upload and dataset-management APIs raise `RuntimeError` if they are called
before a connection is active.
