# Connector

Source: `src/main.py`

The connector configuration is passed to `MemFrame` when you create an
instance. memFrame supports three database backends:

- DuckDB through `connection_type="local"`.
- PostgreSQL through `connection_type="remote"` and `backend="postgres"`.
- ClickHouse through `connection_type="remote"` and `backend="clickhouse"`.

Use `await mf.connect()` before calling upload or dataset-management APIs, or
use `MemFrame` as an asynchronous context manager.

## DuckDB

DuckDB is the local backend.

```python
from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={
        "db_path": "memframe.duckdb",
    },
)

await mf.connect()
```

Parameters:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `db_path` | No | `memFrame_new.duckdb` | File path for the DuckDB database. |

!!! note
    memFrame disables in-memory DuckDB for local mode. If `db_path=":memory:"`
    is supplied, memFrame falls back to a file-backed database.

## PostgreSQL

PostgreSQL is configured as a remote backend.

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

await mf.connect()
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
| `schema_prefix` | No | None | Prefix for memFrame-managed schemas. Useful for test isolation or shared databases. |

## ClickHouse

ClickHouse is configured as a remote backend and uses the HTTP interface.

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
        "secure": False,
        "timeout": 10.0,
    },
)

await mf.connect()
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
| `schema_prefix` | No | None | Prefix for memFrame-managed schemas or database namespaces where supported. |

## Schema Prefixes

When `schema_prefix` is provided, memFrame derives isolated schema names for
uploads, transient operation tables, and registry tables:

```python
mf = MemFrame(
    connection_type="remote",
    connection_params={
        "backend": "postgres",
        "host": "localhost",
        "user": "postgres",
        "password": "secret",
        "database": "memframe",
        "schema_prefix": "demo",
    },
)
```

This creates names such as `demo_upload`, `demo_transient`, and
`demo_registry` after sanitization.

## Connection Lifecycle

Use an async context manager when possible:

```python
async with MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
) as mf:
    dataset = await mf.aupload_csv("data/sales.csv")
```

Or connect and close explicitly:

```python
mf = MemFrame(connection_type="local")

await mf.connect()
try:
    dataset = await mf.aupload_csv("data/sales.csv")
finally:
    await mf.close()
```

Upload and dataset-management APIs raise `RuntimeError` if they are called
before a connection is active.
