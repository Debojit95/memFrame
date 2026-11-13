# Connector

Source: `src/memframe/db_manager/connection/connector.py`

The connection lifecycle (pool, backend and uploader) is owned by a
`ConnectorManager` instance; `MemFrame` holds one and delegates to it.
`MemFrame` also owns uploads and dataset operations, so one instance is the
whole public surface. memFrame supports three database backends:

- DuckDB through `connection_type="local"`.
- PostgreSQL through `connection_type="remote"` and `backend="postgres"`.
- ClickHouse through `connection_type="remote"` and `backend="clickhouse"`.

Construct `MemFrame(...)` and call `connect()` (or `await aconnect()`) before
upload or dataset-management APIs.

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
mf.connect()
```

In async code:

```python
await mf.aconnect()
```

Parameters:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `db_path` | No | `memFrame_new.duckdb` | File path for the DuckDB database. Use `:memory:` for a temporary in-memory database (nothing is persisted to disk). |
| `lock_timeout` | No | `0` | Seconds to keep retrying when another process holds the DuckDB file lock. |
| `memory_limit` | No | DuckDB default (80% RAM) | Cap DuckDB's working memory, e.g. `"2GB"`. Excess intermediate data spills to `temp_directory`. |
| `temp_directory` | No | DuckDB default (`.tmp`) | Directory for spilling large sorts/joins. Point this at disk with free space. |
| `threads` | No | all cores | DuckDB worker threads. Lowering it reduces peak memory. |

!!! note
    For a temporary in-memory database, pass `db_path=":memory:"`. The database
    lives and dies with the connection; data is not persisted to disk.

### Large datasets (Colab / small machines)

DuckDB defaults to using up to 80% of system RAM. On a 12 GB Colab runtime
that leaves little headroom for pandas results, and the kernel can be killed.
Cap DuckDB and give it a disk spill directory:

```python
mf = MemFrame(
    connection_type="local",
    connection_params={
        "db_path": "memFrame.duckdb",
        "memory_limit": "2GB",
        "temp_directory": "/content/duck_tmp",
        "threads": 2,
    },
)
```

Use a file-backed database — `:memory:` cannot spill to disk. Pair this with
`preview_rows=N` so row-returning operations materialize a small preview while
the full result stays in DuckDB.

### File locking (DuckDB UI / CLI)

DuckDB allows **one read-write process per database file**. If the file is
open in the DuckDB UI/CLI, `aconnect()` raises a `ConfigurationError` naming
the blocking process instead of a raw `IOException`:

```text
DuckDB database file 'memdb.duckdb' is locked by /path/to/duckdb (PID 1234).
...
```

Read-only opens also fail while a writer holds the lock, so memFrame cannot
share the file. Either close the UI/CLI, retry briefly with
`lock_timeout=5`, or let the UI browse a snapshot:

```bash
cp memdb.duckdb memdb_ui.duckdb
duckdb -ui memdb_ui.duckdb
```

For truly concurrent application + UI access, use PostgreSQL or ClickHouse
as the memFrame backend.

## Result previews

`MemFrame(..., preview_rows=N)` limits the rows materialized into `result`
for feature-generating operations (window, cumulative, grouped cumulative,
cleaning, grouped fillna, group-by). The source table still receives every
row; only the returned preview is capped. Cache hits apply the same limit.

```python
mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
    preview_rows=1000,
)
```

Default is `None`, which returns the full result frame. Use `preview_rows` for
dashboards and benchmarks where scanning millions of rows into pandas is not
needed; read explicit full tables via `head`/`full_table` when required.

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
mf.connect()
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
mf.connect()
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
from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe.duckdb"},
)
await mf.aconnect()
try:
    dataset = await mf.aupload_csv("data/sales.csv")
finally:
    await mf.aclose()
```

Or let the instance manage the lifecycle with an async context manager:

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
