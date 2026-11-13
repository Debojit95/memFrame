import asyncio
import logging
import re
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, List, Optional, Tuple

import pandas as pd

from memframe.core.ingestion.datatype_detector import Backend
from memframe.exceptions import (
    BackendNotSupported, ConfigurationError, ConnectionNotReady,
)


logger = logging.getLogger("memFrame")

class BasePool(ABC):
    @abstractmethod
    async def connect(self): ...

    @abstractmethod
    async def close(self): ...

    @abstractmethod
    async def execute(self, sql: str, *args) -> None: ...

    @abstractmethod
    async def fetch(self, sql: str, *args) -> List[Tuple]: ...

    @abstractmethod
    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]: ...

    @abstractmethod
    async def fetchval(self, sql: str, *args) -> Any: ...

    async def fetch_df(self, sql: str, *args):
        """Execute a SELECT and return a pandas DataFrame."""
        import pandas as pd

        rows = await self.fetch(sql, *args)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records(rows)


class DuckDBPool(BasePool):
    def __init__(
        self,
        db_path: str,
        lock_timeout: float = 0.0,
        memory_limit: Optional[str] = None,
        temp_directory: Optional[str] = None,
        threads: Optional[int] = None,
    ):
        self.db_path = db_path
        self.lock_timeout = max(float(lock_timeout or 0), 0.0)
        self.memory_limit = memory_limit
        self.temp_directory = temp_directory
        self.threads = threads
        self._conn = None
        # ponytail: one connection serves every coroutine — serialize use so two
        # ops can't interleave on it (same pattern as PostgresPool._ensure_lock).
        self._lock = asyncio.Lock()
        self._lock_owner = None
        # Open transaction owner: a nested transaction() on the same task joins
        # the outer one instead of issuing a second BEGIN on this connection.
        self._txn_owner = None

    @asynccontextmanager
    async def _guarded(self):
        """Hold the pool lock, re-entrant for the owning task.

        Lets transaction() wrap pool-method calls without deadlocking on the
        non-re-entrant asyncio.Lock.
        """
        current = asyncio.current_task()
        if current is not None and self._lock_owner is current:
            yield
            return
        async with self._lock:
            self._lock_owner = current
            try:
                yield
            finally:
                self._lock_owner = None

    @asynccontextmanager
    async def transaction(self):
        """All-or-nothing block: every statement inside commits or rolls back together."""
        current = asyncio.current_task()
        if current is not None and self._txn_owner is current:
            yield  # nested: join the outer transaction, no second BEGIN
            return
        async with self._guarded():
            conn = self._require_conn()
            self._txn_owner = current
            conn.execute("BEGIN")
            try:
                yield
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
            finally:
                self._txn_owner = None

    @property
    def conn(self):
        return self._conn

    @staticmethod
    def _lock_error(db_path: str, exc: Exception) -> ConfigurationError:
        text = str(exc)
        match = re.search(
            r"Conflicting lock is held in (.+?) \(PID (\d+)\)", text
        )
        holder = (
            f"{match.group(1)} (PID {match.group(2)})"
            if match else "another process"
        )
        return ConfigurationError(
            f"DuckDB database file '{db_path}' is locked by {holder}. "
            "DuckDB allows only one read-write process per file, and "
            "read-only opens also fail while a writer holds the lock. "
            "Close the DuckDB UI/CLI or the other memFrame process, then "
            "retry. To browse a snapshot in the UI, copy the file first:\n"
            f"    cp '{db_path}' '{db_path}.ui'\n"
            f"    duckdb -ui '{db_path}.ui'\n"
            "For concurrent application + UI access, use PostgreSQL or "
            "ClickHouse as the memFrame backend."
        )

    async def connect(self):
        import duckdb
        # ponytail: close any prior handle first — DuckDB is single-writer, so
        # a leaked second connection keeps the database file locked.
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                self._conn = duckdb.connect(self.db_path)
                break
            except duckdb.IOException as exc:
                if "Conflicting lock" not in str(exc):
                    raise
                if time.monotonic() >= deadline:
                    raise self._lock_error(self.db_path, exc) from exc
                logger.warning(
                    "DuckDB file %s is locked; retrying for up to %.1fs",
                    self.db_path, self.lock_timeout,
                )
                await asyncio.sleep(0.5)
        # ponytail: DuckDB draws a Jupyter progress-bar widget into the cell
        # output for queries >2s (a black bar whose injected JS errors on
        # Colab). Disable it for every session; config= at connect is rejected
        # as a global option, so use session SETs. enable_progress_bar_print
        # also keeps terminal output quiet.
        try:
            self._conn.execute("SET enable_progress_bar = false")
            self._conn.execute("SET enable_progress_bar_print = false")
        except Exception as exc:
            logger.debug("DuckDB progress-bar disable skipped: %s", exc)
        for name, value in (
            ("memory_limit", self.memory_limit),
            ("temp_directory", self.temp_directory),
            ("threads", self.threads),
        ):
            if value is None:
                continue
            self._conn.execute(
                f"SET {name} = {self._render_setting(name, value)}"
            )
        logger.info(f"DuckDB connected: {self.db_path}")

    @staticmethod
    def _render_setting(name: str, value: Any) -> str:
        """Render a SET value, rejecting anything that could break out of it."""
        if name == "memory_limit":
            text = str(value).strip()
            if not re.fullmatch(
                r"\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?",
                text,
                re.IGNORECASE,
            ):
                raise ConfigurationError(
                    f"Invalid memory_limit {value!r}; use e.g. '2GB'"
                )
            return f"'{text}'"
        if name == "threads":
            try:
                count = int(value)
            except (TypeError, ValueError) as exc:
                raise ConfigurationError(
                    f"Invalid threads {value!r}; use a positive integer"
                ) from exc
            if count <= 0:
                raise ConfigurationError(
                    f"Invalid threads {value!r}; use a positive integer"
                )
            return str(count)
        # temp_directory is a path string.
        return "'" + str(value).replace("'", "''") + "'"

    async def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connection closed")

    def _require_conn(self):
        if self._conn is None:
            raise ConnectionNotReady("DuckDB not connected. Call connect() first.")
        return self._conn

    async def execute(self, sql: str, *args) -> None:
        async with self._guarded():
            self._require_conn().execute(sql, list(args))

    async def fetch(self, sql: str, *args) -> List[Tuple]:
        async with self._guarded():
            cursor = self._require_conn().execute(sql, list(args))
            return [tuple(r) for r in cursor.fetchall()]

    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]:
        async with self._guarded():
            row = self._require_conn().execute(sql, list(args)).fetchone()
            return tuple(row) if row else None

    async def fetchval(self, sql: str, *args) -> Any:
        row = await self.fetchrow(sql, *args)
        return row[0] if row else None

    async def fetch_df(self, sql: str, *args):
        async with self._guarded():
            df = self._require_conn().execute(sql, list(args)).df()
            for col in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue
                series = df[col]
                if getattr(series.dt, "tz", None) is not None:
                    series = series.dt.tz_convert("UTC").dt.tz_localize(None)
                df[col] = series.astype("datetime64[ns]")
            return df


def _terminate_pool(pool) -> None:
    try:
        pool.terminate()
    except RuntimeError as exc:
        if "Event loop is closed" not in str(exc):
            raise
        logger.debug("Ignoring pool terminate on closed event loop")


class PostgresPool(BasePool):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        min_size: int = 0,
        max_size: int = 4,
        command_timeout: int = 300,
    ):
        self._config = dict(
            host=host, port=port, user=user,
            password=password, database=database,
            min_size=min_size, max_size=max_size,
            command_timeout=command_timeout,
        )
        self._pool = None
        self._loop = None
        # ponytail: 3.10+ asyncio primitives grab the running loop at await
        # time, so one lock instance works across sequential event loops.
        self._ensure_lock = asyncio.Lock()
        # Active transaction connection, when a transaction() block is open on
        # the current task. Pool calls run on it instead of acquiring a fresh
        # connection, so the whole block commits or rolls back together.
        self._txn_conn = None
        self._txn_owner = None

    @asynccontextmanager
    async def transaction(self):
        """All-or-nothing block on one held connection (asyncpg transaction)."""
        current = asyncio.current_task()
        if current is not None and self._txn_owner is current:
            yield  # nested: join the outer transaction, don't open a new one
            return
        await self._ensure()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                self._txn_conn, self._txn_owner = conn, current
                try:
                    yield
                finally:
                    self._txn_conn, self._txn_owner = None, None

    def _txn_conn_for_current(self):
        if self._txn_conn is not None and self._txn_owner is asyncio.current_task():
            return self._txn_conn
        return None

    async def connect(self):
        import asyncpg
        self._pool = await asyncpg.create_pool(**self._config)
        self._loop = asyncio.get_running_loop()
        logger.info(f"PostgreSQL pool created: {self._config['host']}:{self._config['port']}/{self._config['database']}")

    async def _ensure(self):
        import asyncpg
        # ponytail: recreate-on-loop-change stays as the fallback for mixed
        # sync/async usage on foreign loops; the shared sync loop makes it a
        # no-op for sync-driven workloads. The lock prevents the create race
        # where two coroutines both see _pool=None and leak the loser's pool.
        async with self._ensure_lock:
            loop = asyncio.get_running_loop()
            if self._pool is None or self._loop is not loop:
                if self._pool:
                    await self._close_pool(self._pool, self._loop)
                self._pool = await asyncpg.create_pool(**self._config)
                self._loop = loop

    async def _close_pool(self, pool, pool_loop):
        loop = asyncio.get_running_loop()
        try:
            if pool_loop is loop:
                await pool.close()
            else:
                _terminate_pool(pool)
        except Exception as exc:
            logger.warning("Closing PG pool failed: %s", exc)
            _terminate_pool(pool)

    async def close(self):
        async with self._ensure_lock:
            if self._pool:
                await self._close_pool(self._pool, self._loop)
                self._pool = None
                self._loop = None
                logger.info("PostgreSQL pool closed")

    @property
    def pool(self):
        return self._pool

    async def copy_to_table(
        self, table, source, columns, schema_name,
        format, header, encoding, null=None,
    ):
        await self._ensure()
        owned = self._txn_conn_for_current()
        if owned is not None:
            await self._copy_with(owned, table, source, columns, schema_name,
                                  format, header, encoding, null)
            return
        async with self._pool.acquire() as conn:
            await self._copy_with(conn, table, source, columns, schema_name,
                                  format, header, encoding, null)

    @staticmethod
    async def _copy_with(conn, table, source, columns, schema_name,
                         format, header, encoding, null):
        kwargs = dict(
            source=source, columns=columns,
            schema_name=schema_name, format=format,
            header=header, encoding=encoding,
        )
        # ponytail: only forward `null` when explicitly provided —
        # asyncpg rejects null=None (must be a non-empty marker string).
        if null is not None:
            kwargs["null"] = null
        await conn.copy_to_table(table, **kwargs)

    async def execute(self, sql: str, *args) -> None:
        await self._ensure()
        owned = self._txn_conn_for_current()
        if owned is not None:
            await owned.execute(sql, *args)
            return
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args) -> List[Tuple]:
        await self._ensure()
        owned = self._txn_conn_for_current()
        if owned is not None:
            rows = await owned.fetch(sql, *args)
            return [tuple(r) for r in rows]
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [tuple(r) for r in rows]

    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]:
        await self._ensure()
        owned = self._txn_conn_for_current()
        if owned is not None:
            row = await owned.fetchrow(sql, *args)
            return tuple(row) if row else None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return tuple(row) if row else None

    async def fetchval(self, sql: str, *args) -> Any:
        row = await self.fetchrow(sql, *args)
        return row[0] if row else None


class ClickHousePool(BasePool):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        secure: bool = False,
        timeout: float = 300.0,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.timeout = timeout
        self._client = None
        self._ensure_lock = asyncio.Lock()

    async def connect(self):
        client = None
        try:
            from memframe.db_manager.adapters.clickhouse import ClickHouseConnectClient

            client = ClickHouseConnectClient(
                host=self.host, port=self.port,
                username=self.user, password=self.password,
                database=self.database,
                secure=self.secure, timeout=self.timeout,
            )
            # Validate reachability so the httpx fallback is genuinely reachable
            # (clickhouse-connect connects lazily, so a SELECT 1 proves the server
            # accepts this client before we commit to it).
            await client.command("SELECT 1")
            self._client = client
            logger.info(f"ClickHouse client created (clickhouse-connect): {self.host}:{self.port}")
            return
        except Exception as exc:  # ImportError, auth, network, protocol mismatch, ...
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass
            logger.warning(
                "clickhouse-connect unavailable (%s); falling back to httpx", exc
            )

        from memframe.db_manager.adapters.clickhouse import HttpxClickHouseClient

        self._client = HttpxClickHouseClient(
            host=self.host, port=self.port,
            username=self.user, password=self.password,
            database=self.database,
            secure=self.secure, timeout=self.timeout,
        )
        logger.info(f"ClickHouse client created (httpx fallback): {self.host}:{self.port}")

    async def close(self):
        async with self._ensure_lock:
            if self._client:
                await self._client.close()
                self._client = None
                logger.info("ClickHouse client closed")

    @property
    def client(self):
        return self._client

    async def _ensure(self):
        # lock prevents the double-connect race where both coroutines see
        # _client=None and the first-created client leaks
        async with self._ensure_lock:
            if self._client is None:
                await self.connect()

    @asynccontextmanager
    async def transaction(self):
        # ponytail: ClickHouse has no transactions — passthrough. Callers do
        # best-effort compensation (drop partial tables) on failure instead.
        yield

    async def execute(self, sql: str, *args) -> None:
        await self._ensure()
        await self._client.command(sql, parameters=args if args else None)

    async def fetch(self, sql: str, *args) -> List[Tuple]:
        await self._ensure()
        result = await self._client.query(sql, parameters=args if args else None)
        return result.result_rows

    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]:
        await self._ensure()
        result = await self._client.query(sql, parameters=args if args else None)
        return result.first_row

    async def fetchval(self, sql: str, *args) -> Any:
        row = await self.fetchrow(sql, *args)
        return row[0] if row else None

    async def fetch_df(self, sql: str, *args):
        import pandas as pd

        await self._ensure()
        result = await self._client.query(sql, parameters=args if args else None)
        columns = getattr(result, "column_names", None)
        return pd.DataFrame(result.result_rows, columns=columns)

    async def insert(self, table, rows, database=None, column_names=None):
        await self._ensure()
        await self._client.insert(table, rows, database=database, column_names=column_names)

    async def insert_arrow(self, table, arrow_table, database=None):
        await self._ensure()
        await self._client.insert_arrow(table, arrow_table, database=database)


def create_pool(backend: Backend, params: dict) -> BasePool:
    if backend == Backend.DUCKDB:
        return DuckDBPool(
            params["db_path"], lock_timeout=params.get("lock_timeout", 0),
            memory_limit=params.get("memory_limit"),
            temp_directory=params.get("temp_directory"),
            threads=params.get("threads"),
        )
    elif backend == Backend.POSTGRES:
        return PostgresPool(
            host=params["host"], port=params.get("port", 5432),
            user=params["user"], password=params["password"],
            database=params["database"],
            command_timeout=params.get("command_timeout", 300),
        )
    elif backend == Backend.CLICKHOUSE:
        return ClickHousePool(
            host=params["host"], port=params.get("port", 8123),
            user=params["user"], password=params["password"],
            database=params.get("database"),
            secure=params.get("secure", False),
            timeout=params.get("timeout", 300.0),
        )
    else:
        raise BackendNotSupported(f"Unsupported backend: {backend}")
