import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

from memframe.core.ingestion.datatype_detector import Backend
from memframe.exceptions import BackendNotSupported


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

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


class DuckDBPool(BasePool):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None

    @property
    def conn(self):
        return self._conn

    async def connect(self):
        import duckdb
        self._conn = duckdb.connect(self.db_path)
        logger.info(f"DuckDB connected: {self.db_path}")

    async def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connection closed")

    async def execute(self, sql: str, *args) -> None:
        self._conn.execute(sql, list(args))

    async def fetch(self, sql: str, *args) -> List[Tuple]:
        return [tuple(r) for r in self._conn.execute(sql, list(args)).fetchall()]

    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]:
        row = self._conn.execute(sql, list(args)).fetchone()
        return tuple(row) if row else None

    async def fetchval(self, sql: str, *args) -> Any:
        row = await self.fetchrow(sql, *args)
        return row[0] if row else None


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

    async def connect(self):
        import asyncpg
        self._pool = await asyncpg.create_pool(**self._config)
        self._loop = asyncio.get_running_loop()
        logger.info(f"PostgreSQL pool created: {self._config['host']}:{self._config['port']}/{self._config['database']}")

    async def _ensure(self):
        import asyncpg
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
        async with self._pool.acquire() as conn:
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
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args) -> List[Tuple]:
        await self._ensure()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [tuple(r) for r in rows]

    async def fetchrow(self, sql: str, *args) -> Optional[Tuple]:
        await self._ensure()
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
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("ClickHouse client closed")

    @property
    def client(self):
        return self._client

    async def _ensure(self):
        if self._client is None:
            await self.connect()

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

    async def insert(self, table, rows, database=None, column_names=None):
        await self._ensure()
        await self._client.insert(table, rows, database=database, column_names=column_names)

    async def insert_arrow(self, table, arrow_table, database=None):
        await self._ensure()
        await self._client.insert_arrow(table, arrow_table, database=database)


def create_pool(backend: Backend, params: dict) -> BasePool:
    if backend == Backend.DUCKDB:
        return DuckDBPool(params["db_path"])
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
