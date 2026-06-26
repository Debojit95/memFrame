from typing import Any, Dict, Optional
import asyncio
import logging
import asyncpg

from .base import DatabaseAdapter

logger = logging.getLogger("memFrame")


def _loop_is_running(loop: Optional[asyncio.AbstractEventLoop]) -> bool:
    return bool(
        loop
        and loop.is_running()
        and not loop.is_closed()
    )


def _terminate_pool(pool: asyncpg.Pool) -> None:
    try:
        pool.terminate()
    except RuntimeError as exc:
        if "Event loop is closed" not in str(exc):
            raise
        logger.debug("Ignoring asyncpg pool terminate on closed event loop")


# ----------------------------------------------------------------------
# PostgreSQL Adapter
# ----------------------------------------------------------------------
class PostgresAdapter(DatabaseAdapter):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        min_size: int = 0,
        max_size: int = 1,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Optional[asyncpg.Pool] = None
        self._pool_loop: Optional[asyncio.AbstractEventLoop] = None

    async def _create_pool(self) -> asyncpg.Pool:
        return await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            min_size=self.min_size,
            max_size=self.max_size,
        )

    async def _close_pool(
        self,
        pool: asyncpg.Pool,
        pool_loop: Optional[asyncio.AbstractEventLoop],
    ) -> None:
        current_loop = asyncio.get_running_loop()
        try:
            if pool_loop is current_loop:
                await pool.close()
            elif _loop_is_running(pool_loop):
                future = asyncio.run_coroutine_threadsafe(pool.close(), pool_loop)
                await asyncio.wrap_future(future)
            else:
                _terminate_pool(pool)
        except Exception as exc:
            logger.warning("Terminating PostgreSQL pool after close failed: %s", exc)
            _terminate_pool(pool)

    async def connect(self):
        if self.pool is not None:
            await self.close()
        self.pool = await self._create_pool()
        self._pool_loop = asyncio.get_running_loop()

    async def _ensure_pool(self):
        current_loop = asyncio.get_running_loop()
        if self.pool is None or self._pool_loop is not current_loop:
            if self.pool is not None:
                await self._close_pool(self.pool, self._pool_loop)
            self.pool = await self._create_pool()
            self._pool_loop = current_loop

    async def close(self):
        if self.pool:
            await self._close_pool(self.pool, self._pool_loop)
            self.pool = None
            self._pool_loop = None

    async def execute(self, sql: str, *args):
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args):
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def fetchval(self, sql: str, *args):
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def fetchrow(self, sql: str, *args):
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        rows = await self.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = $2
            ORDER BY ordinal_position
            """,
            table,
            schema,
        )
        return {row["column_name"]: row["data_type"] for row in rows}

    async def get_table_info(self, table: str, schema: str) -> Dict[str, Any]:
        count_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        row_count = await self.fetchval(count_sql)

        size_sql = f"""
            SELECT pg_size_pretty(pg_total_relation_size('"{schema}"."{table}"')) as total_size,
                   pg_size_pretty(pg_relation_size('"{schema}"."{table}"')) as table_size
        """
        size_row = await self.fetchrow(size_sql)

        columns = await self.get_column_types(table, schema)
        return {
            "table_name": table,
            "row_count": row_count or 0,
            "column_count": len(columns),
            "total_size": size_row["total_size"] if size_row else "Unknown",
            "table_size": size_row["table_size"] if size_row else "Unknown",
            "columns": columns,
        }

    async def table_exists(self, table: str, schema: str) -> bool:
        result = await self.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = $1 AND table_schema = $2
            )
            """,
            table,
            schema,
        )
        return bool(result)

    def placeholder(self, index: int = 1) -> str:
        return f"${index}"

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    async def fetch_iter(self, sql: str, *args, chunk_size: int = 1000):
        """
        Async streaming iterator over query results.
        Yields rows one by one without loading entire result into memory.
        """
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            # 🔥 Create cursor (streaming)
            stmt = await conn.prepare(sql)

            async with conn.transaction():
                async for record in stmt.cursor(*args, prefetch=chunk_size):
                    yield record
