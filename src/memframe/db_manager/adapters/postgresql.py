from typing import Any, Dict
import logging

from .base import DatabaseAdapter

logger = logging.getLogger("memFrame")


class PostgresAdapter(DatabaseAdapter):
    def __init__(self, pool):
        super().__init__(pool)
        self._pg_pool = pool

    @classmethod
    def connection_params(cls, conn_params: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            "host": conn_params["host"],
            "port": conn_params.get("port", 5432),
            "user": conn_params["user"],
            "password": conn_params["password"],
            "database": conn_params["database"],
        }
        return params

    async def execute(self, sql: str, *args):
        return await self._pool.execute(sql, *args)

    async def fetch(self, sql: str, *args):
        await self._pg_pool._ensure()
        async with self._pg_pool.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(r) for r in rows]

    async def fetchval(self, sql: str, *args):
        return await self._pool.fetchval(sql, *args)

    async def fetchrow(self, sql: str, *args):
        await self._pg_pool._ensure()
        async with self._pg_pool.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row else None

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
        quoted_full = f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"
        count_sql = f"SELECT COUNT(*) FROM {quoted_full}"
        row_count = await self.fetchval(count_sql)

        # ponytail: pass the quoted name as a regclass bind param — stuffing it
        # into a string literal corrupts names containing single quotes.
        size_sql = """
            SELECT pg_size_pretty(pg_total_relation_size($1::regclass)) as total_size,
                   pg_size_pretty(pg_relation_size($1::regclass)) as table_size
        """
        size_row = await self.fetchrow(size_sql, quoted_full)

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
        return '"' + name.replace('"', '""') + '"'

    async def fetch_iter(self, sql: str, *args, chunk_size: int = 1000):
        """
        Async streaming iterator over query results.
        Yields rows one by one without loading entire result into memory.
        """
        await self._pg_pool._ensure()
        async with self._pg_pool.pool.acquire() as conn:
            stmt = await conn.prepare(sql)
            async with conn.transaction():
                async for record in stmt.cursor(*args, prefetch=chunk_size):
                    yield record
