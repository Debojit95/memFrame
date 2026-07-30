import logging
from typing import Any, Dict, Optional

from memframe.db_manager.pool import BasePool, DuckDBPool

from .base import DatabaseAdapter

logger = logging.getLogger("memFrame")


class DuckDBAdapter(DatabaseAdapter):
    def __init__(self, pool: BasePool):
        super().__init__(pool)
        self._ddb_pool: DuckDBPool = pool  # type: ignore[assignment]

    @classmethod
    def connection_params(cls, conn_params: Dict[str, Any]) -> Dict[str, Any]:
        db_path = conn_params.get("db_path", "memFrame_new.duckdb")
        if db_path == ":memory:":
            logger.warning(
                "In-memory DuckDB is disabled for local mode; using "
                "'memFrame_new.duckdb' instead."
            )
            db_path = "memFrame_new.duckdb"
        return {"db_path": db_path}

    async def execute(self, sql: str, *args):
        self._ddb_pool.conn.execute(sql, list(args))

    async def fetch(self, sql: str, *args):
        cursor = self._ddb_pool.conn.execute(sql, list(args))
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    async def fetchrow(self, sql: str, *args):
        cursor = self._ddb_pool.conn.execute(sql, list(args))
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row))

    async def fetchval(self, sql: str, *args):
        cursor = self._ddb_pool.conn.execute(sql, list(args))
        row = cursor.fetchone()
        return row[0] if row else None

    async def get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        resolved_schema = schema if schema else "main"
        rows = await self.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            resolved_schema,
            table,
        )
        return {row["column_name"]: row["data_type"] for row in rows}

    async def get_table_info(self, table: str, schema: str) -> Dict[str, Any]:
        quoted_full = f'"{schema}"."{table}"' if schema and schema != "main" else f'"{table}"'
        count_sql = f"SELECT COUNT(*) FROM {quoted_full}"
        row_count = await self.fetchval(count_sql)
        columns = await self.get_column_types(table, schema)
        return {
            "table_name": table,
            "row_count": row_count or 0,
            "column_count": len(columns),
            "total_size": "N/A (DuckDB)",
            "table_size": "N/A (DuckDB)",
            "columns": columns,
        }

    async def table_exists(self, table: str, schema: str) -> bool:
        resolved_schema = schema if schema else "main"
        result = await self.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            resolved_schema,
            table,
        )
        return result > 0

    def placeholder(self, index: int = 1) -> str:
        return "?"

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    async def fetch_iter(self, sql, *args, chunk_size=1000):
        cursor = self._ddb_pool.conn.execute(sql, list(args))
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                yield row
