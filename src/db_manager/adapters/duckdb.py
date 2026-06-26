from typing import Any, Dict, Optional
import asyncio

import duckdb

from .base import DatabaseAdapter
# ----------------------------------------------------------------------
# DuckDB Adapter
# ----------------------------------------------------------------------


class DuckDBAdapter(DatabaseAdapter):
    def __init__(
        self,
        db_path: str = ":memory:",
        existing_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ):
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = existing_conn
        self._owns_connection = existing_conn is None

    async def connect(self):
        if self.conn is not None:
            return
        # DuckDB's Python API is synchronous, so we run it in a thread
        def _connect():
            return duckdb.connect(self.db_path)
        self.conn = await asyncio.to_thread(_connect)

    async def close(self):
        if self.conn and self._owns_connection:
            self.conn.close()
        if self._owns_connection:
            self.conn = None

    async def execute(self, sql: str, *args):
        def _exec():
            return self.conn.execute(sql, args)
        return await asyncio.to_thread(_exec)

    async def fetch(self, sql: str, *args):
        def _fetch():
            cursor = self.conn.execute(sql, args)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            # Convert each tuple to a dict using column names
            return [dict(zip(columns, row)) for row in rows]
        return await asyncio.to_thread(_fetch)

    async def fetchrow(self, sql: str, *args):
        def _fetchrow():
            cursor = self.conn.execute(sql, args)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            row = cursor.fetchone()
            if row is None:
                return None
            return dict(zip(columns, row))
        return await asyncio.to_thread(_fetchrow)

    async def fetchval(self, sql: str, *args):
        def _fetchval():
            cursor = self.conn.execute(sql, args)
            row = cursor.fetchone()
            return row[0] if row else None
        return await asyncio.to_thread(_fetchval)

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
        # Properly quote schema and table separately
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
        def _iter():
            cursor = self.conn.execute(sql, args)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    yield row

        for row in await asyncio.to_thread(lambda: list(_iter())):
            yield row
