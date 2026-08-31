from typing import Any, Dict, List

import pyarrow as pa


class DuckDBUploadImpl:
    """DuckDB-specific upload method bodies, extracted from the monolithic Uploader."""

    # ── Table creation: Typed ──────────────────────────────────
    async def _create_final_table_typed_duckdb(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    # ── Table creation: All TEXT (Legacy fallback) ─────────────────
    async def _create_final_table_all_text_duckdb(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {table_name} ({col_defs})')

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table_duckdb(self, table_name: str, arrow_table: pa.Table) -> None:
        conn = self._backend.pool.conn
        conn.register("arrow_temp", arrow_table)
        try:
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_temp")
        finally:
            conn.unregister("arrow_temp")

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample_duckdb(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = self._backend.pool.conn.execute(f"SELECT {col_str} FROM {table_name} LIMIT {limit}").fetch_arrow_table()
        return res

    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place_duckdb(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        schema_name, raw_table = self._split_qualified_table_name(final_table)
        tmp_table = f'{schema_name}."{raw_table}_tmp"'

        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {tmp_table} ({", ".join(col_defs)})')

        select_parts = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(f'TRY_CAST("{col}" AS {target_type}) AS "{col}"')
        await self.execute(f'INSERT INTO {tmp_table} SELECT {", ".join(select_parts)} FROM {final_table}')

        await self.drop_table(final_table)
        await self.execute(f'ALTER TABLE {tmp_table} RENAME TO {self._quote_identifier(raw_table)}')
