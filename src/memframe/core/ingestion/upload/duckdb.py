import logging
from typing import Any, Dict, List, TYPE_CHECKING

import pyarrow as pa

from memframe.core.ingestion.upload.base import Uploader

if TYPE_CHECKING:
    pass


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

class DuckDBUploader(Uploader):
    """DuckDB-specific upload implementation."""

    def __init__(self, backend):
        self._backend = backend
        self._type_detector = backend._type_detector
        self._conn = backend.pool.conn

    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    @property
    def backend(self):
        return self._backend

    @property
    def _placeholder(self):
        return self._backend.placeholder

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

    def _memframe_from_data_id(self, data_id: str):
        from memframe.db_manager.context import ContextManager
        return ContextManager(self, data_id=data_id)

    async def _resolve_encoding(self, file_path: str) -> str:
        return await self._backend._resolve_encoding(file_path)

    # ── Table creation: All TEXT ─────────────────────────────────
    async def _create_final_table_all_text_duckdb(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {table_name} ({col_defs})')

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table_duckdb(self, table_name: str, arrow_table: pa.Table) -> None:
        self._conn.register("arrow_temp", arrow_table)
        self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_temp")
        self._conn.unregister("arrow_temp")

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample_duckdb(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = self._conn.execute(f"SELECT {col_str} FROM {table_name} LIMIT {limit}").fetch_arrow_table()
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