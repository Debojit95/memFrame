import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import pyarrow as pa

from memframe.core.ingestion.upload.base import Uploader
from memframe.core.ingestion.datatype_detector import Backend

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger("memFrame")


class ClickHouseUploader(Uploader):
    """ClickHouse-specific upload implementation."""

    def __init__(self, backend):
        self._backend = backend
        self._type_detector = backend._type_detector
        self._conn = backend._conn

    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        await self.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")

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

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        return self._backend._split_qualified_table_name(table_name)

    def _clickhouse_qualified_table_name(
        self,
        table_name: str,
        default_database: Optional[str] = None,
    ) -> str:
        return self._backend._clickhouse_qualified_table_name(table_name, default_database)

    # ── Table creation: All TEXT ─────────────────────────────────
    async def _create_final_table_all_text_clickhouse(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f"`{col}` String" for col in columns)
        await self.execute(
            f"CREATE TABLE {table_name} ({col_defs}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table_clickhouse(self, table_name: str, arrow_table: pa.Table) -> None:
        for batch in arrow_table.to_batches(max_chunksize=100000):
            await self._backend.insert_arrow_table(table_name, pa.Table.from_batches([batch]))

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample_clickhouse(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = await self._conn.query(f"SELECT {col_str} FROM {table_name} LIMIT {limit}")
        data = {col: [row[i] for row in res.result_rows] for i, col in enumerate(res.column_names)}
        return pa.Table.from_pydict(data)
    
    
    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place_clickhouse(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        schema_name, raw_table = self._split_qualified_table_name(final_table)
        tmp_table = f"`{schema_name}`.`{raw_table}_tmp`"
        
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {tmp_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )
        
        select_parts = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(self._build_safe_cast_clickhouse(col, pg_type))
        await self.execute(f"INSERT INTO {tmp_table} SELECT {', '.join(select_parts)} FROM {final_table}")
        
        await self.drop_table(final_table)
        await self.execute(f"RENAME TABLE {tmp_table} TO {final_table}")