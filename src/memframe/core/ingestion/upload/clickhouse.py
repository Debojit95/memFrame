import csv
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

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

    async def _load_csv_into_staging(
        self,
        staging_table: str,
        file_path: str,
        columns: List[str],
        encoding: str,
    ) -> None:
        await self._load_csv_into_staging_clickhouse(staging_table, file_path, columns, encoding)

    async def _load_csv_into_staging_clickhouse(
        self, staging_table: str, file_path: str, columns: List[str], encoding: str
    ) -> None:
        def _read_batches():
            with open(file_path, "r", encoding=encoding, newline="") as f:
                reader = csv.reader(f)
                next(reader)
                batch = []
                for row in reader:
                    row = row[: len(columns)]
                    row += [""] * (len(columns) - len(row))
                    batch.append(row)
                    if len(batch) >= 10000:
                        yield batch
                        batch = []
                if batch:
                    yield batch

        for batch in list(_read_batches()):
            await self._backend.insert_rows(staging_table, batch, columns)

    async def _create_final_table_direct(
        self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {final_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    async def _create_final_table_clickhouse(
        self,
        final_table: str,
        staging_table: str,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")

        await self.execute(
            f"CREATE TABLE {final_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

        select_parts = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(self._build_safe_cast_clickhouse(col, pg_type))

        await self.execute(
            f"INSERT INTO {final_table} SELECT {', '.join(select_parts)} FROM {staging_table}"
        )

    async def _create_final_table_from_staging(
        self,
        final_table: str,
        staging_table: str,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        await self._create_final_table_clickhouse(final_table, staging_table, columns, schema)

    async def _create_text_staging_table(self, staging_table: str, columns: List[str]) -> None:
        col_defs = ", ".join(f"`{col}` String" for col in columns)
        await self.execute(f"CREATE TABLE {staging_table} ({col_defs})")

    async def _insert_arrow_table_postgres(self, final_table: str, arrow_table: pa.Table, columns: List[str]) -> None:
        full_table = arrow_table.rename_columns(columns)
        rows = []
        for batch in full_table.to_batches(max_chunksize=10000):
            for i in range(batch.num_rows):
                row = [batch.column(j)[i].as_py() for j in range(batch.num_columns)]
                rows.append(row)
                if len(rows) >= 10000:
                    await self._backend.insert_rows(final_table, rows, columns)
                    rows = []
        if rows:
            await self._backend.insert_rows(final_table, rows, columns)