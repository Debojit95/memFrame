import csv
import io
import logging
import asyncpg
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from memframe.core.ingestion.upload.base import Uploader
from memframe.core.ingestion.datatype_detector import Backend

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger("memFrame")


class PostgresUploader(Uploader):
    """PostgreSQL-specific upload implementation."""

    def __init__(self, backend):
        self._backend = backend
        self._type_detector = backend._type_detector
        self._conn = backend._conn

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

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        return self._backend._split_qualified_table_name(table_name)

    async def _load_csv_into_staging(
        self,
        staging_table: str,
        file_path: str,
        columns: List[str],
        encoding: str,
    ) -> None:
        schema_name, raw_table = self._split_qualified_table_name(staging_table)

        original_encoding = await self.fetch_val("SHOW client_encoding")
        await self.execute("SET client_encoding = 'LATIN1'")

        try:
            await self._conn.copy_to_table(
                raw_table,
                source=file_path,
                columns=columns,
                schema_name=schema_name,
                format="csv",
                header=True,
                encoding="LATIN1",
            )
        except asyncpg.exceptions.BadCopyFileFormatError as e:
            logger.warning(f"COPY failed, falling back to row padding: {e}")
            await self._fallback_load_with_padding(
                staging_table, file_path, columns, raw_table, schema_name
            )
        finally:
            await self.execute(f"SET client_encoding = '{original_encoding}'")

    async def _fallback_load_with_padding(
        self, staging_table: str, file_path: str, columns: List[str],
        raw_table: str, schema_name: Optional[str]
    ) -> None:
        expected_cols = len(columns)
        buffer = io.BytesIO()
        text_wrapper = io.TextIOWrapper(buffer, encoding="latin-1", write_through=True)
        writer = csv.writer(text_wrapper, quoting=csv.QUOTE_MINIMAL)

        with open(file_path, "r", encoding="latin-1", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) > expected_cols:
                    row = row[:expected_cols]
                elif len(row) < expected_cols:
                    row = row + [''] * (expected_cols - len(row))
                writer.writerow(row)

        text_wrapper.flush()
        buffer.seek(0)

        await self._conn.copy_to_table(
            raw_table,
            source=buffer,
            columns=columns,
            schema_name=schema_name,
            format="csv",
            header=False,
            encoding="LATIN1",
        )

    async def _create_final_table_direct(
        self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {final_table} ({", ".join(col_defs)})')

    async def _create_final_table_postgres(
        self,
        final_table: str,
        staging_table: str,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {final_table} ({", ".join(col_defs)})')

        select_parts = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(self._build_safe_cast_postgres(col, target_type))
        await self.execute(
            f'INSERT INTO {final_table} SELECT {", ".join(select_parts)} FROM {staging_table}'
        )

    async def _create_final_table_from_staging(
        self,
        final_table: str,
        staging_table: str,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        await self._create_final_table_postgres(final_table, staging_table, columns, schema)

    async def _create_text_staging_table(self, staging_table: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f"CREATE TABLE {staging_table} ({col_defs})")