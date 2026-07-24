import csv
import io
import os
import tempfile
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from memframe.core.ingestion.upload.base import Uploader
from memframe.core.ingestion.datatype_detector import Backend

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger("memFrame")


class DuckDBUploader(Uploader):
    """DuckDB-specific upload implementation."""

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

    async def _load_csv_into_staging(
        self,
        staging_table: str,
        file_path: str,
        columns: List[str],
        encoding: str,
    ) -> None:
        def _read_pyarrow(enc):
            read_opts = pcsv.ReadOptions(encoding=enc, use_threads=True)
            parse_opts = pcsv.ParseOptions(newlines_in_values=True)
            header_reader = pcsv.open_csv(
                file_path,
                read_options=read_opts,
                parse_options=parse_opts,
            )
            orig_names = header_reader.schema.names
            header_reader.close()
            convert_opts = pcsv.ConvertOptions(
                column_types={name: pa.string() for name in orig_names},
                auto_dict_encode=False,
            )
            return pcsv.read_csv(
                file_path,
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts,
            )

        for enc in (encoding, "latin-1"):
            try:
                arrow_table = _read_pyarrow(enc)
                renamed = arrow_table.rename_columns(columns)
                self._conn.register("arrow_temp", renamed)
                self._conn.execute(f"INSERT INTO {staging_table} SELECT * FROM arrow_temp")
                self._conn.unregister("arrow_temp")
                return
            except Exception as e:
                logger.warning(f"PyArrow read with encoding {enc} failed: {e}")

        logger.info("Falling back to Python CSV reader for DuckDB loading")
        await self._fallback_load_duckdb_python_csv(staging_table, file_path, columns)

    async def _fallback_load_duckdb_python_csv(
        self, staging_table: str, file_path: str, columns: List[str]
    ) -> None:
        def _insert():
            with open(file_path, "r", encoding="latin-1", newline="") as f:
                reader = csv.reader(f)
                next(reader)
                values = []
                for row in reader:
                    row = row[: len(columns)]
                    row += [""] * (len(columns) - len(row))
                    values.append(row)
                    if len(values) >= 10000:
                        self._conn.executemany(
                            f"INSERT INTO {staging_table} VALUES ({', '.join(['?'] * len(columns))})",
                            values,
                        )
                        values.clear()
                if values:
                    self._conn.executemany(
                        f"INSERT INTO {staging_table} VALUES ({', '.join(['?'] * len(columns))})",
                        values,
                    )

        _insert()

    async def _create_final_table_direct(
        self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {final_table} ({", ".join(col_defs)})')

    async def _create_final_table_duckdb(
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
            select_parts.append(f'TRY_CAST("{col}" AS {target_type}) AS "{col}"')
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
        await self._create_final_table_duckdb(final_table, staging_table, columns, schema)

    async def _create_text_staging_table(self, staging_table: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f"CREATE TABLE {staging_table} ({col_defs})")