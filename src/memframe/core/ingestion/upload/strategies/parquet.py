from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from memframe.core.ingestion.datatype_detector import Backend
from memframe.core.ingestion.upload.strategies.base import UploadStrategy


logger = logging.getLogger("memFrame")


class ParquetUploadStrategy(UploadStrategy):
    """Upload pipeline for Parquet files.

    Types come directly from the Parquet schema (no sampling needed); the
    table is created typed and data is streamed with proper conversion.
    """

    async def upload(
        self,
        file_path: Union[str, Path],
        dtypes: Optional[Dict[str, str]] = None,
    ) -> str:
        await self._require_connection()
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data_id, table_name = await self._alloc_data_id()

        logger.info(f"Uploading {file_path.name} as {data_id}...")
        row_count = await self._create_table_from_parquet(table_name, str(file_path), dtypes)

        await self._register(data_id, file_path.name, table_name, row_count)
        logger.info(f"Uploaded {file_path.name} -> {data_id} ({row_count} rows)")
        return data_id

    async def _create_table_from_parquet(
        self, table_name: str, file_path: str, dtypes: Optional[Dict[str, str]] = None
    ) -> int:
        arrow_table = pq.read_table(file_path)
        original_names = arrow_table.schema.names
        columns = self._uploader._make_unique_column_names(original_names)

        # Phase 1: Infer types directly from Parquet schema (no sampling needed!)
        schema = self._infer_types_from_parquet(arrow_table, columns)

        # User override wins over the native Parquet schema
        schema = self._uploader._apply_dtype_override(schema, columns, dtypes)

        schema_name = self._uploader._backend.upload_schema
        if self._uploader._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self._uploader.create_schema_if_not_exists(schema_name)

        # Phase 2: Create table with proper types directly
        await self._uploader._create_final_table_typed(final_table, columns, schema)

        # Phase 3: Stream Parquet with proper type conversion
        await self._insert_parquet_typed(final_table, arrow_table, columns, schema)

        # No casting phase needed! Types are already correct.
        row_count = await self._uploader.fetchval(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

    def _infer_types_from_parquet(
        self, arrow_table: pa.Table, columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Infer types from Parquet schema (already has types!)."""
        schema = {}
        for i, col in enumerate(columns):
            pa_field = arrow_table.schema.field(i)
            pg_type = self._uploader._arrow_type_to_postgres(pa_field.type)
            schema[col] = {"postgres_type": pg_type, "clickhouse_type": self._uploader._postgres_type_to_clickhouse(pg_type), "is_nullable": pa_field.nullable}
        return schema

    async def _insert_parquet_typed(
        self,
        table_name: str,
        arrow_table: pa.Table,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        """Insert Parquet data with proper type conversion."""
        # Build target schema
        field_list = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            arrow_type = self._uploader._postgres_type_to_arrow(pg_type)
            field_list.append(pa.field(col, arrow_type))

        target_schema = pa.schema(field_list)

        # Rename and cast
        full_table = arrow_table.rename_columns(columns)
        full_table = full_table.cast(target_schema)

        await self._uploader._insert_arrow_table(table_name, full_table)
