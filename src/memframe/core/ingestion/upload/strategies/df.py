from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

import pyarrow as pa

from memframe.core.ingestion.datatype_detector import Backend
from memframe.core.ingestion.upload.strategies.base import UploadStrategy
from memframe.exceptions import ConfigurationError


if TYPE_CHECKING:
    import pandas as pd


logger = logging.getLogger("memFrame")


class DfUploadStrategy(UploadStrategy):
    """Upload pipeline for in-memory pandas DataFrames.

    Native Arrow inference first; falls back to an all-string table (fixed by
    the heuristic detector after insert) for mixed-type columns Arrow cannot
    convert. A post-insert sample re-inference catches string->typed patterns
    Arrow misses.
    """

    async def upload(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> str:
        await self._require_connection()
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("upload_df requires pandas. Please install pandas to use this method.") from exc
        if not isinstance(df, pd.DataFrame):
            raise ConfigurationError("upload_df expects a pandas DataFrame.")
        if len(df.columns) == 0:
            raise ConfigurationError("DataFrame must have at least one column.")

        data_id, table_name = await self._alloc_data_id()

        columns = self._uploader._make_unique_column_names([str(col) for col in df.columns])
        # Native Arrow inference first; fall back to an all-string table (whose
        # types are fixed by the heuristic detector after insert) if pandas
        # holds mixed-type columns Arrow cannot convert.
        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
            logger.warning("Native Arrow conversion failed; falling back to heuristic type detection")
            arrow_table = pa.Table.from_pandas(
                df.astype(str), preserve_index=False
            )
        arrow_table = arrow_table.rename_columns(columns)

        schema_name = self._uploader._backend.upload_schema
        if self._uploader._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self._uploader.create_schema_if_not_exists(schema_name)

        # Infer types directly from Arrow schema (no sampling needed)
        schema = {}
        for i, col in enumerate(columns):
            pa_field = arrow_table.schema.field(i)
            pg_type = self._uploader._arrow_type_to_postgres(pa_field.type)
            schema[col] = {"postgres_type": pg_type, "clickhouse_type": self._uploader._postgres_type_to_clickhouse(pg_type), "is_nullable": pa_field.nullable}

        # User override wins over inferred types
        schema = self._uploader._apply_dtype_override(schema, columns, dtypes)

        # Create table with proper types directly
        await self._uploader._create_final_table_typed(final_table, columns, schema)

        # Cast Arrow table to target schema and insert directly
        field_list = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            arrow_type = self._uploader._postgres_type_to_arrow(pg_type)
            field_list.append(pa.field(col, arrow_type))
        target_schema = pa.schema(field_list)
        typed_table = arrow_table.cast(target_schema)
        await self._uploader._insert_arrow_table(final_table, typed_table)

        # Sample and re-infer types to catch string→date/int/float patterns Arrow misses
        sample_table = await self._uploader._fetch_arrow_sample(final_table, columns, 50)
        schema_changed = False
        for col in columns:
            if dtypes and col in dtypes:
                continue
            chunked = sample_table.column(col)
            inferred = self._uploader._type_detector._infer_column(chunked)
            inferred_type = inferred.get("postgres_type", "TEXT")
            current_type = schema[col]["postgres_type"]
            if inferred_type != current_type and current_type == "TEXT":
                schema[col] = {
                    "postgres_type": inferred_type,
                    "clickhouse_type": self._uploader._postgres_type_to_clickhouse(inferred_type),
                    "is_nullable": True,
                }
                schema_changed = True
        if schema_changed:
            await self._uploader._cast_table_in_place(final_table, columns, schema)

        upload_name = filename or f"dataframe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        upload_name = Path(upload_name).name
        if not upload_name.lower().endswith(".csv"):
            upload_name = f"{upload_name}.csv"

        row_count = await self._uploader.fetchval(f"SELECT COUNT(*) FROM {final_table}")

        await self._register(data_id, upload_name, table_name, row_count)
        logger.info(f"Uploaded DataFrame -> {data_id} ({row_count} rows)")
        return data_id
