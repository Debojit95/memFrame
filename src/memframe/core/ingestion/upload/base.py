import csv
import logging
import os
import tempfile
import io
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

import asyncio

from memframe.utils.async_sync import async_to_sync

if TYPE_CHECKING:
    import pandas as pd

from memframe.core.ingestion.datatype_detector import Backend, _generate_6char_id
from memframe.db_manager.context import ContextManager


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class Uploader:
    """Uploader mixin class - works when mixed into MemFrame via BaseWrapper.
    
    Accesses self._backend and self._type_detector from the MemFrame instance.

    Upload strategy (no staging table):
    1. Stream data into the target table with ALL columns as TEXT (via PyArrow)
    2. Pull a sample of 50 rows from the uploaded table
    3. Run existing dtype detection on the sample
    4. Cast the uploaded table to detected types (recreate + rename)
    """

    # ── Helper methods ─────────────────────────────────────────
    def _postgres_type_to_clickhouse(self, pg_type: str) -> str:
        base = pg_type.split("(")[0].upper()
        mapping = {
            "TEXT": "String",
            "VARCHAR": "String",
            "CHAR": "String",
            "INTEGER": "Int32",
            "INT": "Int32",
            "BIGINT": "Int64",
            "SMALLINT": "Int16",
            "NUMERIC": "Decimal(38, 10)",
            "DECIMAL": "Decimal(38, 10)",
            "REAL": "Float32",
            "FLOAT": "Float32",
            "FLOAT4": "Float32",
            "DOUBLE": "Float64",
            "FLOAT8": "Float64",
            "DOUBLE PRECISION": "Float64",
            "BOOLEAN": "UInt8",
            "BOOL": "UInt8",
            "DATE": "Date",
            "TIMESTAMP": "DateTime",
            "DATETIME": "DateTime",
        }
        return mapping.get(base, "String")

    def _build_safe_cast_clickhouse(self, col: str, pg_type: str) -> str:
        ch_type = self._postgres_type_to_clickhouse(pg_type)
        col_q = f"`{col}`"
        if ch_type == "String":
            return f"{col_q} AS `{col}`"
        if ch_type == "Int32":
            return f"toInt32OrNull({col_q}) AS `{col}`"
        if ch_type == "Int64":
            return f"toInt64OrNull({col_q}) AS `{col}`"
        if ch_type == "Int16":
            return f"toInt16OrNull({col_q}) AS `{col}`"
        if ch_type == "Float32":
            return f"toFloat32OrNull({col_q}) AS `{col}`"
        if ch_type == "Float64":
            return f"toFloat64OrNull({col_q}) AS `{col}`"
        if ch_type == "UInt8":
            return f"toUInt8OrNull({col_q}) AS `{col}`"
        if ch_type == "Date":
            return f"toDateOrNull({col_q}) AS `{col}`"
        if ch_type == "DateTime":
            return f"toDateTimeOrNull({col_q}) AS `{col}`"
        if "Decimal" in ch_type:
            return f"toDecimal64OrNull({col_q}, 10) AS `{col}`"
        return f"toString({col_q}) AS `{col}`"

    def _clean_column_name(self, name: str, index: int) -> str:
        if not name or name.strip() == "":
            name = f"column_{index}"
        cleaned = name.strip().strip('"`')
        cleaned = ''.join(c if c.isalnum() or c == '_' else '_' for c in cleaned)
        if cleaned and cleaned[0].isdigit():
            cleaned = '_' + cleaned
        if not cleaned:
            cleaned = f"column_{index}"
        return cleaned

    def _make_unique_column_names(self, original_names: List[str]) -> List[str]:
        cleaned = []
        for i, name in enumerate(original_names):
            cleaned.append(self._clean_column_name(name, i))

        final_names = []
        used = set()
        for name in cleaned:
            if name not in used:
                final_names.append(name)
                used.add(name)
            else:
                counter = 1
                while True:
                    candidate = f"{name}_{counter}"
                    if candidate not in used:
                        final_names.append(candidate)
                        used.add(candidate)
                        break
                    counter += 1
        return final_names

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

    def _memframe_from_data_id(self, data_id: str) -> ContextManager:
        return ContextManager(self, data_id=data_id)

    def _quote_identifier(self, col: str) -> str:
        if self._backend.backend == Backend.CLICKHOUSE:
            return f"`{col}`"
        return f'"{col}"'

    def _get_raw_table_name(self, table_name: str) -> str:
        if "." in table_name:
            raw = table_name.split(".")[-1]
        else:
            raw = table_name
        return raw.strip('"').strip('`')

    # ── Encoding detection ──────────────────────────────────────
    async def _resolve_encoding(self, file_path: str) -> str:
        detected = self._type_detector._detect_encoding(file_path)

        def _validate(enc):
            try:
                with open(file_path, "rb") as f:
                    raw = f.read(65536)
                raw.decode(enc)
                return True
            except (UnicodeDecodeError, LookupError):
                return False

        for enc in (detected, "utf-8", "latin-1", "cp1252"):
            if _validate(enc):
                return enc
        return "latin-1"

    # ── CSV column detection ────────────────────────────────────
    def _get_csv_columns(self, file_path: str, encoding: str) -> List[str]:
        parse_options = pcsv.ParseOptions(newlines_in_values=True)
        try:
            read_options = pcsv.ReadOptions(encoding=encoding, use_threads=True)
            header_reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
            )
            original_names = header_reader.schema.names
            header_reader.close()
        except Exception:
            logger.warning(f"Failed to read CSV header with encoding {encoding}, falling back to latin-1")
            read_options = pcsv.ReadOptions(encoding="latin-1", use_threads=True)
            header_reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
            )
            original_names = header_reader.schema.names
            header_reader.close()
        return self._make_unique_column_names(original_names)

    # ── CSV Type Inference (Phase 1: Type-First Upload) ──────────
    def _infer_csv_types(
        self,
        file_path: str,
        encoding: str,
        columns: List[str],
        sample_rows: int = 1000
    ) -> Dict[str, Dict[str, Any]]:
        """
        Read a sample of CSV rows using PyArrow and infer column types.
        Returns schema dict compatible with existing type detector.
        """
        parse_options = pcsv.ParseOptions(newlines_in_values=True)
        read_options = pcsv.ReadOptions(
            encoding=encoding, 
            use_threads=True,
            block_size=1024 * 1024,  # 1MB blocks
        )
        
        # First read just headers to get original names
        try:
            header_reader = pcsv.open_csv(file_path, read_options=read_options, parse_options=parse_options)
            original_names = header_reader.schema.names
            header_reader.close()
        except Exception:
            read_options = pcsv.ReadOptions(encoding="latin-1", use_threads=True, block_size=1024 * 1024)
            header_reader = pcsv.open_csv(file_path, read_options=read_options, parse_options=parse_options)
            original_names = header_reader.schema.names
            header_reader.close()

        # Read sample rows with all columns as string initially
        convert_opts = pcsv.ConvertOptions(
            column_types={name: pa.string() for name in original_names},
            auto_dict_encode=False,
        )
        
        try:
            # Use PyArrow's streaming reader to get sample rows efficiently
            reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_opts,
            )
            
            all_batches = []
            rows_read = 0
            while rows_read < sample_rows:
                try:
                    batch = reader.read_next_batch()
                    if batch.num_rows == 0:
                        break
                    all_batches.append(batch)
                    rows_read += batch.num_rows
                except StopIteration:
                    break
            
            if not all_batches:
                # Empty file - return all TEXT
                return {col: {"postgres_type": "TEXT", "clickhouse_type": "String", "is_nullable": True} for col in columns}
            
            sample_table = pa.Table.from_batches(all_batches)
            if sample_table.num_rows > sample_rows:
                sample_table = sample_table.slice(0, sample_rows)
            
            # Rename to our cleaned column names
            sample_table = sample_table.rename_columns(columns)
            
            # Use existing type detector on the sample
            schema = {}
            for col in columns:
                chunked = sample_table.column(col)
                schema[col] = self._type_detector._infer_column(chunked)
            
            return schema
            
        except Exception as e:
            logger.warning(f"Type inference failed: {e}. Falling back to TEXT for all columns.")
            return {col: {"postgres_type": "TEXT", "clickhouse_type": "String", "is_nullable": True} for col in columns}

    # ── Table creation: Typed (Phase 1) ────────────────────────
    async def _create_final_table_typed(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        """Create table with proper types directly (no TEXT intermediary)."""
        if self._backend.backend == Backend.DUCKDB:
            await self._create_final_table_typed_duckdb(table_name, columns, schema)
        elif self._backend.backend == Backend.POSTGRES:
            await self._create_final_table_typed_postgres(table_name, columns, schema)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._create_final_table_typed_clickhouse(table_name, columns, schema)

    async def _create_final_table_typed_duckdb(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    async def _create_final_table_typed_postgres(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    async def _create_final_table_typed_clickhouse(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {table_name} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    # ── Table creation: All TEXT (Legacy fallback) ─────────────────
    async def _create_final_table_all_text(self, table_name: str, columns: List[str]) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._create_final_table_all_text_duckdb(table_name, columns)
        elif self._backend.backend == Backend.POSTGRES:
            await self._create_final_table_all_text_postgres(table_name, columns)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._create_final_table_all_text_clickhouse(table_name, columns)

    async def _create_final_table_all_text_duckdb(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {table_name} ({col_defs})')

    async def _create_final_table_all_text_postgres(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {table_name} ({col_defs})')

    async def _create_final_table_all_text_clickhouse(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f"`{col}` String" for col in columns)
        await self.execute(
            f"CREATE TABLE {table_name} ({col_defs}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table(self, table_name: str, arrow_table: pa.Table) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._insert_arrow_table_duckdb(table_name, arrow_table)
        elif self._backend.backend == Backend.POSTGRES:
            await self._insert_arrow_table_postgres(table_name, arrow_table)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._insert_arrow_table_clickhouse(table_name, arrow_table)

    async def _insert_arrow_table_duckdb(self, table_name: str, arrow_table: pa.Table) -> None:
        self._backend._conn.register("arrow_temp", arrow_table)
        self._backend._conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_temp")
        self._backend._conn.unregister("arrow_temp")

    async def _insert_arrow_table_postgres(self, table_name: str, arrow_table: pa.Table) -> None:
        await self._insert_arrow_table_postgres_impl(table_name, arrow_table, arrow_table.schema.names)

    async def _insert_arrow_table_postgres_impl(self, final_table: str, arrow_table: pa.Table, columns: List[str]) -> None:
        full_table = arrow_table.rename_columns(columns)
        with io.BytesIO() as buf:
            text_writer = io.TextIOWrapper(buf, encoding="utf-8", write_through=True, newline="")
            writer = csv.writer(text_writer, quoting=csv.QUOTE_MINIMAL)
            for batch in full_table.to_batches(max_chunksize=10000):
                cols_data = [list(batch.column(j).to_pylist()) for j in range(batch.num_columns)]
                for i in range(batch.num_rows):
                    row = [cols_data[j][i] for j in range(batch.num_columns)]
                    writer.writerow(row)
            text_writer.flush()
            text_writer.detach()
            buf.seek(0)
            schema_name, raw_table = self._split_qualified_table_name(final_table)
            await self._backend._conn.copy_to_table(
                raw_table,
                source=buf,
                columns=columns,
                schema_name=schema_name,
                format="csv",
                header=False,
                encoding="UTF8",
            )

    async def _insert_arrow_table_clickhouse(self, table_name: str, arrow_table: pa.Table) -> None:
        # Chunk large tables to avoid ClickHouse memory limits
        for batch in arrow_table.to_batches(max_chunksize=100000):
            await self._backend.insert_arrow_table(table_name, pa.Table.from_batches([batch]))

    # ── CSV Streaming ───────────────────────────────────────────
    async def _stream_csv_all_text(self, table_name: str, file_path: str, columns: List[str], encoding: str) -> None:
        read_opts = pcsv.ReadOptions(encoding=encoding, use_threads=True)
        parse_opts = pcsv.ParseOptions(newlines_in_values=True)
        
        try:
            header_reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts)
            original_names = header_reader.schema.names
            header_reader.close()
        except Exception:
            read_opts = pcsv.ReadOptions(encoding="latin-1", use_threads=True)
            header_reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts)
            original_names = header_reader.schema.names
            header_reader.close()
            
        convert_opts = pcsv.ConvertOptions(column_types={name: pa.string() for name in original_names}, auto_dict_encode=False)
        
        try:
            # Fastest approach: Bulk read the whole CSV via PyArrow C++ engine
            arrow_table = pcsv.read_csv(
                file_path,
                read_options=read_opts,
                parse_options=parse_opts,
                convert_options=convert_opts
            )
            arrow_table = arrow_table.rename_columns(columns)
            await self._insert_arrow_table(table_name, arrow_table)
        except Exception as e:
            logger.warning(f"PyArrow bulk read failed: {e}. Falling back to chunked reader.")
            reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)
            batches = []
            rows_accumulated = 0
            while True:
                try:
                    batch = reader.read_next_batch()
                    if batch.num_rows == 0:
                        continue
                    batches.append(batch.rename_columns(columns))
                    rows_accumulated += batch.num_rows
                    if rows_accumulated >= 100000:
                        table = pa.Table.from_batches(batches)
                        await self._insert_arrow_table(table_name, table)
                        batches = []
                        rows_accumulated = 0
                except StopIteration:
                    break
            if batches:
                table = pa.Table.from_batches(batches)
                await self._insert_arrow_table(table_name, table)

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample(self, table_name: str, columns: List[str], limit: int = 50) -> pa.Table:
        if self._backend.backend == Backend.DUCKDB:
            return await self._fetch_arrow_sample_duckdb(table_name, columns, limit)
        elif self._backend.backend == Backend.POSTGRES:
            return await self._fetch_arrow_sample_postgres(table_name, columns, limit)
        elif self._backend.backend == Backend.CLICKHOUSE:
            return await self._fetch_arrow_sample_clickhouse(table_name, columns, limit)

    async def _fetch_arrow_sample_duckdb(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = self._backend._conn.execute(f"SELECT {col_str} FROM {table_name} LIMIT {limit}").fetch_arrow_table()
        return res

    async def _fetch_arrow_sample_postgres(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        rows = await self._backend._conn.fetch(f"SELECT {col_str} FROM {table_name} LIMIT {limit}")
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pa.Table.from_pydict(data)

    async def _fetch_arrow_sample_clickhouse(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = await self._backend._conn.query(f"SELECT {col_str} FROM {table_name} LIMIT {limit}")
        data = {col: [row[i] for row in res.result_rows] for i, col in enumerate(res.column_names)}
        return pa.Table.from_pydict(data)

    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._cast_table_in_place_duckdb(final_table, columns, schema)
        elif self._backend.backend == Backend.POSTGRES:
            await self._cast_table_in_place_postgres(final_table, columns, schema)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._cast_table_in_place_clickhouse(final_table, columns, schema)

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

    async def _cast_table_in_place_postgres(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
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
            select_parts.append(self._build_safe_cast_postgres(col, target_type))
        await self.execute(f'INSERT INTO {tmp_table} SELECT {", ".join(select_parts)} FROM {final_table}')
        
        await self.drop_table(final_table)
        await self.execute(f'ALTER TABLE {tmp_table} RENAME TO {self._quote_identifier(raw_table)}')

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

    # ── Safe casting for Postgres ──────────────────────────────
    def _build_safe_cast_postgres(self, col: str, target_type: str) -> str:
        base = target_type.split("(")[0].upper()
        col_quoted = f'"{col}"'
        if base in ("SMALLINT", "INTEGER", "BIGINT"):
            bounds = {
                "SMALLINT": (-32768, 32767),
                "INTEGER": (-2147483648, 2147483647),
                "BIGINT": (-9223372036854775808, 9223372036854775807)
            }
            min_val, max_val = bounds[base]
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^-?[0-9]+$' AND TRIM({col_quoted})::NUMERIC BETWEEN {min_val} AND {max_val} THEN
                        TRIM({col_quoted})::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        elif base in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE PRECISION"):
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^-?[0-9]*\\.?[0-9]+$' THEN
                        REPLACE(TRIM({col_quoted}), ',', '')::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        elif base == "BOOLEAN":
            return f"""
                CASE
                    WHEN UPPER(TRIM({col_quoted})) IN ('TRUE','T','YES','Y','1','ON') THEN TRUE
                    WHEN UPPER(TRIM({col_quoted})) IN ('FALSE','F','NO','N','0','OFF','') THEN FALSE
                    ELSE NULL
                END AS "{col}"
            """
        elif base == "DATE":
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}' THEN
                        TRIM({col_quoted})::DATE
                    ELSE NULL
                END AS "{col}"
            """
        elif base in ("TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}[ T][0-9]{{1,2}}:[0-9]{{1,2}}' THEN
                        TRIM({col_quoted})::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        else:
            return f'{col_quoted} AS "{col}"'
        
    # ── Schema creation ─────────────────────────────────────────
    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        if self._backend.backend in (Backend.DUCKDB, Backend.POSTGRES):
            await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")

    # ── Table operations delegation ─────────────────────────────
    async def execute(self, query: str, *args) -> None:
        return await self._backend.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[Tuple]:
        return await self._backend.fetch(query, *args)

    async def fetch_one(self, query: str, *args) -> Optional[Tuple]:
        return await self._backend.fetch_one(query, *args)

    async def fetch_val(self, query: str, *args) -> Any:
        return await self._backend.fetch_val(query, *args)

    def _placeholder(self, index: int) -> str:
        return self._backend.placeholder(index)

    async def drop_table(self, table_name: str) -> None:
        await self._backend.drop_table(table_name)

    async def table_exists(self, table_name: str) -> bool:
        return await self._backend.table_exists(table_name)

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        return self._backend._split_qualified_table_name(table_name)

    # ── CSV upload ──────────────────────────────────────────────
    async def _aupload_csv_data_id(
        self,
        file_path: Union[str, Path],
        registry_filename: Optional[str] = None,
    ) -> str:
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        registry_filename = registry_filename or file_path.name

        while True:
            data_id = _generate_6char_id()
            table_name = self.get_upload_table_name(data_id)
            if not await self.table_exists(table_name):
                break

        logger.info(f"Uploading {file_path.name} as {data_id}...")
        row_count = await self._create_table_from_csv(table_name, str(file_path))

        await self._backend.execute(
            f"""
            INSERT INTO {self._backend.csv_registry_table} (data_id, filename, table_name, row_count, is_upload_success)
            VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, {self._placeholder(4)}, {self._placeholder(5)})
            """,
            data_id,
            registry_filename,
            table_name,
            row_count,
            True,
        )
        logger.info(f"Uploaded {file_path.name} -> {data_id} ({row_count} rows)")
        return data_id

    async def _create_table_from_csv(self, table_name: str, file_path: str) -> int:
        encoding = await self._resolve_encoding(file_path)
        
        # Phase 1: Infer types from CSV sample (first ~1000 rows)
        columns, schema = await self._infer_types_from_csv(file_path, encoding)
        
        schema_name = self._backend.upload_schema
        if self._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self.create_schema_if_not_exists(schema_name)
        
        # Phase 2: Create table with PROPER types (not TEXT)
        await self._create_final_table_typed(final_table, columns, schema)
        
        # Phase 3: Stream CSV with proper type conversion directly into typed table
        await self._stream_csv_typed(final_table, file_path, columns, encoding, schema)
        
        # No casting phase needed! Types are already correct.
        row_count = await self.fetch_val(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

    # ── Type-First CSV Upload (New Optimized Path) ────────────────
    async def _infer_types_from_csv(
        self, file_path: str, encoding: str, sample_rows: int = 5000
    ) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
        """Read first N rows of CSV with PyArrow to infer column types."""
        read_opts = pcsv.ReadOptions(encoding=encoding, use_threads=True, block_size=1 << 20)
        parse_opts = pcsv.ParseOptions(newlines_in_values=True)
        
        # First, get column names from header
        header_reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts)
        original_names = header_reader.schema.names
        header_reader.close()
        
        columns = self._make_unique_column_names(original_names)
        
        # Now read sample with all columns as string to analyze
        convert_opts = pcsv.ConvertOptions(
            column_types={name: pa.string() for name in original_names},
            auto_dict_encode=False,
            include_columns=original_names,
            strings_can_be_null=True,
        )
        
        # Use streaming reader to get first N rows
        reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)
        
        all_batches = []
        rows_read = 0
        while rows_read < sample_rows:
            try:
                batch = reader.read_next_batch()
                if batch.num_rows == 0:
                    break
                all_batches.append(batch)
                rows_read += batch.num_rows
            except StopIteration:
                break
        
        if not all_batches:
            # Empty file - return all TEXT
            return columns, {col: {"postgres_type": "TEXT", "clickhouse_type": "String", "is_nullable": True} for col in columns}
        
        sample_table = pa.Table.from_batches(all_batches)
        if sample_table.num_rows > sample_rows:
            sample_table = sample_table.slice(0, sample_rows)
        
        # Rename to clean column names
        sample_table = sample_table.rename_columns(columns)
        
        # Infer types using existing detector
        schema = {}
        for col in columns:
            chunked = sample_table.column(col)
            schema[col] = self._type_detector._infer_column(chunked)
        
        # Make types more conservative to handle outliers in full dataset
        schema = self._make_types_conservative(schema)
        
        return columns, schema

    def _make_types_conservative(self, schema: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Upgrade integer types to next level to handle potential outliers."""
        upgrade_map = {
            "SMALLINT": "INTEGER",
            "INTEGER": "BIGINT",
            "INT2": "INTEGER",
            "INT4": "BIGINT",
        }
        for col, info in schema.items():
            pg_type = info.get("postgres_type", "TEXT")
            if pg_type in upgrade_map:
                info["postgres_type"] = upgrade_map[pg_type]
                # Also update clickhouse type
                info["clickhouse_type"] = self._postgres_type_to_clickhouse(upgrade_map[pg_type])
        return schema

    async def _create_final_table_typed(
        self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        """Create table with proper column types (not all TEXT)."""
        if self._backend.backend == Backend.DUCKDB:
            await self._create_final_table_typed_duckdb(table_name, columns, schema)
        elif self._backend.backend == Backend.POSTGRES:
            await self._create_final_table_typed_postgres(table_name, columns, schema)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._create_final_table_typed_clickhouse(table_name, columns, schema)

    async def _create_final_table_typed_duckdb(
        self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    async def _create_final_table_typed_postgres(
        self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    async def _create_final_table_typed_clickhouse(
        self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]
    ) -> None:
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {table_name} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    async def _stream_csv_typed(
        self,
        table_name: str,
        file_path: str,
        columns: List[str],
        encoding: str,
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        """Stream CSV directly into typed table - dispatches to backend-specific implementation."""
        if self._backend.backend == Backend.CLICKHOUSE:
            await self._stream_csv_typed_clickhouse(table_name, file_path, columns, encoding, schema)
        else:
            await self._stream_csv_typed_pyarrow(table_name, file_path, columns, encoding, schema)

    async def _stream_csv_typed_clickhouse(
        self,
        table_name: str,
        file_path: str,
        columns: List[str],
        encoding: str,
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        """
        Upload CSV to ClickHouse using native CSVWithNames format.
        Single HTTP request, native C++ parser - fastest possible.
        """
        database, table = self._split_qualified_table_name(table_name)
        database = database or self._backend.upload_schema
        
        columns_str = ", ".join(f"`{col}`" for col in columns)
        
        # Read file as binary and stream directly
        with open(file_path, "rb") as f:
            csv_data = f.read()
        
        # Use ClickHouse native CSV format with headers
        await self._backend._conn._post(
            f"INSERT INTO `{database}`.`{table}` ({columns_str}) FORMAT CSVWithNames",
            data=csv_data,
        )

    async def _stream_csv_typed_pyarrow(
        self,
        table_name: str,
        file_path: str,
        columns: List[str],
        encoding: str,
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        """Default implementation using PyArrow CSV reader + Arrow insert (DuckDB/PostgreSQL)."""
        # Build PyArrow type mapping from schema
        field_list = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            arrow_type = self._postgres_type_to_arrow(pg_type)
            field_list.append(pa.field(col, arrow_type))
        
        target_schema = pa.schema(field_list)
        
        # Get original column names from CSV header for include_columns
        read_opts = pcsv.ReadOptions(encoding=encoding, use_threads=True, block_size=1 << 20)
        parse_opts = pcsv.ParseOptions(newlines_in_values=True)
        header_reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts)
        original_names = header_reader.schema.names
        header_reader.close()
        
        # Map cleaned column names to original names for conversion
        orig_to_clean = dict(zip(original_names, columns))
        clean_to_orig = {v: k for k, v in orig_to_clean.items()}
        
        # Track columns that need fallback to wider types
        col_types = {col: target_schema.field(col).type for col in columns}
        col_fallback_count = {col: 0 for col in columns}
        
        def get_convert_opts():
            return pcsv.ConvertOptions(
                column_types={clean_to_orig[col]: col_types[col] for col in columns},
                auto_dict_encode=False,
                include_columns=original_names,
                strings_can_be_null=True,
            )
        
        max_fallbacks = 3  # TEXT -> larger numeric -> larger numeric -> TEXT
        
        while True:
            try:
                convert_opts = get_convert_opts()
                reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)
                
                batches = []
                rows_accumulated = 0
                CHUNK_SIZE = 50000
                
                while True:
                    try:
                        batch = reader.read_next_batch()
                        if batch.num_rows == 0:
                            continue
                        # Rename to target column names
                        batch = batch.rename_columns(columns)
                        # Cast to target schema
                        batch = batch.cast(target_schema)
                        batches.append(batch)
                        rows_accumulated += batch.num_rows
                        
                        if rows_accumulated >= CHUNK_SIZE:
                            table = pa.Table.from_batches(batches)
                            await self._insert_arrow_table(table_name, table)
                            batches = []
                            rows_accumulated = 0
                    except StopIteration:
                        break
                
                if batches:
                    table = pa.Table.from_batches(batches)
                    await self._insert_arrow_table(table_name, table)
                break  # Success!
                
            except pa.ArrowInvalid as e:
                error_msg = str(e)
                if "CSV conversion error" in error_msg and any(fallback < max_fallbacks for fallback in col_fallback_count.values()):
                    # Find which column failed and upgrade its type
                    for col in columns:
                        if col in error_msg:
                            col_fallback_count[col] += 1
                            current_type = col_types[col]
                            # Upgrade to wider type
                            if pa.types.is_int16(current_type):
                                col_types[col] = pa.int32()
                            elif pa.types.is_int32(current_type):
                                col_types[col] = pa.int64()
                            elif pa.types.is_int64(current_type):
                                col_types[col] = pa.string()
                            elif pa.types.is_float32(current_type):
                                col_types[col] = pa.float64()
                            elif pa.types.is_date32(current_type):
                                col_types[col] = pa.timestamp("us")
                            else:
                                col_types[col] = pa.string()
                            
                            logger.warning(f"Column '{col}' type conversion failed, upgrading to {col_types[col]}")
                            break
                    continue
                else:
                    # Too many fallbacks or unknown error - re-raise
                    raise

    def _postgres_type_to_arrow(self, pg_type: str) -> pa.DataType:
        """Convert PostgreSQL type string to PyArrow type."""
        base = pg_type.split("(")[0].upper()
        mapping = {
            "TEXT": pa.string(),
            "VARCHAR": pa.string(),
            "CHAR": pa.string(),
            "INTEGER": pa.int32(),
            "INT": pa.int32(),
            "BIGINT": pa.int64(),
            "SMALLINT": pa.int16(),
            "NUMERIC": pa.decimal128(38, 10),
            "DECIMAL": pa.decimal128(38, 10),
            "REAL": pa.float32(),
            "FLOAT": pa.float32(),
            "FLOAT4": pa.float32(),
            "DOUBLE": pa.float64(),
            "FLOAT8": pa.float64(),
            "DOUBLE PRECISION": pa.float64(),
            "BOOLEAN": pa.bool_(),
            "BOOL": pa.bool_(),
            "DATE": pa.date32(),
            "TIMESTAMP": pa.timestamp("us"),
            "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
            "TIMESTAMP WITH TIME ZONE": pa.timestamp("us", tz="UTC"),
        }
        return mapping.get(base, pa.string())

    async def _aupload_csv(self, file_path: Union[str, Path]) -> ContextManager:
        data_id = await self._aupload_csv_data_id(file_path)
        return self._memframe_from_data_id(data_id)

    # ── Parquet upload ──────────────────────────────────────────
    async def _aupload_parquet_data_id(self, file_path: Union[str, Path]) -> str:
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        while True:
            data_id = _generate_6char_id()
            table_name = self.get_upload_table_name(data_id)
            if not await self.table_exists(table_name):
                break

        logger.info(f"Uploading {file_path.name} as {data_id}...")
        row_count = await self._create_table_from_parquet(table_name, str(file_path))

        await self._backend.execute(
            f"""
            INSERT INTO {self._backend.csv_registry_table} (data_id, filename, table_name, row_count, is_upload_success)
            VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, {self._placeholder(4)}, {self._placeholder(5)})
            """,
            data_id,
            file_path.name,
            table_name,
            row_count,
            True,
        )
        logger.info(f"Uploaded {file_path.name} -> {data_id} ({row_count} rows)")
        return data_id

    async def _create_table_from_parquet(self, table_name: str, file_path: str) -> int:
        arrow_table = pq.read_table(file_path)
        original_names = arrow_table.schema.names
        columns = self._make_unique_column_names(original_names)
        
        # Phase 1: Infer types directly from Parquet schema (no sampling needed!)
        schema = self._infer_types_from_parquet(arrow_table, columns)
        
        schema_name = self._backend.upload_schema
        if self._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self.create_schema_if_not_exists(schema_name)
        
        # Phase 2: Create table with proper types directly
        await self._create_final_table_typed(final_table, columns, schema)
        
        # Phase 3: Stream Parquet with proper type conversion
        await self._insert_parquet_typed(final_table, arrow_table, columns, schema)

        # No casting phase needed! Types are already correct.
        row_count = await self.fetch_val(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

    def _infer_types_from_parquet(
        self, arrow_table: pa.Table, columns: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Infer types from Parquet schema (already has types!)."""
        schema = {}
        for i, col in enumerate(columns):
            pa_field = arrow_table.schema.field(i)
            pg_type = self._arrow_type_to_postgres(pa_field.type)
            schema[col] = {"postgres_type": pg_type, "clickhouse_type": self._postgres_type_to_clickhouse(pg_type), "is_nullable": pa_field.nullable}
        return schema

    def _arrow_type_to_postgres(self, arrow_type: pa.DataType) -> str:
        """Convert PyArrow type to PostgreSQL type string."""
        if pa.types.is_int8(arrow_type):
            return "SMALLINT"
        elif pa.types.is_int16(arrow_type):
            return "SMALLINT"
        elif pa.types.is_int32(arrow_type):
            return "INTEGER"
        elif pa.types.is_int64(arrow_type):
            return "BIGINT"
        elif pa.types.is_uint8(arrow_type):
            return "SMALLINT"
        elif pa.types.is_uint16(arrow_type):
            return "INTEGER"
        elif pa.types.is_uint32(arrow_type):
            return "BIGINT"
        elif pa.types.is_uint64(arrow_type):
            return "BIGINT"
        elif pa.types.is_float32(arrow_type):
            return "REAL"
        elif pa.types.is_float64(arrow_type):
            return "DOUBLE PRECISION"
        elif pa.types.is_decimal(arrow_type):
            return f"NUMERIC({arrow_type.precision}, {arrow_type.scale})"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_date32(arrow_type):
            return "DATE"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMPTZ" if arrow_type.tz else "TIMESTAMP"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "TEXT"
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "BYTEA"
        else:
            return "TEXT"

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
            arrow_type = self._postgres_type_to_arrow(pg_type)
            field_list.append(pa.field(col, arrow_type))
        
        target_schema = pa.schema(field_list)
        
        # Rename and cast
        full_table = arrow_table.rename_columns(columns)
        full_table = full_table.cast(target_schema)
        
        await self._insert_arrow_table(table_name, full_table)

    async def _aupload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
        data_id = await self._aupload_parquet_data_id(file_path)
        return self._memframe_from_data_id(data_id)

    # ── DataFrame upload ────────────────────────────────────────
    async def _aupload_df_data_id(self, df: "pd.DataFrame", filename: Optional[str] = None) -> str:
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("upload_df requires pandas. Please install pandas to use this method.") from exc
        if not isinstance(df, pd.DataFrame):
            raise TypeError("upload_df expects a pandas DataFrame.")
        if len(df.columns) == 0:
            raise ValueError("DataFrame must have at least one column.")

        while True:
            data_id = _generate_6char_id()
            table_name = self.get_upload_table_name(data_id)
            if not await self.table_exists(table_name):
                break

        columns = self._make_unique_column_names([str(col) for col in df.columns])
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        arrow_table = arrow_table.rename_columns(columns)

        schema_name = self._backend.upload_schema
        if self._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self.create_schema_if_not_exists(schema_name)
        await self._create_final_table_all_text(final_table, columns)

        # Cast all columns to string
        str_arrays = []
        for col in arrow_table.columns:
            if not pa.types.is_string(col.type):
                try:
                    str_arrays.append(col.cast(pa.string()))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    str_arrays.append(pa.array([str(x) if x is not None else None for x in col.to_pylist()], type=pa.string()))
            else:
                str_arrays.append(col)
        str_table = pa.Table.from_arrays(str_arrays, names=columns)

        await self._insert_arrow_table(final_table, str_table)

        sample_table = await self._fetch_arrow_sample(final_table, columns, 50)
        schema = {}
        for col in columns:
            chunked = sample_table.column(col)
            schema[col] = self._type_detector._infer_column(chunked)

        await self._cast_table_in_place(final_table, columns, schema)

        upload_name = filename or f"dataframe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        upload_name = Path(upload_name).name
        if not upload_name.lower().endswith(".csv"):
            upload_name = f"{upload_name}.csv"

        row_count = await self.fetch_val(f"SELECT COUNT(*) FROM {final_table}")

        await self._backend.execute(
            f"""
            INSERT INTO {self._backend.csv_registry_table} (data_id, filename, table_name, row_count, is_upload_success)
            VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, {self._placeholder(4)}, {self._placeholder(5)})
            """,
            data_id,
            upload_name,
            table_name,
            row_count,
            True,
        )
        logger.info(f"Uploaded DataFrame -> {data_id} ({row_count} rows)")
        return data_id

    async def _aupload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        data_id = await self._aupload_df_data_id(df, filename)
        return self._memframe_from_data_id(data_id)