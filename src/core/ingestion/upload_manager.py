import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from src.core.ingestion.datatype_detector import Backend, _generate_6char_id
from src.db_manager.context import ContextManager


import csv
import logging
import os
import tempfile
import io
import asyncpg
import asyncio
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple,Union,TYPE_CHECKING






if TYPE_CHECKING:
    import pandas as pd


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

class Uploader:
    
    async def _aupload_csv_data_id(self, file_path: Union[str, Path]) -> str:
        """Upload a CSV file and return the generated data_id."""
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        while True:
            data_id = _generate_6char_id()
            table_name = self.get_upload_table_name(data_id)
            if not await self._backend.table_exists(table_name):
                break

        logger.info(f"Uploading {file_path.name} as {data_id}...")
        row_count = await self._create_table_from_csv(table_name, str(file_path))

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

    async def _aupload_parquet_data_id(self, file_path: Union[str, Path]) -> str:
        """Upload a Parquet file and return the generated data_id."""
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        while True:
            data_id = _generate_6char_id()
            table_name = self.get_upload_table_name(data_id)
            if not await self._backend.table_exists(table_name):
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

        upload_name = filename or f"dataframe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        upload_name = Path(upload_name).name
        if not upload_name.lower().endswith(".csv"):
            upload_name = f"{upload_name}.csv"

        fd, temp_csv = tempfile.mkstemp(prefix="memframe_upload_df_", suffix=".csv")
        os.close(fd)
        temp_csv_path = Path(temp_csv)

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: df.to_csv(temp_csv_path, index=False))

            data_id = await self._aupload_csv_data_id(temp_csv_path)

            await self._backend.execute(
                f"""
                UPDATE {self._backend.csv_registry_table}
                SET filename = {self._placeholder(1)}
                WHERE data_id = {self._placeholder(2)}
                """,
                upload_name,
                data_id,
            )
            logger.info(f"Uploaded DataFrame -> {data_id} ({len(df)} rows)")
            return data_id
        finally:
            temp_csv_path.unlink(missing_ok=True)

    def _memframe_from_data_id(self, data_id: str) -> ContextManager:
        return ContextManager(self, data_id=data_id)

    async def _aupload_csv(self, file_path: Union[str, Path]) -> ContextManager:
        data_id = await self._aupload_csv_data_id(file_path)
        return self._memframe_from_data_id(data_id)

    async def _aupload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
        data_id = await self._aupload_parquet_data_id(file_path)
        return self._memframe_from_data_id(data_id)

    async def _aupload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        data_id = await self._aupload_df_data_id(df, filename)
        return self._memframe_from_data_id(data_id)
   
   
    # helpers
    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

       
   
    #  CSV IMPORT (PyArrow‑based)  
    async def _create_table_from_csv(self, table_name: str, file_path: str) -> int:
        loop = asyncio.get_running_loop()

        # ---------- Robust encoding ----------
        encoding = await self._resolve_encoding(file_path)

        # ---------- Read columns & schema using that encoding ----------
        columns, schema = await loop.run_in_executor(
            None, self._get_columns_and_schema, file_path, encoding
        )

        schema_name = "upload"
        base_table = table_name
        await self.create_schema_if_not_exists(schema_name)
        staging_table = f'{schema_name}."{base_table}_staging"'
        final_table = f'{schema_name}."{base_table}"'

        # Load all data as TEXT
        await self._create_text_staging_table(staging_table, columns)
        await self._load_csv_into_staging(staging_table, file_path, columns, encoding)
        # Safe casting – individual column failures never stop the process
        await self._create_final_table_from_staging(final_table, staging_table, columns, schema)

        await self.drop_table(staging_table)
        row_count = await self.fetch_val(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

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

    def _get_columns_and_schema(self, file_path: str, encoding: str) -> Tuple[List[str], Dict]:
        """
        Returns:
            - columns: list of unique cleaned column names
            - schema: dict {col: {type, postgres_type, ...}}
        """
        parse_options = pcsv.ParseOptions(newlines_in_values=True)

        # Attempt to read with given encoding, fallback to latin-1 if it fails
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
            encoding = "latin-1"
            read_options = pcsv.ReadOptions(encoding=encoding, use_threads=True)
            header_reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
            )
            original_names = header_reader.schema.names
            header_reader.close()

        columns = self._make_unique_column_names(original_names)

        # Read a sample with all columns forced to string
        column_types = {name: pa.string() for name in original_names}
        convert_options = pcsv.ConvertOptions(
            column_types=column_types,
            auto_dict_encode=False,
        )

        try:
            reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )
        except Exception:
            # If even latin-1 fails, try again with pure latin-1 and no fancy options
            encoding = "latin-1"
            read_options = pcsv.ReadOptions(encoding=encoding, use_threads=True)
            reader = pcsv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )

        all_batches = []
        rows_read = 0
        while rows_read < self._type_detector.sample_size:
            try:
                batch = reader.read_next_batch()
                if batch.num_rows == 0:
                    break
                all_batches.append(batch)
                rows_read += batch.num_rows
            except StopIteration:
                break

        if not all_batches:
            sample_table = pa.table({col: pa.array([], type=pa.string()) for col in columns})
        else:
            sample_table = pa.Table.from_batches(all_batches)
            if sample_table.num_rows > self._type_detector.sample_size:
                sample_table = sample_table.slice(0, self._type_detector.sample_size)

        sample_table = sample_table.rename_columns(columns)

        schema = {}
        for col in columns:
            chunked = sample_table.column(col)
            schema[col] = self._type_detector._infer_column(chunked)

        return columns, schema

    async def _create_text_staging_table(self, staging_table: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {staging_table} ({col_defs})')

    async def _load_csv_into_staging(
        self,
        staging_table: str,
        file_path: str,
        columns: List[str],
        encoding: str,
    ) -> None:
        """Load CSV into staging table using the most robust method available."""
        if self.backend == Backend.DUCKDB:
            loop = asyncio.get_running_loop()

            # Helper: attempt to read with PyArrow using a given encoding
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

            # Try the provided encoding, then latin-1, then fallback to Python CSV
            for enc in (encoding, "latin-1"):
                try:
                    arrow_table = await loop.run_in_executor(None, _read_pyarrow, enc)
                    renamed = arrow_table.rename_columns(columns)
                    self._conn.register("arrow_temp", renamed)
                    self._conn.execute(f"INSERT INTO {staging_table} SELECT * FROM arrow_temp")
                    self._conn.unregister("arrow_temp")
                    return
                except Exception as e:
                    logger.warning(f"PyArrow read with encoding {enc} failed: {e}")
            # Final fallback: pure Python CSV reader (ultra-safe)
            logger.info("Falling back to Python CSV reader for DuckDB loading")
            await self._fallback_load_duckdb_python_csv(staging_table, file_path, columns)

        else:  # PostgreSQL – server‑side COPY from file
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

    async def _fallback_load_duckdb_python_csv(
        self, staging_table: str, file_path: str, columns: List[str]
    ) -> None:
        """Insert rows using Python's csv module – handles any byte."""
        loop = asyncio.get_running_loop()

        def _insert():
            with open(file_path, "r", encoding="latin-1", newline="") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                values = []
                for row in reader:
                    # Ensure row matches expected column count
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

        await loop.run_in_executor(None, _insert)

    async def _fallback_load_with_padding(
        self, staging_table: str, file_path: str, columns: List[str],
        raw_table: str, schema_name: Optional[str]) -> None:
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

    # ------------------------------------------------------------------
    #  PARQUET SUPPORT 
    # ------------------------------------------------------------------
    async def _create_table_from_parquet(self, table_name: str, file_path: str) -> int:
        loop = asyncio.get_running_loop()

        arrow_table = await loop.run_in_executor(None, pq.read_table, file_path)

        original_names = arrow_table.schema.names
        columns = self._make_unique_column_names(original_names)

        # Sample for type inference
        if arrow_table.num_rows > self._type_detector.sample_size:
            indices = np.random.choice(arrow_table.num_rows, self._type_detector.sample_size, replace=False)
            sample_table = arrow_table.take(pa.array(indices))
        else:
            sample_table = arrow_table

        sample_table = sample_table.rename_columns(columns)

        schema = {}
        for col in columns:
            chunked = sample_table.column(col)
            schema[col] = self._type_detector._infer_column(chunked)

        schema_name = "upload"
        final_table = f'{schema_name}."{table_name}"'
        await self.create_schema_if_not_exists(schema_name)

        if self.backend == Backend.DUCKDB:
            await self._create_final_table_direct(final_table, columns, schema)
            full_table = arrow_table.rename_columns(columns)
            self._conn.register("parquet_temp", full_table)
            self._conn.execute(f"INSERT INTO {final_table} SELECT * FROM parquet_temp")
            self._conn.unregister("parquet_temp")
        else:
            await self._create_final_table_direct(final_table, columns, schema)
            await self._insert_arrow_table_postgres(final_table, arrow_table, columns)

        row_count = await self.fetch_val(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

    async def _create_final_table_direct(self, final_table: str, columns: List[str],
                                        schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {final_table} ({", ".join(col_defs)})')

    async def _insert_arrow_table_postgres(self, final_table: str, arrow_table: pa.Table,
                                           columns: List[str]) -> None:
        full_table = arrow_table.rename_columns(columns)

        text_buffer = io.StringIO()
        writer = csv.writer(text_buffer, quoting=csv.QUOTE_MINIMAL)
        for batch in full_table.to_batches(max_chunksize=10000):
            cols_data = [list(batch.column(j).to_pylist()) for j in range(batch.num_columns)]
            for i in range(batch.num_rows):
                row = [cols_data[j][i] for j in range(batch.num_columns)]
                writer.writerow(row)

        with io.BytesIO(text_buffer.getvalue().encode("utf-8")) as buf:
            await self._conn.copy_to_table(
                self._split_qualified_table_name(final_table)[1],
                source=buf,
                columns=columns,
                schema_name="upload",
                format="csv",
                header=False,
                encoding="UTF8",
            )

    # ------------------------------------------------------------------
    #  Safe column‑by‑column casting 
    # ------------------------------------------------------------------
    async def _create_final_table_from_staging(
        self,
        final_table: str,
        staging_table: str,
        columns: List[str],
        schema: Dict[str, Dict[str, Any]],
    ) -> None:
        if self.backend == Backend.DUCKDB:
            await self._create_final_table_duckdb(final_table, staging_table, columns, schema)
        else:
            await self._create_final_table_postgres(final_table, staging_table, columns, schema)

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
            # TRY_CAST returns NULL on failure – never errors
            select_parts.append(f'TRY_CAST("{col}" AS {target_type}) AS "{col}"')
        await self.execute(
            f'INSERT INTO {final_table} SELECT {", ".join(select_parts)} FROM {staging_table}'
        )

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

    def _build_safe_cast_postgres(self, col: str, target_type: str) -> str:
        base = target_type.split("(")[0].upper()
        col_quoted = f'"{col}"'
        # All cases use CASE … ELSE NULL → never fails
        if base in ("SMALLINT", "INTEGER", "BIGINT"):
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^-?[0-9]+$' THEN
                        TRIM({col_quoted})::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        elif base in ("NUMERIC", "DECIMAL", "REAL", "DOUBLE PRECISION"):
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
        elif base == "TIMESTAMP":
            return f"""
                CASE
                    WHEN TRIM({col_quoted}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}[ T][0-9]{{1,2}}:[0-9]{{1,2}}' THEN
                        TRIM({col_quoted})::TIMESTAMP
                    ELSE NULL
                END AS "{col}"
            """
        else:
            return f'{col_quoted} AS "{col}"'


    
    # # ====================== SYNC APIS ===================
    # async def aupload_csv(self, file_path: Union[str, Path]) -> ContextManager:
    #     return await self._aupload_csv(file_path)
    
    # @async_to_sync
    # async def upload_csv(self, file_path: Union[str, Path]) -> ContextManager:
    #     return await self.aupload_csv(file_path)

    
    # async def aupload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
    #     return await self._aupload_parquet(file_path)

    # @async_to_sync
    # async def upload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
    #     return await self.aupload_parquet(file_path)
    
    # async def aupload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
    #     return await self._aupload_df(df, filename)
        
    # @async_to_sync
    # async def upload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
    #     return await self.aupload_df(df, filename)






