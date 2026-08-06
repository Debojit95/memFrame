from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.csv as pcsv

from memframe.core.ingestion.datatype_detector import Backend
from memframe.core.ingestion.upload.strategies.base import UploadStrategy
from memframe.exceptions import ConfigurationError


logger = logging.getLogger("memFrame")

_FALLBACK_SAMPLE_ROWS = 5000


class CsvUploadStrategy(UploadStrategy):
    """Upload pipeline for CSV files.

    ClickHouse: all-text insert then cast (native CSV parser can't handle
    typed conversion). DuckDB/PostgreSQL: type-first optimized path.
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
        row_count = await self._create_table_from_csv(table_name, str(file_path), dtypes)

        await self._register(data_id, file_path.name, table_name, row_count)
        logger.info(f"Uploaded {file_path.name} -> {data_id} ({row_count} rows)")
        return data_id

    async def _create_table_from_csv(
        self, table_name: str, file_path: str, dtypes: Optional[Dict[str, str]] = None
    ) -> int:
        encoding = await self._uploader._resolve_encoding(file_path)

        schema_name = self._uploader._backend.upload_schema
        if self._uploader._backend.backend == Backend.CLICKHOUSE:
            final_table = f"`{schema_name}`.`{table_name}`"
        else:
            final_table = f'{schema_name}."{table_name}"'

        await self._uploader.create_schema_if_not_exists(schema_name)

        if self._uploader._backend.backend == Backend.CLICKHOUSE:
            # ClickHouse: all-text insert then cast (native CSV parser can't handle typed conversion)
            columns = self._get_csv_columns(file_path, encoding)
            await self._uploader._create_final_table_all_text(final_table, columns)
            await self._stream_csv_all_text(final_table, file_path, columns, encoding)
            sample_table = await self._uploader._fetch_arrow_sample(final_table, columns, 50)
            schema = {}
            for col in columns:
                chunked = sample_table.column(col)
                schema[col] = self._uploader._type_detector._infer_column(chunked)
            schema = self._uploader._apply_dtype_override(schema, columns, dtypes)
            await self._uploader._cast_table_in_place(final_table, columns, schema)
        else:
            # DuckDB/PostgreSQL: type-first optimized path.
            try:
                columns, schema = await self._infer_types_from_csv(file_path, encoding)
                schema = self._uploader._apply_dtype_override(schema, columns, dtypes)
                await self._uploader._create_final_table_typed(final_table, columns, schema)
                await self._stream_csv_typed(
                    final_table, file_path, columns, encoding, schema,
                    locked_cols=set(dtypes or {}),
                )
            except Exception as exc:
                logger.warning(
                    f"Typed CSV upload failed ({type(exc).__name__}: {exc}); "
                    "retrying as all-text then casting."
                )
                await self._uploader.drop_table(final_table)
                columns = self._get_csv_columns(file_path, encoding)
                await self._uploader._create_final_table_all_text(final_table, columns)
                await self._stream_csv_all_text(final_table, file_path, columns, encoding)
                sample_table = await self._uploader._fetch_arrow_sample(
                    final_table, columns, _FALLBACK_SAMPLE_ROWS
                )
                schema = {}
                for col in columns:
                    schema[col] = self._uploader._type_detector._infer_column(
                        sample_table.column(col)
                    )
                schema = self._uploader._apply_dtype_override(schema, columns, dtypes)
                schema = self._uploader._make_types_conservative(schema)
                await self._uploader._cast_table_in_place(final_table, columns, schema)

        row_count = await self._uploader.fetchval(f"SELECT COUNT(*) FROM {final_table}")
        return row_count

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
        return self._uploader._make_unique_column_names(original_names)

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
                schema[col] = self._uploader._type_detector._infer_column(chunked)

            return schema

        except Exception as e:
            logger.warning(f"Type inference failed: {e}. Falling back to TEXT for all columns.")
            return {col: {"postgres_type": "TEXT", "clickhouse_type": "String", "is_nullable": True} for col in columns}

    async def _infer_types_from_csv(
        self, file_path: str, encoding: str, sample_rows: int = 5000
    ) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
        """Read first N rows of CSV.

        Native PyArrow inference is the initial choice; columns PyArrow
        leaves as strings fall back to sampling + heuristic detection.
        """
        read_opts = pcsv.ReadOptions(encoding=encoding, use_threads=True, block_size=1 << 20)
        parse_opts = pcsv.ParseOptions(newlines_in_values=True)

        # First, get column names from header
        header_reader = pcsv.open_csv(file_path, read_options=read_opts, parse_options=parse_opts)
        original_names = header_reader.schema.names
        header_reader.close()

        columns = self._uploader._make_unique_column_names(original_names)

        # Native inference (no forced string types) so PyArrow resolves
        # int/float/date columns itself.
        convert_opts = pcsv.ConvertOptions(
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

        schema = {}
        heuristic_cols = []
        for i, col in enumerate(columns):
            field = sample_table.schema.field(i)
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                # PyArrow gave up (mixed values, yes/no bools, ...); fall back to
                # sampling + heuristic detection.
                schema[col] = self._uploader._type_detector._infer_column(sample_table.column(col))
                heuristic_cols.append(col)
            else:
                pg_type = self._uploader._arrow_type_to_postgres(field.type)
                schema[col] = {
                    "postgres_type": pg_type,
                    "clickhouse_type": self._uploader._postgres_type_to_clickhouse(pg_type),
                    "is_nullable": field.nullable,
                }

        # Only heuristic columns need conservative widening (native int64 is widest already)
        if heuristic_cols:
            heuristic_schema = {col: schema[col] for col in heuristic_cols}
            heuristic_schema = self._uploader._make_types_conservative(heuristic_schema)
            schema.update(heuristic_schema)

        return columns, schema

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
            await self._uploader._insert_arrow_table(table_name, arrow_table)
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
                        await self._uploader._insert_arrow_table(table_name, table)
                        batches = []
                        rows_accumulated = 0
                except StopIteration:
                    break
            if batches:
                table = pa.Table.from_batches(batches)
                await self._uploader._insert_arrow_table(table_name, table)

    async def _stream_csv_typed(
        self,
        table_name: str,
        file_path: str,
        columns: List[str],
        encoding: str,
        schema: Dict[str, Dict[str, Any]],
        locked_cols: Optional[set] = None,
    ) -> None:
        """Stream CSV directly into typed table using PyArrow reader (DuckDB/PostgreSQL)."""
        await self._stream_csv_typed_pyarrow(
            table_name, file_path, columns, encoding, schema, locked_cols=locked_cols
        )

    async def _stream_csv_typed_pyarrow(
        self,
        table_name: str,
        file_path: str,
        columns: List[str],
        encoding: str,
        schema: Dict[str, Dict[str, Any]],
        locked_cols: Optional[set] = None,
    ) -> None:
        """Default implementation using PyArrow CSV reader + Arrow insert (DuckDB/PostgreSQL)."""
        # Build PyArrow type mapping from schema
        field_list = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            arrow_type = self._uploader._postgres_type_to_arrow(pg_type)
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
        locked_cols = locked_cols or set()

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
                # Cast schema must track widened col_types, else a column
                # upgraded to string is cast back to its original type and fails
                # (e.g. 'Failed to parse value: No' for a BOOLEAN target).
                cast_schema = pa.schema([pa.field(col, col_types[col]) for col in columns])
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
                        batch = batch.cast(cast_schema)
                        batches.append(batch)
                        rows_accumulated += batch.num_rows

                        if rows_accumulated >= CHUNK_SIZE:
                            table = pa.Table.from_batches(batches)
                            await self._uploader._insert_arrow_table(table_name, table)
                            batches = []
                            rows_accumulated = 0
                    except StopIteration:
                        break

                if batches:
                    table = pa.Table.from_batches(batches)
                    await self._uploader._insert_arrow_table(table_name, table)
                break  # Success!

            except pa.ArrowInvalid as e:
                error_msg = str(e)
                if "CSV conversion error" not in error_msg:
                    raise
                # Identify the failing column. PyArrow reports it either by
                # name ("...column 'score'...") or by zero-based index
                # ("...In CSV column #2:..."), where the index refers to the
                # original CSV column order.
                col = self._failing_csv_column(error_msg, original_names, columns)
                if col is None:
                    raise
                if col in locked_cols:
                    raise ConfigurationError(
                        f"Column '{col}' cannot be cast to the requested dtype "
                        f"({schema.get(col, {}).get('postgres_type', 'TEXT')}): {error_msg}"
                    ) from e
                col_fallback_count[col] += 1
                if col_fallback_count[col] > max_fallbacks:
                    raise
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
                continue

    def _failing_csv_column(
        self,
        error_msg: str,
        original_names: List[str],
        columns: List[str],
    ) -> Optional[str]:
        """Map a pyarrow CSV conversion error to the failing cleaned column name."""
        m = re.search(r"column\s*#?(\d+)", error_msg)
        if m:
            idx = int(m.group(1))
            if 0 <= idx < len(original_names):
                return columns[idx]
        for name in original_names:
            if name in error_msg:
                return columns[original_names.index(name)]
        return None
