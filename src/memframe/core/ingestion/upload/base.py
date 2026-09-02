import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import pyarrow as pa

from memframe.core.ingestion.upload.duckdb_impl import DuckDBUploadImpl
from memframe.core.ingestion.upload.postgres_impl import PostgresUploadImpl
from memframe.core.ingestion.upload.clickhouse_impl import ClickHouseUploadImpl

if TYPE_CHECKING:
    import pandas as pd

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.context import ContextManager
from memframe.exceptions import ConfigurationError


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class Uploader(DuckDBUploadImpl, PostgresUploadImpl, ClickHouseUploadImpl):
    """Uploader mixin class - works when mixed into MemFrame via BaseWrapper.

    Accesses self._backend and self._type_detector from the MemFrame instance.

    Format-specific pipelines live in the upload strategies
    (CsvUploadStrategy / ParquetUploadStrategy / DfUploadStrategy); this
    class holds the shared infrastructure (type mapping, backend dispatch,
    dtype overrides) and the public entry points. Per-backend method bodies
    live in the DuckDBUploadImpl / PostgresUploadImpl / ClickHouseUploadImpl
    mixins composed above.
    """

    # ── Helper methods ─────────────────────────────────────────
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
                    raw = f.read(4 * 1024 * 1024)
                raw.decode(enc)
                return True
            except (UnicodeDecodeError, LookupError):
                return False

        # ponytail: chardet removed — stdlib only; ascii is subset of utf-8, prefer it.
        candidates = ["utf-8" if detected and detected.lower() == "ascii" else detected]
        candidates += ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
        for enc in candidates:
            if enc and _validate(enc):
                return enc
        return "latin-1"

    # ── Table creation: Typed ──────────────────────────────────
    async def _create_final_table_typed(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        """Create table with proper types directly (no TEXT intermediary)."""
        if self._backend.backend == Backend.DUCKDB:
            await self._create_final_table_typed_duckdb(table_name, columns, schema)
        elif self._backend.backend == Backend.POSTGRES:
            await self._create_final_table_typed_postgres(table_name, columns, schema)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._create_final_table_typed_clickhouse(table_name, columns, schema)

    # ── Table creation: All TEXT (Legacy fallback) ─────────────────
    async def _create_final_table_all_text(self, table_name: str, columns: List[str]) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._create_final_table_all_text_duckdb(table_name, columns)
        elif self._backend.backend == Backend.POSTGRES:
            await self._create_final_table_all_text_postgres(table_name, columns)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._create_final_table_all_text_clickhouse(table_name, columns)

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table(self, table_name: str, arrow_table: pa.Table) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._insert_arrow_table_duckdb(table_name, arrow_table)
        elif self._backend.backend == Backend.POSTGRES:
            await self._insert_arrow_table_postgres(table_name, arrow_table)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._insert_arrow_table_clickhouse(table_name, arrow_table)

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample(self, table_name: str, columns: List[str], limit: int = 50) -> pa.Table:
        if self._backend.backend == Backend.DUCKDB:
            return await self._fetch_arrow_sample_duckdb(table_name, columns, limit)
        elif self._backend.backend == Backend.POSTGRES:
            return await self._fetch_arrow_sample_postgres(table_name, columns, limit)
        elif self._backend.backend == Backend.CLICKHOUSE:
            return await self._fetch_arrow_sample_clickhouse(table_name, columns, limit)

    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        if self._backend.backend == Backend.DUCKDB:
            await self._cast_table_in_place_duckdb(final_table, columns, schema)
        elif self._backend.backend == Backend.POSTGRES:
            await self._cast_table_in_place_postgres(final_table, columns, schema)
        elif self._backend.backend == Backend.CLICKHOUSE:
            await self._cast_table_in_place_clickhouse(final_table, columns, schema)

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

    async def fetchval(self, query: str, *args) -> Any:
        return await self._backend.fetchval(query, *args)

    def _placeholder(self, index: int) -> str:
        return self._backend.placeholder(index)

    async def drop_table(self, table_name: str) -> None:
        await self._backend.drop_table(table_name)

    async def table_exists(self, table_name: str) -> bool:
        return await self._backend.table_exists(table_name)

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        return self._backend._split_qualified_table_name(table_name)

    # ── Dtype override helpers ─────────────────────────────────
    _PANDAS_DTYPE_TO_POSTGRES = {
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime64": "TIMESTAMP",
        "str": "TEXT",
        "object": "TEXT",
        "string": "TEXT",
        "category": "TEXT",
    }

    def _normalize_dtype_override(self, dtypes: Dict[str, str]) -> Dict[str, str]:
        """Validate and normalize a user dtype override to postgres_type strings.

        Accepts both SQL type names (BIGINT, TIMESTAMP, ...) and pandas dtype
        names (int64, float64, bool, ...).
        """
        known_sql = {
            "TEXT", "VARCHAR", "CHAR", "INTEGER", "INT", "BIGINT", "SMALLINT",
            "NUMERIC", "DECIMAL", "REAL", "FLOAT", "FLOAT4", "DOUBLE", "FLOAT8",
            "DOUBLE PRECISION", "BOOLEAN", "BOOL", "DATE", "TIMESTAMP",
            "TIMESTAMPTZ", "DATETIME", "BYTEA",
        }
        normalized = {}
        for col, raw in dtypes.items():
            if not isinstance(raw, str) or not raw.strip():
                raise ConfigurationError(f"dtypes value for '{col}' must be a non-empty string")
            spec = raw.strip()
            base = spec.split("(")[0].split("[")[0].strip().lower()
            if base in self._PANDAS_DTYPE_TO_POSTGRES:
                pg_type = self._PANDAS_DTYPE_TO_POSTGRES[base]
                if base == "datetime64" and any(x in spec.lower() for x in ("utc", "tz=")):
                    pg_type = "TIMESTAMPTZ"
                normalized[col] = pg_type
            elif base.upper() in known_sql:
                normalized[col] = spec.upper()
            else:
                raise ConfigurationError(
                    f"Unknown dtype '{raw}' for column '{col}'. Use a SQL type "
                    f"(e.g. BIGINT, TIMESTAMP, TEXT) or pandas dtype (e.g. int64, float64)."
                )
        return normalized

    def _apply_dtype_override(
        self,
        schema: Dict[str, Dict[str, Any]],
        columns: List[str],
        dtypes: Optional[Dict[str, str]],
    ) -> Dict[str, Dict[str, Any]]:
        """Overlay a user dtype override on an inferred schema (highest precedence)."""
        if not dtypes:
            return schema
        normalized = self._normalize_dtype_override(dtypes)
        unknown = sorted(set(normalized) - set(columns))
        if unknown:
            raise ConfigurationError(f"dtypes references unknown column(s): {', '.join(unknown)}")
        for col, pg_type in normalized.items():
            schema[col] = {
                "postgres_type": pg_type,
                "clickhouse_type": self._postgres_type_to_clickhouse(pg_type),
                "is_nullable": schema.get(col, {}).get("is_nullable", True),
            }
        return schema

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

    # ── Arrow type mapping ──────────────────────────────────────
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

    # ── Upload entry points (delegate to strategies) ───────────
    async def _aupload_csv(
        self, file_path: Union[str, Path], dtypes: Optional[Dict[str, str]] = None
    ) -> ContextManager:
        from memframe.core.ingestion.upload.strategies.csv import CsvUploadStrategy

        data_id = await CsvUploadStrategy(self).upload(file_path, dtypes=dtypes)
        return self._memframe_from_data_id(data_id)

    async def _aupload_parquet(
        self, file_path: Union[str, Path], dtypes: Optional[Dict[str, str]] = None
    ) -> ContextManager:
        from memframe.core.ingestion.upload.strategies.parquet import ParquetUploadStrategy

        data_id = await ParquetUploadStrategy(self).upload(file_path, dtypes=dtypes)
        return self._memframe_from_data_id(data_id)

    async def _aupload_df(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        from memframe.core.ingestion.upload.strategies.df import DfUploadStrategy

        data_id = await DfUploadStrategy(self).upload(df, filename, dtypes=dtypes)
        return self._memframe_from_data_id(data_id)
