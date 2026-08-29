"""
Selection operations - shared infrastructure.

Backend-agnostic helpers only. All operational methods (asof, at, iat, get,
select_dtypes, iloc) and the per-backend divergent hooks live in
duckdb.py / postgres.py / clickhouse.py as complete, self-contained copies.
"""

from typing import Any, List, Union
from collections.abc import Mapping

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.exceptions import DataNotFound
from memframe.core.analytix._response import fail, ok


class DataSelectionOps:
    """
    Shared SQL helpers for row/column selection, label-based access,
    and conditional replacement. Backend-specific logic is not present here.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    def _quote(self, identifier: str) -> str:
        return self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(identifier))

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}"

    def _success_response(self, message, sample_df=None, **extra):
        result = extra.pop("result", sample_df)
        return ok(message, result=result, **extra)

    def _error_response(self, msg):
        return fail(msg)

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for selection operation: {self.db.__class__.__name__}"
        )

    @staticmethod
    def _row_get(row: object, key: str, idx: int):
        if isinstance(row, Mapping):
            return row[key]
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
        return row[idx]

    def _first_value_from_row(self, row: object):
        if isinstance(row, Mapping):
            return next(iter(row.values()))
        if hasattr(row, "keys"):
            keys = list(row.keys())
            if keys:
                return row[keys[0]]
        return row[0]

    def _first_value_from_rows(self, rows: List[Any]):
        if not rows:
            raise DataNotFound("Query returned no rows")
        return self._first_value_from_row(rows[0])

    async def _fetch_sample(
        self,
        table: str,
        schema: str,
        columns: Union[str, List[str]] = "*",
    ) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            if isinstance(columns, str):
                columns = [columns]
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self._quote(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_in_chunks(
        self,
        table: str,
        schema: str,
        chunk_size: int,
        columns: Union[str, List[str]] = "*",
    ):
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            if isinstance(columns, str):
                columns = [columns]
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self._quote(c) for c in sanitized)
        offset = 0
        while True:
            query = f"SELECT {col_clause} FROM {qualified} LIMIT {chunk_size} OFFSET {offset}"
            rows = await self._fetch(query)
            if not rows:
                break
            yield pd.DataFrame([dict(r) for r in rows])
            offset += chunk_size

    async def _generate_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = max_op + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    async def _resolve_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
        candidate = await self._generate_transient_table_name(base_table, backend, data_id)
        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, backend.transient_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1
        return output_table

    @staticmethod
    def _classify_column_type(sql_type: str) -> str:
        low = sql_type.lower()
        normalized = low
        if normalized.startswith("nullable(") and normalized.endswith(")"):
            normalized = normalized[len("nullable("):-1]
        t = normalized.split("(")[0].strip()
        numeric_types = {
            "smallint", "integer", "bigint", "int2", "int4", "int8",
            "decimal", "numeric", "real", "float4", "float8", "double precision",
            "double", "float",
            # ClickHouse numeric types (UInt8 is reserved for booleans, see below)
            "int8", "int16", "int32", "int64", "uint16", "uint32", "uint64",
            "float32", "float64"
        }
        boolean_types = {"bool", "boolean", "uint8"}  # ponytail: ClickHouse stores bool as UInt8
        categorical_types = {
            "varchar", "character varying", "char", "character", "text",
            "nchar", "nvarchar", "clob",
            # ClickHouse string types
            "string", "fixedstring"
        }
        date_types = {"date"}
        timestamp_types = {
            "timestamp", "timestamptz", "datetime", "datetime64",
            "timestamp with time zone", "timestamp without time zone",
        }

        if t in numeric_types:
            return "numeric"
        elif t in boolean_types:
            return "boolean"
        elif t in categorical_types:
            return "categorical"
        elif t in date_types:
            return "date"
        elif t in timestamp_types or t.startswith("datetime") or low.startswith("timestamp"):
            return "timestamp"
        else:
            return "other"

    def _normalize_asof_value(self, value, column_kind: str):
        if column_kind in {"date", "timestamp"}:
            ts = pd.Timestamp(value)
            if column_kind == "date":
                return ts.date()
            return ts.to_pydatetime()
        if column_kind == "numeric":
            if isinstance(value, str):
                parsed = pd.to_numeric(value)
                if hasattr(parsed, "item"):
                    return parsed.item()
                return parsed
            return value
        return value
