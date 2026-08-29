from __future__ import annotations

from typing import Any, List, Optional, Union
from datetime import datetime, timezone

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class ArithmeticOps:
    """
    Shared infrastructure for arithmetic operations executed on the database.

    Backend-specific logic lives in duckdb.py / postgres.py / clickhouse.py as
    complete, self-contained copies of the operational methods + the divergent
    helpers (_numeric_text_cast, _numeric_exprs, _operand_expr, _build_binary,
    _add_column_if_not_exists, _apply_expression). This base holds only the
    backend-agnostic helpers.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers (mirror DataCleaningOps)
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _fetch_data(self, table: str, schema: str, columns: list) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if not columns:
            return pd.DataFrame()
        sanitized = [
            SQLIdentifierSanitizer.sanitize(str(c), allow_qualified=False) for c in columns
        ]
        col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TEXT")

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = (max_op or 0) + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    async def _resolve_output_table_name(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)

        if new_table:
            candidate = SQLIdentifierSanitizer.sanitize(new_table)
        elif backend is not None and data_id:
            candidate = await self._generate_transient_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    async def _prepare_operation_table(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        source_table = SQLIdentifierSanitizer.sanitize(table)
        output_table = await self._resolve_output_table_name(
            source_table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

        qualified_source = self._qualified_table(source_table, safe_schema)
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source}")
        return output_table

    async def _materialize_query_as_table(
        self,
        query: str,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        output_table = await self._resolve_output_table_name(
            table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS {query}")
        return output_table

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for arithmetic operation: {self.db.__class__.__name__}"
        )

    def _expr(self, val: Union[str, float, int]) -> str:
        """Return a SQL‑safe expression: quoted column or numeric literal."""
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, str):
            try:
                float(val)                     # it's a numeric string
                return val
            except ValueError:
                safe = SQLIdentifierSanitizer.sanitize(val)
                return self.db.quote_identifier(safe)
        return str(val)

    def _is_textual_dtype(self, dtype: Optional[str]) -> bool:
        if not dtype:
            return False
        normalized = dtype.lower()
        return any(
            token in normalized
            for token in ("char", "varchar", "string", "text")
        )

    def _involved_cols(self, *vals) -> List[str]:
        """Extract column names (strings that aren't numeric literals) from arguments."""
        cols = []
        for v in vals:
            if isinstance(v, str):
                try:
                    float(v)   # numeric string – not a column
                except ValueError:
                    cols.append(v)
        return cols

    def _generate_target_col(self, col1: Any, op: str, col2: Any = None) -> str:
        """Create a human‑readable default column name."""
        def _clean(x):
            if isinstance(x, str):
                return x.strip('"').strip("`").replace('"', '').replace(" ", "_")
            elif isinstance(x, (int, float)):
                return SQLIdentifierSanitizer.sanitize(f"_{x}")
            return "scalar"
        parts = [f"{_clean(col1)}"]
        if op:
            parts.append(op)
        if col2 is not None:
            parts.append(_clean(col2))
        return "_".join(parts)

    def _operand_kind(self, val) -> str:
        if isinstance(val, (int, float)):
            return "scalar"
        if isinstance(val, str):
            try:
                float(val)
                return "scalar"
            except ValueError:
                return "column"
        return "scalar"
