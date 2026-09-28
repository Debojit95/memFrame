from typing import Any, Dict, List, Optional, Union
import traceback
from datetime import datetime, UTC
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class ComparisonOps:
    """
    Core comparison operations – compares two columns element‑wise and
    adds a boolean result column to the table.

    Every public method creates a new transient table, adds the result
    column there, and returns a standardised response with the new table
    name (identical pattern to DataCleaningOps and ArithmeticOps).

    Shared infrastructure lives here on DuckDB/PostgreSQL-flavoured
    defaults; clickhouse.py overrides small dialect hooks
    (_engine_clause, _datetime_cast) and one structural override
    (_compare_columns, single CTAS instead of clone → add → update).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ----------------------------------------------------------------
    #  Internal helpers (identical to other core classes)
    # ----------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetch_data(self, table: str, schema: str, columns: Union[str, List[str]] = "*") -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            sanitized = [SQLIdentifierSanitizer.sanitize(str(c)) for c in columns]
            col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TEXT")

    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await backend.fetchval(sql, *args)

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self._backend_fetch_val(
            backend,
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
            candidate = f"{safe_table}__op_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    def _engine_clause(self) -> str:
        """Extra table-engine SQL for CTAS; ClickHouse overrides."""
        return ""

    def _datetime_cast(self) -> str:
        """Target type for datetime comparisons; ClickHouse overrides."""
        return "TIMESTAMPTZ"

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

        await self._exec(
            f"CREATE TABLE {qualified_target} {self._engine_clause()}"
            f"AS SELECT * FROM {qualified_source}"
        )

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

        await self._exec(
            f"CREATE TABLE {qualified_target} {self._engine_clause()}AS {query}"
        )

        return output_table

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    def _generate_cleaned_column_name(self, col1: str, op: str, col2: str) -> str:
        """Create a safe, descriptive column name."""
        op_map = {
            "==": "eq",
            "=": "eq",
            "!=": "ne",
            ">": "gt",
            "<": "lt",
            ">=": "ge",
            "<=": "le",
        }
        return f"cmp_{col1}_{op_map.get(op, op)}_{col2}"

    def _sql_operator(self, operator: str) -> str:
        """Translate user-facing comparison operators into SQL operators."""
        return "=" if operator == "==" else operator

    async def _add_new_column(self, table: str, schema: str, col_name: str, col_type: str):
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(col_name)
        await self._exec(
            f'ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {col_type}'
        )

    def _success_response(
        self, message: str, sample_df: pd.DataFrame, **extra
    ) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": sample_df,
            **extra,
        }

    def _error_response(self, msg: str) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for comparison operation: {self.db.__class__.__name__}"
        )

    # ----------------------------------------------------------------
    #  Core comparison helper – now creates a new table first
    # ----------------------------------------------------------------
    async def _compare_columns(
        self,
        table: str,
        schema: str,
        col1: str,
        col2: str,
        operator: str,
        col1_expr: Optional[str] = None,
        col2_expr: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generic comparison that:
        - PostgreSQL/DuckDB: Creates a clone, adds a boolean column, UPDATEs it
        - ClickHouse: Creates a new table via CTAS with the computed column
                      (avoids asynchronous mutations which are heavy and non-blocking)
        """
        try:
            supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
            if not isinstance(self.db, supported):
                raise self._unsupported_backend_error()

            safe_col1 = SQLIdentifierSanitizer.sanitize(col1)
            safe_col2 = SQLIdentifierSanitizer.sanitize(col2)

            left_expr = col1_expr if col1_expr else self.db.quote_identifier(safe_col1)
            right_expr = col2_expr if col2_expr else self.db.quote_identifier(safe_col2)
            sql_operator = self._sql_operator(operator)

            new_col = self._generate_cleaned_column_name(col1, operator, col2)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            # ──────────────────────────────────────────────────────────
            # PostgreSQL / DuckDB: Clone → Add Column → UPDATE
            # ──────────────────────────────────────────────────────────
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            qualified = self._qualified_table(working_table, schema)
            await self._exec(
                f"""
                UPDATE {qualified}
                SET {self.db.quote_identifier(safe_new)} = ({left_expr} {sql_operator} {right_expr})
                """
            )

            # Sample result from the working table
            sample = await self._fetch_data(
                working_table, schema, columns=[safe_col1, safe_col2, safe_new]
            )

            return self._success_response(
                f"Compared {col1} {operator} {col2} → {new_col}",
                sample,
                col1=col1,
                col2=col2,
                operator=operator,
                new_column=new_col,
                new_table=working_table,
            )

        except Exception as e:
            return self._error_response(
                f"compare error: {str(e)}\n{traceback.format_exc()}"
            )

    # ----------------------------------------------------------------
    #  CORE COMPARE APIs
    # ----------------------------------------------------------------
    async def compare_numeric(
        self, table: str, schema: str, col1: str, col2: str, operator: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Compare two numeric columns."""
        supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
        if isinstance(self.db, supported):
            return await self._compare_columns(
                table, schema, col1, col2, operator,
                backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    async def compare_categorical(
        self, table: str, schema: str, col1: str, col2: str, operator: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Compare two categorical (string) columns."""
        supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
        if isinstance(self.db, supported):
            return await self._compare_columns(
                table, schema, col1, col2, operator,
                backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    async def compare_datetime(
        self, table: str, schema: str, col1: str, col2: str, operator: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compare two datetime columns.  Automatically casts both columns to
        appropriate datetime types to handle VARCHAR-stored timestamps.
        """
        supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
        if isinstance(self.db, supported):
            safe_col1 = SQLIdentifierSanitizer.sanitize(col1)
            safe_col2 = SQLIdentifierSanitizer.sanitize(col2)

            cast_type = self._datetime_cast()
            cast_expr1 = f"CAST({self.db.quote_identifier(safe_col1)} AS {cast_type})"
            cast_expr2 = f"CAST({self.db.quote_identifier(safe_col2)} AS {cast_type})"

            return await self._compare_columns(
                table, schema, col1, col2, operator,
                col1_expr=cast_expr1,
                col2_expr=cast_expr2,
                backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()
