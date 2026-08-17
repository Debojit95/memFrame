from __future__ import annotations
from functools import wraps
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


def _response_errors(method):
    """Keep arithmetic operation failures inside the response contract."""
    @wraps(method)
    async def wrapped(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except Exception as exc:
            return fail(f"{method.__name__} error: {exc}")

    return wrapped


class ArithmeticOps:
    """
    Core arithmetic operations executed directly on the database.
    Every method creates a new transient table, adds the result column,
    and returns a standardised response (identical pattern to DataCleaningOps).

    Works uniformly with DuckDB, PostgreSQL, and ClickHouse.
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

    async def _add_column_if_not_exists(
        self, table: str, schema: str, column: str, data_type: str = "DOUBLE PRECISION"
    ) -> None:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        
        if isinstance(self.db, ClickHouseAdapter) and data_type == "DOUBLE PRECISION":
            data_type = "Float64"

        try:
            await self._exec(f"SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1")
        except Exception:
            await self._exec(
                f"ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {data_type}"
            )

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for arithmetic operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Expression helpers
    # ------------------------------------------------------------------
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

    def _numeric_text_cast(self, quoted_col: str) -> str:
        if isinstance(self.db, PostgresAdapter):
            return (
                "CASE "
                f"WHEN TRIM(({quoted_col})::TEXT) ~ "
                "'^[+-]?(?:(?:[0-9]+(?:\\.[0-9]*)?)|(?:\\.[0-9]+))(?:[eE][+-]?[0-9]+)?$' "
                f"THEN TRIM(({quoted_col})::TEXT)::DOUBLE PRECISION "
                "ELSE NULL END"
            )
        elif isinstance(self.db, DuckDBAdapter):
            return f"TRY_CAST({quoted_col} AS DOUBLE)"
        elif isinstance(self.db, ClickHouseAdapter):
            return f"toFloat64OrNull({quoted_col})"
        else:
            raise self._unsupported_backend_error()

    async def _numeric_exprs(
        self, table: str, schema: str, *vals: Union[str, float, int]
    ) -> List[str]:
        """
        Return SQL expressions for numeric operations.

        Uploads can leave numeric-looking values as text, especially floats written
        in scientific notation. Cast only textual columns so native numeric columns
        keep their backend-specific type behavior.
        """
        involved = self._involved_cols(*vals)
        column_types = await self.db.get_column_types(table, schema) if involved else {}
        expressions = []

        for val in vals:
            if not isinstance(val, str):
                expressions.append(self._expr(val))
                continue

            try:
                float(val)
                expressions.append(val)
                continue
            except ValueError:
                pass

            safe = SQLIdentifierSanitizer.sanitize(val)
            quoted = self.db.quote_identifier(safe)
            if self._is_textual_dtype(column_types.get(safe)):
                expressions.append(self._numeric_text_cast(quoted))
            else:
                expressions.append(quoted)

        return expressions

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

    # ------------------------------------------------------------------
    # Operand classification (vector-vector / vector-scalar / scalar-scalar)
    # ------------------------------------------------------------------
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

    def _operand_expr(self, val, types: Dict[str, str]) -> str:
        if isinstance(val, (int, float)):
            return f"({val})"
        if isinstance(val, str):
            try:
                return f"({float(val)})"
            except ValueError:
                pass
            safe = SQLIdentifierSanitizer.sanitize(val)
            quoted = self.db.quote_identifier(safe)
            if self._is_textual_dtype(types.get(safe)):
                return self._numeric_text_cast(quoted)
            return quoted
        return f"({val})"

    async def _build_binary(self, table, schema, col1, col2, op, *, divisor_nullif=False):
        lk, rk = self._operand_kind(col1), self._operand_kind(col2)
        if lk == "scalar" and rk == "scalar":
            return None, None, "scalar-scalar"
        types = await self.db.get_column_types(table, schema) if (lk == "column" or rk == "column") else {}
        left = self._operand_expr(col1, types)
        right = self._operand_expr(col2, types)
        if divisor_nullif:
            right = f"NULLIF({right}, 0)"
        return f"{left} {op} {right}", self._involved_cols(col1, col2), f"{lk}-{rk}"

    # ------------------------------------------------------------------
    # ** NEW ** Core operation wrapper – creates a new table first
    # ------------------------------------------------------------------
    async def _apply_expression(
        self,
        table: str,
        schema: str,
        involved_cols: List[str],
        expression: str,
        target_col: str,
        operation_name: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
        **extra,
    ) -> Dict[str, Any]:
        """
        - Clone the source table into a new transient table.
        - Add the target column (if missing) and update with the expression.
        - Return a standardised response that includes the new table name.
        """
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            pass
        else:
            raise self._unsupported_backend_error()

        # 1. Create a working copy (transient table)
        working_table = await self._prepare_operation_table(
            table, schema, backend=backend, data_id=data_id, new_table=new_table
        )

        # 2. Ensure target column exists
        tgt_safe = SQLIdentifierSanitizer.sanitize(target_col)
        await self._add_column_if_not_exists(working_table, schema, tgt_safe)

        # 3. Update the column with the expression
        qualified = self._qualified_table(working_table, schema)
        if isinstance(self.db, ClickHouseAdapter):
            await self._exec(
                f'ALTER TABLE {qualified} UPDATE {self.db.quote_identifier(tgt_safe)} = {expression} WHERE 1'
            )
        else:
            await self._exec(
                f'UPDATE {qualified} SET {self.db.quote_identifier(tgt_safe)} = {expression}'
            )

        # 4. Mirror the new column onto the original table (in-place mutation)
        await self._add_column_if_not_exists(table, schema, tgt_safe)
        original_qualified = self._qualified_table(table, schema)
        if isinstance(self.db, ClickHouseAdapter):
            await self._exec(
                f'ALTER TABLE {original_qualified} UPDATE {self.db.quote_identifier(tgt_safe)} = {expression} WHERE 1'
            )
        else:
            await self._exec(
                f'UPDATE {original_qualified} SET {self.db.quote_identifier(tgt_safe)} = {expression}'
            )

        # 5. Fetch a sample of the changed columns
        cols_to_fetch = list(set(involved_cols)) + [target_col]
        sample = await self._fetch_data(working_table, schema, cols_to_fetch)

        return ok(
            f"{operation_name}: {expression} → {tgt_safe}",
            involved_cols,
            [tgt_safe],
            sample,
            expression=expression,
            new_table=working_table,          # <-- the newly created table
            **extra,
        )

    # ==================================================================
    #  BINARY OPERATIONS (updated signatures with backend etc.)
    # ==================================================================
    @_response_errors
    async def add(self, table: str, schema: str, col1: Union[str, float, int],
                  col2: Union[str, float, int], target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        expr, involved, mode = await self._build_binary(table, schema, col1, col2, "+")
        if mode == "scalar-scalar":
            return fail("scalar-scalar arithmetic is not supported; provide at least one column")
        return await self._apply_expression(
            table, schema, involved, expr,
            target_col or self._generate_target_col(col1, "add", col2),
            "Addition", backend=backend, data_id=data_id, new_table=new_table
        )

    @_response_errors
    async def subtract(self, table: str, schema: str, col1: Union[str, float, int],
                        col2: Union[str, float, int], target_col: Optional[str] = None,
                        backend=None, data_id: Optional[str] = None,
                        new_table: Optional[str] = None):
        expr, involved, mode = await self._build_binary(table, schema, col1, col2, "-")
        if mode == "scalar-scalar":
            return fail("scalar-scalar arithmetic is not supported; provide at least one column")
        return await self._apply_expression(
            table, schema, involved, expr,
            target_col or self._generate_target_col(col1, "sub", col2),
            "Subtraction", backend=backend, data_id=data_id, new_table=new_table
        )

    @_response_errors
    async def multiply(self, table: str, schema: str, col1: Union[str, float, int],
                        col2: Union[str, float, int], target_col: Optional[str] = None,
                        backend=None, data_id: Optional[str] = None,
                        new_table: Optional[str] = None):
        expr, involved, mode = await self._build_binary(table, schema, col1, col2, "*")
        if mode == "scalar-scalar":
            return fail("scalar-scalar arithmetic is not supported; provide at least one column")
        return await self._apply_expression(
            table, schema, involved, expr,
            target_col or self._generate_target_col(col1, "mul", col2),
            "Multiplication", backend=backend, data_id=data_id, new_table=new_table
        )

    @_response_errors
    async def divide(self, table: str, schema: str, col1: Union[str, float, int],
                      col2: Union[str, float, int], target_col: Optional[str] = None,
                      backend=None, data_id: Optional[str] = None,
                      new_table: Optional[str] = None):
        expr, involved, mode = await self._build_binary(table, schema, col1, col2, "/", divisor_nullif=True)
        if mode == "scalar-scalar":
            return fail("scalar-scalar arithmetic is not supported; provide at least one column")
        return await self._apply_expression(
            table, schema, involved, expr,
            target_col or self._generate_target_col(col1, "div", col2),
            "Division", backend=backend, data_id=data_id, new_table=new_table
        )

    @_response_errors
    async def modulo(self, table: str, schema: str, col1: Union[str, float, int],
                     col2: Union[str, float, int], target_col: Optional[str] = None,
                     backend=None, data_id: Optional[str] = None,
                     new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            mod_func = "modulo" if isinstance(self.db, ClickHouseAdapter) else "MOD"
            expr = f"{mod_func}({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "mod", col2),
                "Modulo", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def power(self, table: str, schema: str, col1: Union[str, float, int],
                    col2: Union[str, float, int], target_col: Optional[str] = None,
                    backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            pow_func = "pow" if isinstance(self.db, ClickHouseAdapter) else "POWER"
            expr = f"{pow_func}({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "pow", col2),
                "Power", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    # ==================================================================
    #  UNARY OPERATIONS (updated)
    # ==================================================================
    @_response_errors
    async def absolute(self, table: str, schema: str, column: str,
                       target_col: Optional[str] = None,
                       backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"ABS({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "abs"),
                "Absolute value", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def negate(self, table: str, schema: str, column: str,
                     target_col: Optional[str] = None,
                     backend=None, data_id: Optional[str] = None,
                     new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"-({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "neg"),
                "Negation", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def round(self, table: str, schema: str, column: str,
                    digits: int = 0, target_col: Optional[str] = None,
                    backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            if isinstance(self.db, PostgresAdapter):
                expr = f"ROUND(CAST({col} AS NUMERIC), {digits})"
            else:
                expr = f"ROUND({col}, {digits})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, f"round{digits}"),
                f"Round to {digits} decimals", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def ceil(self, table: str, schema: str, column: str,
                   target_col: Optional[str] = None,
                   backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"CEIL({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "ceil"),
                "Ceiling", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def floor(self, table: str, schema: str, column: str,
                    target_col: Optional[str] = None,
                    backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"FLOOR({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "floor"),
                "Floor", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def truncate(self, table: str, schema: str, column: str,
                       digits: int = 0, target_col: Optional[str] = None,
                       backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            
            if isinstance(self.db, PostgresAdapter):
                # Postgres requires explicit cast to NUMERIC for TRUNC(val, digits)
                expr = f"TRUNC(CAST({col} AS NUMERIC), {digits})"
            elif isinstance(self.db, DuckDBAdapter):
                expr = f"TRUNC({col}, {digits})"
            elif isinstance(self.db, ClickHouseAdapter):
                # ClickHouse's truncate function takes (value, digits) 
                expr = f"truncate({col}, {digits})"
            else:
                raise self._unsupported_backend_error()
                
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, f"trunc{digits}"),
                f"Truncate to {digits} decimals", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()
    
    # ==================================================================
    #  EXP / LOG / ROOT (updated)
    # ==================================================================
    @_response_errors
    async def exp(self, table: str, schema: str, column: str,
                  target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"EXP({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "exp"),
                "Exponential", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def log(self, table: str, schema: str, column: str,
                  target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            ln_func = "log" if isinstance(self.db, ClickHouseAdapter) else "LN"
            expr = f"CASE WHEN {col} > 0 THEN {ln_func}({col}) ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "ln"),
                "Natural logarithm", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    
    @_response_errors
    async def log10(self, table: str, schema: str, column: str,
                    target_col: Optional[str] = None,
                    backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            
            if isinstance(self.db, PostgresAdapter):
                log_expr = f"LOG({col})"  # Postgres LOG() with 1 arg is base 10
            elif isinstance(self.db, DuckDBAdapter):
                log_expr = f"LOG10({col})"
            elif isinstance(self.db, ClickHouseAdapter):
                log_expr = f"log10({col})"
            else:
                raise self._unsupported_backend_error()
                
            expr = f"CASE WHEN {col} > 0 THEN {log_expr} ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "log10"),
                "Base‑10 logarithm", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()
    
    @_response_errors
    async def sqrt(self, table: str, schema: str, column: str,
                   target_col: Optional[str] = None,
                   backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"CASE WHEN {col} >= 0 THEN SQRT({col}) ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "sqrt"),
                "Square root", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    # ==================================================================
    #  TRIGONOMETRIC (updated)
    # ==================================================================
    @_response_errors
    async def sin(self, table: str, schema: str, column: str,
                  target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"SIN({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "sin"),
                "Sine", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()


    @_response_errors
    async def cos(self, table: str, schema: str, column: str,
                  target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"COS({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "cos"),
                "Cosine", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def tan(self, table: str, schema: str, column: str,
                  target_col: Optional[str] = None,
                  backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"TAN({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "tan"),
                "Tangent", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def asin(self, table: str, schema: str, column: str,
                   target_col: Optional[str] = None,
                   backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"ASIN({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "asin"),
                "Arcsine", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def acos(self, table: str, schema: str, column: str,
                   target_col: Optional[str] = None,
                   backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"ACOS({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "acos"),
                "Arccosine", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def atan(self, table: str, schema: str, column: str,
                   target_col: Optional[str] = None,
                   backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"ATAN({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "atan"),
                "Arctangent", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def atan2(self, table: str, schema: str,
                    col1: Union[str, float, int], col2: Union[str, float, int],
                    target_col: Optional[str] = None,
                    backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"ATAN2({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "atan2", col2),
                "ATAN2", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    # ==================================================================
    #  COMPLEX OPERATIONS (updated)
    # ==================================================================
    @_response_errors
    async def weighted_average(self, table: str, schema: str,
                               col1: Union[str, float, int], col2: Union[str, float, int],
                               weight1: float = 1, weight2: float = 1,
                               target_col: Optional[str] = None,
                               backend=None, data_id: Optional[str] = None,
                               new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"(({left} * {weight1}) + ({right} * {weight2})) / NULLIF(({weight1} + {weight2}), 0)"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "wavg", col2),
                "Weighted average", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def percentage_change(self, table: str, schema: str,
                                old_col: str, new_col: str,
                                target_col: Optional[str] = None,
                                backend=None, data_id: Optional[str] = None,
                                new_table: Optional[str] = None):
        if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
            old, new = await self._numeric_exprs(table, schema, old_col, new_col)
            expr = f"((1.0 * {new} - 1.0 * {old}) / NULLIF(ABS(1.0 * {old}), 0)) * 100"
            involved = [old_col, new_col]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(new_col, "pctch", old_col),
                "Percentage change", backend=backend, data_id=data_id, new_table=new_table
            )
        else:
            raise self._unsupported_backend_error()

    @_response_errors
    async def normalize_range(self, table: str, schema: str, column: str,
                              target_col: Optional[str] = None,
                              backend=None, data_id: Optional[str] = None,
                              new_table: Optional[str] = None):
        try:
            # For normalisation we need min/max from the *source* table.
            # We first fetch them, then operate on the new table.
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                source_qualified = self._qualified_table(table, schema)
                (col,) = await self._numeric_exprs(table, schema, column)
                min_val = await self._fetchval(f"SELECT MIN({col}) FROM {source_qualified}")
                max_val = await self._fetchval(f"SELECT MAX({col}) FROM {source_qualified}")
                expr = f"(1.0 * {col} - {min_val}) / NULLIF(({max_val} - {min_val}), 0)"
                involved = [column]
                return await self._apply_expression(
                    table, schema, involved, expr,
                    target_col or self._generate_target_col(column, "norm"),
                    "Min‑max normalisation", backend=backend, data_id=data_id, new_table=new_table
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return fail(f"normalize_range error: {str(e)}", [column])
