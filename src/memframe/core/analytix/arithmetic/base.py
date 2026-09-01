from __future__ import annotations

from typing import Any, List, Optional, Union
from datetime import datetime, timezone

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


class ArithmeticOps:
    """
    Shared infrastructure for arithmetic operations executed on the database.

    All 27 operational methods and the shared helpers are defined once here
    on DuckDB-flavoured defaults; postgres.py / clickhouse.py override small
    dialect hooks (_numeric_text_cast, _fn_name, _round_expr, _trunc_expr,
    _update_stmt) and ClickHouse keeps one structural override
    (_add_column_if_not_exists).
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

    # ------------------------------------------------------------------
    # Backend dialect hooks (DuckDB defaults; PG/ClickHouse override)
    # ------------------------------------------------------------------
    def _numeric_text_cast(self, quoted_col: str) -> str:
        """Cast a (possibly textual) column expression to double."""
        return f"TRY_CAST({quoted_col} AS DOUBLE)"

    def _fn_name(self, name: str) -> str:
        """Scalar SQL function name as emitted in expressions."""
        return name

    def _round_expr(self, col: str, digits) -> str:
        return f"ROUND({col}, {digits})"

    def _trunc_expr(self, col: str, digits) -> str:
        return f"TRUNC({col}, {digits})"

    def _update_stmt(self, qualified: str, set_expr: str) -> str:
        """In-place UPDATE; ClickHouse overrides with ALTER TABLE UPDATE."""
        return f"UPDATE {qualified} SET {set_expr}"

    # ------------------------------------------------------------------
    # Divergent helpers (hoisted; call the hooks above)
    # ------------------------------------------------------------------
    async def _numeric_exprs(self, table, schema, *vals):
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

    def _operand_expr(self, val, types):
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

    async def _add_column_if_not_exists(self, table, schema, column, data_type="DOUBLE PRECISION"):
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        try:
            await self._exec(f"SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1")
        except Exception:
            await self._exec(
                f"ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {data_type}"
            )

    async def _apply_expression(self, table, schema, involved_cols, expression, target_col,
                                operation_name, backend=None, data_id=None, new_table=None, **extra):
        working_table = await self._prepare_operation_table(
            table, schema, backend=backend, data_id=data_id, new_table=new_table
        )
        tgt_safe = SQLIdentifierSanitizer.sanitize(target_col)
        await self._add_column_if_not_exists(working_table, schema, tgt_safe)
        qualified = self._qualified_table(working_table, schema)
        await self._exec(
            self._update_stmt(qualified, f'{self.db.quote_identifier(tgt_safe)} = {expression}')
        )
        await self._add_column_if_not_exists(table, schema, tgt_safe)
        original_qualified = self._qualified_table(table, schema)
        await self._exec(
            self._update_stmt(original_qualified, f'{self.db.quote_identifier(tgt_safe)} = {expression}')
        )
        # ponytail: sorted() — set ordering was hash-seed dependent, making the
        # sample SELECT nondeterministic run to run
        cols_to_fetch = sorted(set(involved_cols)) + [target_col]
        sample = await self._fetch_data(working_table, schema, cols_to_fetch)
        return ok(
            f"{operation_name}: {expression} → {tgt_safe}",
            involved_cols,
            [tgt_safe],
            sample,
            expression=expression,
            new_table=working_table,
            **extra,
        )

    # ------------------------------------------------------------------
    # Operational methods (hoisted verbatim from the per-backend copies)
    # ------------------------------------------------------------------
    async def add(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            expr, involved, mode = await self._build_binary(table, schema, col1, col2, "+")
            if mode == "scalar-scalar":
                return fail("scalar-scalar arithmetic is not supported; provide at least one column")
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "add", col2),
                "Addition", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"add error: {e}")

    async def subtract(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                       target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        try:
            expr, involved, mode = await self._build_binary(table, schema, col1, col2, "-")
            if mode == "scalar-scalar":
                return fail("scalar-scalar arithmetic is not supported; provide at least one column")
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "sub", col2),
                "Subtraction", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"subtract error: {e}")

    async def multiply(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                       target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        try:
            expr, involved, mode = await self._build_binary(table, schema, col1, col2, "*")
            if mode == "scalar-scalar":
                return fail("scalar-scalar arithmetic is not supported; provide at least one column")
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "mul", col2),
                "Multiplication", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"multiply error: {e}")

    async def divide(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                     target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                     new_table: Optional[str] = None):
        try:
            expr, involved, mode = await self._build_binary(table, schema, col1, col2, "/", divisor_nullif=True)
            if mode == "scalar-scalar":
                return fail("scalar-scalar arithmetic is not supported; provide at least one column")
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "div", col2),
                "Division", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"divide error: {e}")

    async def modulo(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                     target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                     new_table: Optional[str] = None):
        try:
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"{self._fn_name('MOD')}({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "mod", col2),
                "Modulo", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"modulo error: {e}")

    async def power(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                    target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        try:
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"{self._fn_name('POWER')}({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "pow", col2),
                "Power", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"power error: {e}")

    async def absolute(self, table, schema, column: Union[str, float, int],
                       target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('ABS')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "abs"),
                "Absolute value", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"absolute error: {e}")

    async def negate(self, table, schema, column: Union[str, float, int],
                     target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                     new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"-({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "neg"),
                "Negation", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"negate error: {e}")

    async def round(self, table, schema, column: Union[str, float, int], digits: int = 0,
                    target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = self._round_expr(col, digits)
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, f"round{digits}"),
                f"Round to {digits} decimals", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"round error: {e}")

    async def ceil(self, table, schema, column: Union[str, float, int],
                   target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('CEIL')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "ceil"),
                "Ceiling", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"ceil error: {e}")

    async def floor(self, table, schema, column: Union[str, float, int],
                    target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('FLOOR')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "floor"),
                "Floor", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"floor error: {e}")

    async def truncate(self, table, schema, column: Union[str, float, int], digits: int = 0,
                       target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                       new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = self._trunc_expr(col, digits)
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, f"trunc{digits}"),
                f"Truncate to {digits} decimals", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"truncate error: {e}")

    async def exp(self, table, schema, column: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('EXP')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "exp"),
                "Exponential", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"exp error: {e}")

    async def log(self, table, schema, column: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"CASE WHEN {col} > 0 THEN {self._fn_name('LN')}({col}) ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "ln"),
                "Natural logarithm", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"log error: {e}")

    async def log10(self, table, schema, column: Union[str, float, int],
                    target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"CASE WHEN {col} > 0 THEN {self._fn_name('LOG10')}({col}) ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "log10"),
                "Base‑10 logarithm", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"log10 error: {e}")

    async def sqrt(self, table, schema, column: Union[str, float, int],
                   target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"CASE WHEN {col} >= 0 THEN {self._fn_name('SQRT')}({col}) ELSE NULL END"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "sqrt"),
                "Square root", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"sqrt error: {e}")

    async def sin(self, table, schema, column: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('SIN')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "sin"),
                "Sine", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"sin error: {e}")

    async def cos(self, table, schema, column: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('COS')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "cos"),
                "Cosine", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"cos error: {e}")

    async def tan(self, table, schema, column: Union[str, float, int],
                  target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                  new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('TAN')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "tan"),
                "Tangent", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"tan error: {e}")

    async def asin(self, table, schema, column: Union[str, float, int],
                   target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('ASIN')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "asin"),
                "Arcsine", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"asin error: {e}")

    async def acos(self, table, schema, column: Union[str, float, int],
                   target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('ACOS')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "acos"),
                "Arccosine", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"acos error: {e}")

    async def atan(self, table, schema, column: Union[str, float, int],
                   target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                   new_table: Optional[str] = None):
        try:
            (col,) = await self._numeric_exprs(table, schema, column)
            expr = f"{self._fn_name('ATAN')}({col})"
            involved = [column]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(column, "atan"),
                "Arctangent", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"atan error: {e}")

    async def atan2(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                    target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                    new_table: Optional[str] = None):
        try:
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"{self._fn_name('ATAN2')}({left}, {right})"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "atan2", col2),
                "ATAN2", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"atan2 error: {e}")

    async def weighted_average(self, table, schema, col1: Union[str, float, int], col2: Union[str, float, int],
                              weight1: Union[int, float] = 1, weight2: Union[int, float] = 1,
                              target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                              new_table: Optional[str] = None):
        try:
            left, right = await self._numeric_exprs(table, schema, col1, col2)
            expr = f"(({left} * {weight1}) + ({right} * {weight2})) / NULLIF(({weight1} + {weight2}), 0)"
            involved = self._involved_cols(col1, col2)
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(col1, "wavg", col2),
                "Weighted average", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"weighted_average error: {e}")

    async def percentage_change(self, table, schema, old_col: Union[str, float, int], new_col: Union[str, float, int],
                               target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                               new_table: Optional[str] = None):
        try:
            old, new = await self._numeric_exprs(table, schema, old_col, new_col)
            expr = f"((1.0 * {new} - 1.0 * {old}) / NULLIF(ABS(1.0 * {old}), 0)) * 100"
            involved = [old_col, new_col]
            return await self._apply_expression(
                table, schema, involved, expr,
                target_col or self._generate_target_col(new_col, "pctch", old_col),
                "Percentage change", backend=backend, data_id=data_id, new_table=new_table
            )

        except Exception as e:
            return fail(f"percentage_change error: {e}")

    async def normalize_range(self, table, schema, column: Union[str, float, int],
                              target_col: Optional[str] = None, backend=None, data_id: Optional[str] = None,
                              new_table: Optional[str] = None):
        try:
            try:
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
            except Exception as e:
                return fail(f"normalize_range error: {str(e)}", [column])
        except Exception as e:
            return fail(f"normalize_range error: {e}")
