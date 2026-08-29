from __future__ import annotations

from typing import Optional, Union

from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok
from memframe.core.analytix.arithmetic.base import ArithmeticOps


class DuckDBArithmeticOps(ArithmeticOps):
    # ------------------------------------------------------------------
    # Divergent helpers (backend‑specific)
    # ------------------------------------------------------------------
    def _numeric_text_cast(self, quoted_col: str) -> str:
        return f"TRY_CAST({quoted_col} AS DOUBLE)"

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
            f'UPDATE {qualified} SET {self.db.quote_identifier(tgt_safe)} = {expression}'
        )
        await self._add_column_if_not_exists(table, schema, tgt_safe)
        original_qualified = self._qualified_table(table, schema)
        await self._exec(
            f'UPDATE {original_qualified} SET {self.db.quote_identifier(tgt_safe)} = {expression}'
        )
        cols_to_fetch = list(set(involved_cols)) + [target_col]
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
    # Operational methods
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
            expr = f"MOD({left}, {right})"
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
            expr = f"POWER({left}, {right})"
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
            expr = f"ABS({col})"
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
            expr = f"ROUND({col}, {digits})"
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
            expr = f"CEIL({col})"
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
            expr = f"FLOOR({col})"
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
            expr = f"TRUNC({col}, {digits})"
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
            expr = f"EXP({col})"
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
            expr = f"CASE WHEN {col} > 0 THEN LN({col}) ELSE NULL END"
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
            expr = f"CASE WHEN {col} > 0 THEN LOG10({col}) ELSE NULL END"
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
            expr = f"CASE WHEN {col} >= 0 THEN SQRT({col}) ELSE NULL END"
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
            expr = f"SIN({col})"
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
            expr = f"COS({col})"
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
            expr = f"TAN({col})"
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
            expr = f"ASIN({col})"
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
            expr = f"ACOS({col})"
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
            expr = f"ATAN({col})"
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
            expr = f"ATAN2({left}, {right})"
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
