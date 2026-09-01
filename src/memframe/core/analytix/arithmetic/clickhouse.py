from memframe.core.analytix.arithmetic.base import ArithmeticOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseArithmeticOps(ArithmeticOps):
    """ClickHouse backend.

    Dialect hooks (lowercase function names, pow/modulo aliases,
    toFloat64OrNull casts, ALTER TABLE UPDATE mutations) plus one structural
    override: column adds need IF NOT EXISTS and a Float64 type mapping.
    """

    def _numeric_text_cast(self, quoted_col: str) -> str:
        return f"toFloat64OrNull(TRIM({quoted_col}))"

    def _fn_name(self, name: str) -> str:
        # ponytail: CH's natural log is spelled `log` (ln is an alias, but the
        # pre-refactor SQL said log); MOD/POWER have different CH names
        return {"MOD": "modulo", "POWER": "pow", "LN": "log"}.get(name, name.lower())

    def _trunc_expr(self, col: str, digits) -> str:
        return f"truncate({col}, {digits})"

    def _update_stmt(self, qualified: str, set_expr: str) -> str:
        return f"ALTER TABLE {qualified} UPDATE {set_expr} WHERE 1"

    async def _add_column_if_not_exists(self, table, schema, column, data_type="DOUBLE PRECISION"):
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        data_type = "Float64" if data_type.upper() == "DOUBLE PRECISION" else data_type
        try:
            await self._exec(f"SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1")
        except Exception:
            await self._exec(
                f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS {self.db.quote_identifier(safe_col)} {data_type}"
            )
