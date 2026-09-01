from memframe.core.analytix.arithmetic.base import ArithmeticOps


class PostgresArithmeticOps(ArithmeticOps):
    """PostgreSQL backend — dialect hooks only; all operations inherited."""

    def _numeric_text_cast(self, quoted_col: str) -> str:
        # ponytail: PG has no TRY_CAST — emulate it with a numeric-pattern
        # CASE so junk text becomes NULL instead of a syntax error
        return (
            "CAST(CASE WHEN TRIM((" + quoted_col + ")::TEXT) ~ "
            "'^[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?$' "
            "THEN TRIM((" + quoted_col + ")::TEXT) END AS DOUBLE PRECISION)"
        )

    def _fn_name(self, name: str) -> str:
        # PG spells base-10 log LOG, and needs NUMERIC casts for round/trunc
        return {"LOG10": "LOG"}.get(name, name)

    def _round_expr(self, col: str, digits) -> str:
        return f"ROUND(CAST({col} AS NUMERIC), {digits})"

    def _trunc_expr(self, col: str, digits) -> str:
        return f"TRUNC(CAST({col} AS NUMERIC), {digits})"
