from memframe.core.analytix.window.base import WindowOps
from memframe.core.analytix.window._ewm_hooks import PostgresEWMHooks


class PostgresWindowOps(PostgresEWMHooks, WindowOps):
    """PostgreSQL window engine — dialect and EWM hooks."""

    # --------------------------------------------------
    # Dialect hooks
    # --------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        return "_mf_idx"

    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"EXTRACT(EPOCH FROM {column_expr})"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})"

    def _std_agg(self) -> str:
        return "STDDEV"

    def _var_agg(self) -> str:
        return "VARIANCE"

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {col_quoted})"
