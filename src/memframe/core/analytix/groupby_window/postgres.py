from __future__ import annotations

from memframe.core.analytix.groupby_window.base import GroupbyWindowOps
from memframe.core.analytix.window._ewm_hooks import PostgresEWMHooks


class PostgresGroupbyWindowOps(PostgresEWMHooks, GroupbyWindowOps):
    """PostgreSQL backend — dialect EWM/window hooks; all else inherited."""

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {col_quoted})"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})"

    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"EXTRACT(EPOCH FROM {column_expr})"

    @property
    def _row_id_col(self) -> str:
        return "_mf_idx"
