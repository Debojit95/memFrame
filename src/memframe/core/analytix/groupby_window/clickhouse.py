"""
ClickHouse group‑by window operations.
MergeTree CTAS, row-number ordering fallback, and native aggregate
spellings for partitioned rolling / expanding windows.
"""

from __future__ import annotations

from memframe.core.analytix.groupby_window.base import GroupbyWindowOps
from memframe.core.analytix.window._ewm_hooks import ClickHouseEWMHooks


class ClickHouseGroupbyWindowOps(ClickHouseEWMHooks, GroupbyWindowOps):
    """ClickHouse backend — MergeTree CTAS with ordering fallback."""

    def _ch_agg_spec(self, raw: str) -> tuple[str, str] | None:
        """Normalize public and backend-specific aggregation aliases for ClickHouse."""
        key = raw.strip().lower()
        agg_map = {
            "sum": ("sum", "sum"),
            "avg": ("avg", "mean"),
            "mean": ("avg", "mean"),
            "min": ("min", "min"),
            "max": ("max", "max"),
            "count": ("count", "count"),
            "first": ("first_value_respect_nulls", "first"),
            "first_value": ("first_value_respect_nulls", "first"),
            "last": ("last_value_respect_nulls", "last"),
            "last_value": ("last_value_respect_nulls", "last"),
            "std": ("stddevSamp", "std"),
            "stddev": ("stddevSamp", "std"),
            "stddev_samp": ("stddevSamp", "std"),
            "stddevsamp": ("stddevSamp", "std"),
            "var": ("varSamp", "var"),
            "variance": ("varSamp", "var"),
            "var_samp": ("varSamp", "var"),
            "varsamp": ("varSamp", "var"),
        }
        return agg_map.get(key)

    def _window_agg_spec(self, raw: str):
        return self._ch_agg_spec(raw)

    def _std_agg(self) -> str:
        return "stddevSamp"

    def _var_agg(self) -> str:
        return "varSamp"

    def _avg_fn(self) -> str:
        return "avg"

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"quantile({q})({col_quoted})"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"quantile(0.5)({epoch_expr})"

    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"toUnixTimestamp({column_expr})"

    def _from_epoch_expr(self, is_date_only: bool) -> str:
        expr = "toDateTime(__agg.value_epoch)"
        return "toDate(__agg.value_epoch)" if is_date_only else expr

    def _distinct_count_sql(self, expr: str) -> str:
        return f"countDistinct({expr})"

