from memframe.core.analytix.window.base import WindowOps
from memframe.core.analytix.window._ewm_hooks import ClickHouseEWMHooks


class ClickHouseWindowOps(ClickHouseEWMHooks, WindowOps):
    """ClickHouse window engine.

    ClickHouse computes each window inline in a single ``CREATE TABLE AS``
    (asynchronous ``UPDATE`` mutations can return stale data), so the
    operations that the shared engine performs via ``ADD COLUMN`` + ``UPDATE``
    are overridden here, alongside the dialect hooks.
    """

    # --------------------------------------------------
    # Dialect hooks
    # --------------------------------------------------
    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"toUnixTimestamp({column_expr})"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"quantile(0.5)({epoch_expr})"

    def _from_epoch_expr(self, is_date_only: bool) -> str:
        expr = "toDateTime(__agg.value_epoch)"
        return "toDate(__agg.value_epoch)" if is_date_only else expr

    def _avg_fn(self) -> str:
        return "avg"

    def _count_fn(self) -> str:
        return "count"

    def _std_agg(self) -> str:
        return "stddevSamp"

    def _var_agg(self) -> str:
        return "varSamp"

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"quantile({q})({col_quoted})"

    def _first_value_fn(self) -> str:
        return "first_value_respect_nulls"

    def _last_value_fn(self) -> str:
        return "last_value_respect_nulls"

    def _distinct_count_sql(self, expr: str) -> str:
        return f"countDistinct({expr})"
