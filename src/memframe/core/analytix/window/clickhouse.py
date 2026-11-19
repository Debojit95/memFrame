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

    def _epoch_expr(self, column_expr: str) -> str:
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

    def _distinct_count_sql(self, expr: str) -> str:
        return f"countDistinct({expr})"

    def _center_anchor(self, alias="r", base="b") -> str:
        # ClickHouse mis-plans correlated scans whose only predicates compare
        # a non-unique column (duplicated rows); anchor on the unique __rn.
        return f" AND {alias}.__rn >= {base}.__rn - 1000000000"

    def _ordered_value_sql(self, where, mode, value_expr="r.v") -> str:
        idx = 1 if mode == "first" else -1
        return (
            f"(SELECT arrayElement(arraySort(x -> x.1, "
            f"groupArray((r.__rn, {value_expr}))), {idx}).2 "
            f"FROM __base r WHERE {where})"
        )
