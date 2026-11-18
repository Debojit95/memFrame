"""
Group‑by window operations (rolling, expanding, ewm).
Partitioned window aggregations executed directly on the database.
Supports DuckDB, PostgreSQL, and ClickHouse.
"""

from __future__ import annotations
import traceback
from typing import Any, Dict, List, Optional, Union

from memframe.core.analytix.window import WindowOps
from memframe.core.analytix._generated import assert_owned_columns
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class GroupbyWindowOps(WindowOps):
    """
    Adds PARTITION BY support to all windowed aggregations.
    Supports DuckDB, PostgreSQL, and ClickHouse.
    """
    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for groupby window operation: {self.db.__class__.__name__}"
        )

    def _error_response(self, msg, **extra):
        # ponytail: the pre-split engine accepted extras (e.g. group_cols);
        # keep that shape on top of the narrowed base signature.
        resp = super()._error_response(msg)
        resp.update(extra)
        return resp

    def _sql_agg_spec(self, raw: str) -> tuple[str, str] | None:
        """Normalize public and backend-specific aggregation aliases for Postgres/DuckDB."""
        key = raw.strip().lower()
        agg_map = {
            "sum": ("SUM", "sum"),
            "avg": ("AVG", "mean"),
            "mean": ("AVG", "mean"),
            "min": ("MIN", "min"),
            "max": ("MAX", "max"),
            "count": ("COUNT", "count"),
            "first": ("FIRST_VALUE", "first"),
            "first_value": ("FIRST_VALUE", "first"),
            "last": ("LAST_VALUE", "last"),
            "last_value": ("LAST_VALUE", "last"),
        }
        if key in agg_map:
            return agg_map[key]
        if key in ("std", "stddev", "stddev_samp"):
            if isinstance(self.db, PostgresAdapter):
                return ("STDDEV", "std")
            if isinstance(self.db, DuckDBAdapter):
                return ("STDDEV_SAMP", "std")
            raise self._unsupported_backend_error()
        if key in ("var", "variance", "var_samp"):
            if isinstance(self.db, PostgresAdapter):
                return ("VARIANCE", "var")
            if isinstance(self.db, DuckDBAdapter):
                return ("VAR_SAMP", "var")
            raise self._unsupported_backend_error()
        return None

    def _window_agg_spec(self, raw: str):
        return self._sql_agg_spec(raw)

    def _window_result_type(self, source_type, alias_key, sql_agg) -> str:
        if alias_key == "count":
            return self._ewm_int_type()
        return self._window_result_sql_type(source_type, alias_key, sql_agg)

    def _group_base_cte(
        self, table, schema, safe_col, safe_groups, orders, name="__base",
        ord_expr=None,
    ) -> str:
        q = self.db.quote_identifier
        group_cols = ", ".join(q(c) for c in safe_groups)
        order_sql = ", ".join(q(c) for c in orders)
        qualified = self._qualified_table(table, schema)
        extra = f", {ord_expr} AS __ord" if ord_expr else ""
        return f"""{name} AS (
                SELECT {q("_mf_idx")}, {group_cols}, {q(safe_col)} AS v,
                       ROW_NUMBER() OVER (
                           PARTITION BY {group_cols}
                           ORDER BY {order_sql}
                       ) AS __rn{extra}
                FROM {qualified}
            )"""

    def _group_eq(self, safe_groups, left="r", right="b") -> str:
        q = self.db.quote_identifier
        return "".join(
            f" AND {left}.{q(c)} = {right}.{q(c)}" for c in safe_groups
        )

    async def _finish_grouped_special(
        self, table, schema, column, new_col, feature_sql, col_type,
        backend, data_id, safe_groups, order_by, message, **extra,
    ):
        collision = await assert_owned_columns(
            self, table, schema, [new_col], "groupby_win_map",
            backend, data_id,
        )
        if collision:
            return self._error_response(
                f"{message}: {collision}", group_cols=safe_groups,
            )
        await self._merge_features(
            table, schema, feature_sql, [new_col], [col_type],
            backend=backend, data_id=data_id,
        )
        preview_cols = [column, new_col]
        if order_by:
            preview_cols += [
                c for c in self._order_keys(order_by) if c != "_mf_idx"
            ]
        preview_cols += safe_groups
        preview_cols = list(dict.fromkeys(preview_cols))
        res = await self._preview_data(table, schema, preview_cols)
        response = self._success_response(
            message, res, new_table=table, new_column=new_col,
            group_cols=safe_groups, source_updated=True, **extra,
        )
        return response

    # ------------------------------------------------------------------
    #  ROLLING with GROUP BY – generic engine
    # ------------------------------------------------------------------
    async def _rolling_agg_with_partition(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_by: Union[str, List[str]] = None,
        window: int = 3,
        agg: Union[str, List[str]] = "SUM",
        backend=None,
        data_id=None,
        min_periods: Optional[int] = None,
        center: bool = False,
        closed: Optional[str] = None,
        ddof: int = 1,
        win_type: Optional[str] = None,
        new_table: str = None,
    ) -> Dict[str, Any]:
        """Generic rolling aggregation with PARTITION BY group_cols."""
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            kind, win_value, tag = self._parse_window(window)

            if isinstance(agg, str):
                raw_aggs = [agg]
            else:
                raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
            if not raw_aggs:
                return self._error_response("At least one aggregation function is required")

            min_p = self._resolve_min_periods(min_periods, window)

            if win_type:
                return await self._rolling_weighted(
                    table, schema, column, order_by, window, raw_aggs,
                    min_p, self._resolve_ddof(ddof), win_type, backend,
                    data_id, new_table, group_cols=safe_groups,
                    center=center, closed=closed,
                )

            ddof = self._resolve_ddof(ddof)
            if kind == "time":
                return await self._rolling_time_agg(
                    table, schema, column, order_by, window, raw_aggs,
                    min_p, self._resolve_ddof(ddof), backend, data_id,
                    new_table, group_cols=safe_groups, closed=closed,
                    center=center,
                )

            orders = self._order_keys(order_by)
            order_sql = ", ".join(
                self.db.quote_identifier(c) for c in orders
            )
            frame = self._rolling_frame(window, center, closed)
            q_col = self.db.quote_identifier(safe_col)
            part = self._ewm_partition(safe_groups)
            qualified = self._qualified_table(table, schema)
            source_type = await self._get_column_sql_type(table, schema, safe_col)
            count_over = (
                f"{self._count_expr(q_col)} "
                f"OVER ({part}ORDER BY {order_sql} {frame})"
            )
            rows_over = f"COUNT(1) OVER ({part}ORDER BY {order_sql} {frame})"
            suffix_group = "_".join(safe_groups)

            new_cols: List[str] = []
            exprs: List[str] = []
            col_types: List[str] = []
            used_col_names = set()
            for raw in raw_aggs:
                spec = self._window_agg_spec(raw)
                if spec is None:
                    return self._error_response(
                        f"Unsupported rolling agg '{raw}' for groupby"
                    )
                sql_agg, alias_key = spec

                base_col = f"{safe_col}_rolling_{alias_key}_w{tag}_by_{suffix_group}"
                col_name = base_col
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                agg_call = (
                    self._count_expr(q_col)
                    if alias_key == "count"
                    else f"{sql_agg}({q_col})"
                )
                if alias_key == "std":
                    value_sql = self._std_over(
                        q_col, order_sql, frame, part, ddof,
                    )
                elif alias_key == "var":
                    value_sql = self._var_over(
                        q_col, order_sql, frame, part, ddof,
                    )
                else:
                    value_sql = (
                        f"{agg_call} OVER ({part}ORDER BY {order_sql} {frame})"
                    )
                gate_count = rows_over if alias_key == "count" else count_over
                gate_at = min_p if (alias_key == "count" or min_p > 1) else 0
                value_sql = self._min_periods_gate(
                    value_sql, gate_count, gate_at,
                )
                if alias_key == "sum" and min_p == 0:
                    value_sql = f"COALESCE({value_sql}, 0)"
                exprs.append(
                    f"{value_sql} AS {self.db.quote_identifier(col_name)}"
                )
                col_types.append(
                    self._window_result_type(source_type, alias_key, sql_agg)
                )

            collision = await assert_owned_columns(
                self, table, schema, new_cols, "groupby_win_map",
                backend, data_id,
            )
            if collision:
                return self._error_response(
                    f"Rolling group-by error: {collision}",
                    group_cols=safe_groups,
                )

            select_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                + ", ".join(exprs)
                + f" FROM {qualified}"
            )
            await self._merge_features(
                table, schema, select_sql, new_cols, col_types,
                backend=backend, data_id=data_id,
            )

            preview_cols = [column, *new_cols]
            if order_by:
                preview_cols += [c for c in orders if c != "_mf_idx"]
            preview_cols += safe_groups
            preview_cols = list(dict.fromkeys(preview_cols))
            res = await self._preview_data(table, schema, preview_cols)

            response = self._success_response(
                f"Rolling {', '.join(raw_aggs)} on '{column}' "
                f"grouped by {safe_groups} (window={window})",
                result=res,
                new_table=table,
                new_columns=new_cols,
                window=window,
                min_periods=min_p,
                group_cols=safe_groups,
                source_updated=True,
            )
            if len(new_cols) == 1:
                response["new_column"] = new_cols[0]
            return response
        except Exception as e:
            return self._error_response(
                f"rolling group-by error: {str(e)}\n{traceback.format_exc()}"
            )

    # --------------------------------------------------
    #  ROLLING GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def rolling_sum_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="SUM", **kwargs)
    async def rolling_mean_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="AVG", **kwargs)
    async def rolling_min_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MIN", **kwargs)
    async def rolling_max_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MAX", **kwargs)
    async def rolling_count_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="COUNT", **kwargs)
    async def rolling_std_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter):
            func = "STDDEV"
        elif isinstance(self.db, DuckDBAdapter):
            func = "STDDEV_SAMP"
        else:
            raise self._unsupported_backend_error()
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg=func, **kwargs)
    async def rolling_var_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter):
            func = "VARIANCE"
        elif isinstance(self.db, DuckDBAdapter):
            func = "VAR_SAMP"
        else:
            raise self._unsupported_backend_error()
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg=func, **kwargs)
    async def rolling_first_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="FIRST_VALUE", **kwargs)
    async def rolling_last_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="LAST_VALUE", **kwargs)
    # ---------- Rolling Specials ----------
    async def rolling_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3,
        q=0.5, backend=None, data_id=None, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None,
        label: Optional[str] = None, interpolation: str = "linear",
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            self._resolve_interpolation(interpolation)

            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            pred = self._frame_predicate(spec)
            frame = (
                self._rolling_frame(window, center, closed)
                if spec["kind"] == "rows" else None
            )
            tag = label or f"q{q}"
            new_col = f"{safe_col}_rolling_{tag}_w{win_tag}_by_{'_'.join(safe_groups)}"

            if isinstance(self.db, DuckDBAdapter) and spec["kind"] == "rows":
                count_over = (
                    f"{self._count_expr(q_col)} "
                    f"OVER ({part}ORDER BY {order_sql} {frame})"
                )
                agg_over = (
                    f"{self._quantile_cont_sql(q_col, q)} "
                    f"OVER ({part}ORDER BY {order_sql} {frame})"
                )
                expr = self._min_periods_gate(agg_over, count_over, min_p)
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                ord_expr = (
                    self._epoch_expr(self.db.quote_identifier(orders[0]))
                    if spec["kind"] == "time" else None
                )
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                    ord_expr=ord_expr,
                )
                group_eq = self._group_eq(safe_groups)
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE {pred}
                      {group_eq}
                """
                quantile_subq = f"""
                    SELECT {self._quantile_cont_sql("r.v", q)}
                    FROM __base r
                    WHERE {pred}
                      {group_eq}
                      AND r.v IS NOT NULL
                """
                expr = self._min_periods_gate(
                    f"({quantile_subq})", f"({count_subq})", min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """

            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id,
                safe_groups, order_by,
                f"Rolling quantile on '{column}' grouped by {safe_groups} "
                f"(window={window}, q={q}, min_periods={min_p})",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(f"rolling quantile group-by error: {str(e)}")

    async def rolling_median_groupby(self, *args, **kwargs):
        kwargs.setdefault("q", 0.5)
        kwargs.setdefault("label", "median")
        return await self.rolling_quantile_groupby(*args, **kwargs)

    async def rolling_sem_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3,
        backend=None, data_id=None, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None, ddof: int = 1,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            ddof = self._resolve_ddof(ddof)

            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            frame = (
                self._rolling_frame(window, center, closed)
                if spec["kind"] == "rows" else None
            )
            new_col = f"{safe_col}_rolling_sem_w{win_tag}_by_{'_'.join(safe_groups)}"
            if spec["kind"] == "time":
                ord_expr = self._epoch_expr(
                    self.db.quote_identifier(orders[0])
                )
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                    ord_expr=ord_expr,
                )
                pred = (
                    f"{self._frame_predicate(spec)}"
                    f"{self._group_eq(safe_groups)}"
                )
                n_sql = f"(SELECT COUNT(r.v) FROM __base r WHERE {pred})"
                std_sql = (
                    f"(SELECT {self._std_samp_agg()}(r.v) FROM __base r "
                    f"WHERE {pred})"
                )
                denom = (
                    f"CASE WHEN {n_sql} > {ddof} "
                    f"THEN SQRT({n_sql} - {ddof}) ELSE NULL END"
                )
                expr = self._min_periods_gate(
                    f"({std_sql} / {denom})", n_sql, min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
                return await self._finish_grouped_special(
                    table, schema, column, new_col, feature_sql,
                    self._ewm_float_type(), backend, data_id,
                    safe_groups, order_by,
                    f"Rolling SEM on '{column}' grouped by {safe_groups}",
                    window=window, min_periods=min_p,
                )
            count_over = (
                f"{self._count_expr(q_col)} "
                f"OVER ({part}ORDER BY {order_sql} {frame})"
            )
            sem_sql = self._sem_over(q_col, order_sql, frame, part, ddof)
            expr = self._min_periods_gate(sem_sql, count_over, min_p)
            feature_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                f"{expr} AS {self.db.quote_identifier(new_col)} "
                f"FROM {self._qualified_table(table, schema)}"
            )
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id,
                safe_groups, order_by,
                f"Rolling SEM on '{column}' grouped by {safe_groups} "
                f"(window={window}, min_periods={min_p})",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(f"rolling sem group-by error: {str(e)}")

    async def rolling_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3,
        backend=None, data_id=None, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)

            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            pred = self._frame_predicate(spec)
            frame = (
                self._rolling_frame(window, center, closed)
                if spec["kind"] == "rows" else None
            )
            new_col = f"{safe_col}_rolling_nunique_w{win_tag}_by_{'_'.join(safe_groups)}"

            if isinstance(self.db, DuckDBAdapter) and spec["kind"] == "rows":
                count_over = (
                    f"{self._count_expr(q_col)} "
                    f"OVER ({part}ORDER BY {order_sql} {frame})"
                )
                distinct_over = (
                    f"COUNT(DISTINCT {q_col}) "
                    f"OVER ({part}ORDER BY {order_sql} {frame})"
                )
                expr = self._min_periods_gate(
                    distinct_over, count_over, min_p,
                )
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                ord_expr = (
                    self._epoch_expr(self.db.quote_identifier(orders[0]))
                    if spec["kind"] == "time" else None
                )
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                    ord_expr=ord_expr,
                )
                group_eq = self._group_eq(safe_groups)
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE {pred}
                      {group_eq}
                """
                distinct_subq = f"""
                    SELECT {self._distinct_count_sql("r.v")}
                    FROM __base r
                    WHERE {pred}
                      {group_eq}
                """
                expr = self._min_periods_gate(
                    f"({distinct_subq})", f"({count_subq})", min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """

            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_int_type(), backend, data_id,
                safe_groups, order_by,
                f"Rolling nunique on '{column}' grouped by {safe_groups} "
                f"(window={window}, min_periods={min_p})",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(f"rolling nunique group-by error: {str(e)}")

    async def rolling_rank_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3,
        backend=None, data_id=None, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None,
        method: str = "average", ascending: bool = True, pct: bool = False,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)

            orders = self._order_keys(order_by)
            pred = self._frame_predicate(spec)
            new_col = f"{safe_col}_rolling_rank_w{win_tag}_by_{'_'.join(safe_groups)}"
            base = self._group_base_cte(
                table, schema, safe_col, safe_groups, orders,
            )
            group_eq = self._group_eq(safe_groups)
            frame_where = f"{pred}{group_eq}"
            count_sql = (
                f"(SELECT COUNT(r.v) FROM __base r WHERE {frame_where})"
            )
            rank_expr = self._rank_sql(
                frame_where, count_sql, method, ascending, pct,
            )
            expr = self._min_periods_gate(rank_expr, count_sql, min_p)
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._rank_type(method, pct), backend, data_id,
                safe_groups, order_by,
                f"Rolling rank on '{column}' grouped by {safe_groups} "
                f"(window={window}, min_periods={min_p})",
                window=window, min_periods=min_p,
                rank_method=method, ascending=ascending, pct=pct,
            )
        except Exception as e:
            return self._error_response(f"rolling rank group-by error: {str(e)}")

    # ---------- Rolling Datetime Group‑By ----------
    async def rolling_min_datetime_groupby(self, *args, **kwargs):
        return await self._rolling_agg_with_partition(*args, agg="MIN", **kwargs)
    async def rolling_max_datetime_groupby(self, *args, **kwargs):
        return await self._rolling_agg_with_partition(*args, agg="MAX", **kwargs)
    async def rolling_mean_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None, min_periods=None, center=False, closed=None, win_type=None,
    ):
        return await self._rolling_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, window, stat="mean", backend=backend, data_id=data_id, new_table=new_table, min_periods=min_periods, center=center, closed=closed, win_type=win_type,
        )
    async def rolling_median_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None, min_periods=None, center=False, closed=None, win_type=None,
    ):
        return await self._rolling_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, window, stat="median", backend=backend, data_id=data_id, new_table=new_table, min_periods=min_periods, center=center, closed=closed, win_type=win_type,
        )
    async def rolling_mode_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None, min_periods=None, center=False, closed=None, win_type=None,
    ):
        return await self._rolling_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, window, stat="mode", backend=backend, data_id=data_id, new_table=new_table, min_periods=min_periods, center=center, closed=closed, win_type=win_type,
        )
    async def _rolling_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, window, stat,
        backend, data_id, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)

            orders = self._order_keys(order_by)
            pred = self._frame_predicate(spec)
            stat_key = stat.lower()
            new_col = (
                f"{safe_col}_rolling_{stat_key}_w{win_tag}_by_"
                f"{'_'.join(safe_groups)}"
            )
            base = self._group_base_cte(
                table, schema, safe_col, safe_groups, orders,
            )
            group_eq = self._group_eq(safe_groups)
            is_date_only = await self._is_date_only_column(
                table, schema, safe_col,
            )
            from_epoch = self._from_epoch_expr(is_date_only)
            if stat_key == "mean":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._avg_fn() + f"({epoch_expr})"
                value_sql = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE {pred}
                          {group_eq}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._median_epoch_sql(epoch_expr)
                value_sql = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE {pred}
                          {group_eq}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "mode":
                if isinstance(self.db, ClickHouseAdapter):
                    value_sql = f"""
                        SELECT argMin(v, (-cnt, v))
                        FROM (
                            SELECT r.v AS v, COUNT(*) AS cnt
                            FROM __base r
                            WHERE {pred}
                              {group_eq}
                              AND r.v IS NOT NULL
                            GROUP BY r.v
                        )
                    """
                else:
                    value_sql = f"""
                        SELECT r.v
                        FROM __base r
                        WHERE {pred}
                          {group_eq}
                          AND r.v IS NOT NULL
                        GROUP BY r.v
                        ORDER BY COUNT(*) DESC, r.v
                        LIMIT 1
                    """
            else:
                return self._error_response(
                    f"Unsupported datetime rolling stat '{stat}'"
                )

            count_subq = f"""
                SELECT COUNT(r.v) FROM __base r
                WHERE {pred}
                  {group_eq}
            """
            expr = self._min_periods_gate(
                f"({value_sql})", f"({count_subq})", min_p,
            )
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            source_type = await self._get_column_sql_type(
                table, schema, safe_col,
            ) or self._ewm_float_type()
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                source_type, backend, data_id, safe_groups, order_by,
                f"Rolling datetime {stat_key} on '{column}' "
                f"grouped by {safe_groups} "
                f"(window={window}, min_periods={min_p})",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(
                f"rolling datetime group-by error: {str(e)}\n{traceback.format_exc()}"
            )

    # ------------------------------------------------------------------
    #  EXPANDING with GROUP BY – generic engine
    # ------------------------------------------------------------------
    async def _expanding_agg_with_partition(
        self,
        table, schema, column, group_cols,
        order_by=None, agg="SUM", min_periods=1, ddof=1,
        backend=None, data_id=None, new_table=None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            if isinstance(agg, str):
                raw_aggs = [agg]
            else:
                raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
            if not raw_aggs:
                return self._error_response("At least one aggregation function is required")

            ddof = self._resolve_ddof(ddof)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            qualified = self._qualified_table(table, schema)
            q_col = self.db.quote_identifier(safe_col)
            source_type = await self._get_column_sql_type(table, schema, safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            suffix_group = "_".join(safe_groups)

            count_over = (
                f"{self._count_expr(q_col)} "
                f"OVER ({part}ORDER BY {order_sql} {window_frame})"
            )
            new_cols: List[str] = []
            exprs: List[str] = []
            col_types: List[str] = []
            used_col_names = set()
            for raw in raw_aggs:
                spec = self._window_agg_spec(raw)
                if spec is None:
                    return self._error_response(
                        f"Unsupported expanding aggregation '{raw}'"
                    )
                sql_agg, alias_key = spec

                base_col = f"{safe_col}_expanding_{alias_key}_by_{suffix_group}"
                col_name = base_col
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                agg_call = (
                    self._count_expr(q_col)
                    if alias_key == "count"
                    else f"{sql_agg}({q_col})"
                )
                if alias_key == "std":
                    agg_over = self._std_over(
                        q_col, order_sql, window_frame, part, ddof,
                    )
                elif alias_key == "var":
                    agg_over = self._var_over(
                        q_col, order_sql, window_frame, part, ddof,
                    )
                else:
                    agg_over = (
                        f"{agg_call} OVER ({part}ORDER BY {order_sql} {window_frame})"
                    )
                if min_periods > 1:
                    agg_with_null = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {agg_over} ELSE NULL END"
                    )
                else:
                    agg_with_null = agg_over
                exprs.append(
                    f"{agg_with_null} AS {self.db.quote_identifier(col_name)}"
                )
                col_types.append(
                    self._window_result_type(source_type, alias_key, sql_agg)
                )

            collision = await assert_owned_columns(
                self, table, schema, new_cols, "groupby_win_map",
                backend, data_id,
            )
            if collision:
                return self._error_response(
                    f"Expanding group-by error: {collision}",
                    group_cols=safe_groups,
                )

            select_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                + ", ".join(exprs)
                + f" FROM {qualified}"
            )
            await self._merge_features(
                table, schema, select_sql, new_cols, col_types,
                backend=backend, data_id=data_id,
            )

            preview_cols = [column, *new_cols]
            if order_by:
                preview_cols += [c for c in orders if c != "_mf_idx"]
            preview_cols += safe_groups
            preview_cols = list(dict.fromkeys(preview_cols))
            res = await self._preview_data(table, schema, preview_cols)

            response = self._success_response(
                f"Expanding {', '.join(raw_aggs)} on '{column}' "
                f"grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=table,
                new_columns=new_cols,
                min_periods=min_periods,
                group_cols=safe_groups,
                source_updated=True,
            )
            if len(new_cols) == 1:
                response["new_column"] = new_cols[0]
            return response
        except Exception as e:
            return self._error_response(
                f"expanding group-by error: {str(e)}\n{traceback.format_exc()}"
            )

    # --------------------------------------------------
    #  EXPANDING GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def expanding_sum_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="SUM", **kwargs)
    async def expanding_mean_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="AVG", **kwargs)
    async def expanding_min_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
    async def expanding_max_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
    async def expanding_count_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="COUNT", **kwargs)
    async def expanding_std_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter):
            func = "STDDEV"
        elif isinstance(self.db, DuckDBAdapter):
            func = "STDDEV_SAMP"
        else:
            raise self._unsupported_backend_error()
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg=func, **kwargs)
    async def expanding_var_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter):
            func = "VARIANCE"
        elif isinstance(self.db, DuckDBAdapter):
            func = "VAR_SAMP"
        else:
            raise self._unsupported_backend_error()
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg=func, **kwargs)
    async def expanding_first_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="FIRST_VALUE", **kwargs)
    async def expanding_last_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="LAST_VALUE", **kwargs)
    # ---------- Expanding Specials ----------
    async def expanding_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, q=0.5,
        min_periods=1, backend=None, data_id=None, new_table=None,
        label: Optional[str] = None, interpolation: str = "linear",
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            self._resolve_interpolation(interpolation)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            new_col = (
                f"{safe_col}_expanding_{label or f'q{q}'}_by_"
                f"{'_'.join(safe_groups)}"
            )
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

            if isinstance(self.db, DuckDBAdapter):
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"({part}ORDER BY {order_sql} {frame})"
                )
                agg_over = (
                    f"{self._quantile_cont_sql(q_col, q)} OVER "
                    f"({part}ORDER BY {order_sql} {frame})"
                )
                if min_periods > 1:
                    quantile_expr = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {agg_over} ELSE NULL END"
                    )
                else:
                    quantile_expr = agg_over
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{quantile_expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                )
                group_eq = self._group_eq(safe_groups)
                quantile_subq = f"""
                    SELECT {self._quantile_cont_sql("r.v", q)}
                    FROM __base r
                    WHERE r.__rn <= b.__rn
                      {group_eq}
                      AND r.v IS NOT NULL
                """
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE r.__rn <= b.__rn
                      {group_eq}
                """
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({quantile_subq}) ELSE NULL END"
                    )
                else:
                    expr = f"({quantile_subq})"
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, safe_groups, order_by,
                f"Expanding quantile on '{column}' grouped by {safe_groups} (q={q})",
                min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(f"expanding quantile group-by error: {str(e)}")

    async def expanding_median_groupby(self, *args, **kwargs):
        kwargs.setdefault("q", 0.5)
        kwargs.setdefault("label", "median")
        return await self.expanding_quantile_groupby(*args, **kwargs)

    async def expanding_sem_groupby(
        self, table, schema, column, group_cols, order_by=None,
        min_periods=1, ddof=1, backend=None, data_id=None, new_table=None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            ddof = self._resolve_ddof(ddof)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_expanding_sem_by_{'_'.join(safe_groups)}"
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = (
                f"{self._count_expr(q_col)} OVER "
                f"({part}ORDER BY {order_sql} {frame})"
            )
            sem_base = self._sem_over(q_col, order_sql, frame, part, ddof)
            if min_periods > 1:
                sem_expr = (
                    f"CASE WHEN {count_over} >= {min_periods} "
                    f"THEN {sem_base} ELSE NULL END"
                )
            else:
                sem_expr = sem_base
            feature_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                f"{sem_expr} AS {self.db.quote_identifier(new_col)} "
                f"FROM {self._qualified_table(table, schema)}"
            )
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id,
                safe_groups, order_by,
                f"Expanding SEM on '{column}' grouped by {safe_groups} "
                f"(min_periods={min_periods})",
                min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(f"expanding sem group-by error: {str(e)}")

    async def expanding_rank_groupby(
        self, table, schema, column, group_cols, order_by=None,
        min_periods=1, backend=None, data_id=None, new_table=None,
        method: str = "average", ascending: bool = True, pct: bool = False,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            orders = self._order_keys(order_by)
            new_col = f"{safe_col}_expanding_rank_by_{'_'.join(safe_groups)}"
            base = self._group_base_cte(
                table, schema, safe_col, safe_groups, orders,
            )
            group_eq = self._group_eq(safe_groups)
            frame_where = f"r.__rn <= b.__rn{group_eq}"
            count_sql = (
                f"(SELECT COUNT(r.v) FROM __base r WHERE {frame_where})"
            )
            rank_expr = self._rank_sql(
                frame_where, count_sql, method, ascending, pct,
            )
            expr = self._min_periods_gate(
                rank_expr, count_sql, int(min_periods or 0),
            )
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._rank_type(method, pct), backend, data_id,
                safe_groups, order_by,
                f"Expanding rank on '{column}' grouped by {safe_groups} "
                f"(min_periods={min_periods})",
                min_periods=min_periods,
                rank_method=method, ascending=ascending, pct=pct,
            )
        except Exception as e:
            return self._error_response(f"expanding rank group-by error: {str(e)}")

    async def expanding_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None,
        min_periods=1, backend=None, data_id=None, new_table=None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            part = self._ewm_partition(safe_groups)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_expanding_nunique_by_{'_'.join(safe_groups)}"
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

            if isinstance(self.db, DuckDBAdapter):
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"({part}ORDER BY {order_sql} {frame})"
                )
                count_distinct = (
                    f"COUNT(DISTINCT {q_col}) OVER "
                    f"({part}ORDER BY {order_sql} {frame})"
                )
                if min_periods > 1:
                    nunique_expr = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {count_distinct} ELSE NULL END"
                    )
                else:
                    nunique_expr = count_distinct
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{nunique_expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                )
                group_eq = self._group_eq(safe_groups)
                distinct_subq = f"""
                    SELECT {self._distinct_count_sql("r.v")}
                    FROM __base r
                    WHERE r.__rn <= b.__rn
                      {group_eq}
                """
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE r.__rn <= b.__rn
                      {group_eq}
                """
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({distinct_subq}) ELSE NULL END"
                    )
                else:
                    expr = f"({distinct_subq})"
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_int_type(), backend, data_id, safe_groups, order_by,
                f"Expanding nunique on '{column}' grouped by {safe_groups} "
                f"(min_periods={min_periods})",
                min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(f"expanding nunique group-by error: {str(e)}")

    # --------------------------------------------------
    #  EXPANDING DATETIME GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def expanding_min_datetime_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
    async def expanding_max_datetime_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
    async def expanding_mean_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
    ):
        return await self._expanding_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, stat="mean", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
        )
    async def expanding_median_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
    ):
        return await self._expanding_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, stat="median", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
        )
    async def expanding_mode_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
    ):
        return await self._expanding_datetime_stat_with_partition(
            table, schema, column, group_cols, order_by, stat="mode", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
        )
    async def _expanding_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, stat, min_periods,
        backend, data_id, new_table=None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            orders = self._order_keys(order_by)
            stat_key = stat.lower()
            new_col = (
                f"{safe_col}_expanding_{stat_key}_by_{'_'.join(safe_groups)}"
            )
            base = self._group_base_cte(
                table, schema, safe_col, safe_groups, orders,
            )
            group_eq = self._group_eq(safe_groups)
            is_date_only = await self._is_date_only_column(
                table, schema, safe_col,
            )
            from_epoch = self._from_epoch_expr(is_date_only)
            if stat_key == "mean":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._avg_fn() + f"({epoch_expr})"
                value_subq = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          {group_eq}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._median_epoch_sql(epoch_expr)
                value_subq = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          {group_eq}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "mode":
                if isinstance(self.db, ClickHouseAdapter):
                    mode_subq = f"""
                        SELECT argMin(v, (-cnt, v))
                        FROM (
                            SELECT r.v AS v, COUNT(*) AS cnt
                            FROM __base r
                            WHERE r.__rn <= b.__rn
                              {group_eq}
                              AND r.v IS NOT NULL
                            GROUP BY r.v
                        )
                    """
                else:
                    mode_subq = f"""
                        SELECT r.v
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          {group_eq}
                          AND r.v IS NOT NULL
                        GROUP BY r.v
                        ORDER BY COUNT(*) DESC, r.v
                        LIMIT 1
                    """
            else:
                return self._error_response(
                    f"Unsupported datetime expanding stat '{stat}'"
                )
            count_subq = f"""
                SELECT COUNT(r.v) FROM __base r
                WHERE r.__rn <= b.__rn
                  {group_eq}
            """
            if stat_key == "mode":
                if min_periods > 1:
                    value_sql = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({mode_subq}) ELSE NULL END"
                    )
                else:
                    value_sql = f"({mode_subq})"
            else:
                if min_periods > 1:
                    value_sql = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({value_subq}) ELSE NULL END"
                    )
                else:
                    value_sql = f"({value_subq})"

            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {value_sql} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            source_type = await self._get_column_sql_type(
                table, schema, safe_col,
            ) or self._ewm_float_type()
            return await self._finish_grouped_special(
                table, schema, column, new_col, feature_sql,
                source_type, backend, data_id, safe_groups, order_by,
                f"Expanding datetime {stat_key} on '{column}' "
                f"grouped by {safe_groups} (min_periods={min_periods})",
                min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(
                f"expanding datetime group-by error: {str(e)}\n{traceback.format_exc()}"
            )

    # ------------------------------------------------------------------
    #  EWM with GROUP BY
    # ------------------------------------------------------------------
    async def ewm_groupby(
        self,
        table, schema, column, group_cols,
        order_by=None,
        com=None, span=None, halflife=None, alpha=None,
        adjust=True, ignore_na=False, min_periods=0,
        agg="mean",
        backend=None, data_id=None, new_table=None,
    ):
        groups = (
            list(group_cols)
            if isinstance(group_cols, (list, tuple))
            else [group_cols]
        )
        if not groups:
            return self._error_response("group_cols is required")
        return await self._ewm_run(
            table, schema, column, groups,
            order_by=order_by, com=com, span=span, halflife=halflife,
            alpha=alpha, adjust=adjust, ignore_na=ignore_na,
            min_periods=min_periods, agg=agg,
            backend=backend, data_id=data_id,
            op_type="groupby_win_map",
        )
