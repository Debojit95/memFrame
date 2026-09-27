"""
ClickHouse group‑by window operations.
MergeTree CTAS, row-number ordering fallback, and native aggregate
spellings for partitioned rolling / expanding windows.
"""

from __future__ import annotations
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Union
import pandas as pd
import numpy as np

from memframe.core.analytix.groupby_window.base import GroupbyWindowOps
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseGroupbyWindowOps(GroupbyWindowOps):
    """ClickHouse backend — MergeTree CTAS with ordering fallback."""

    # Internal row-number column used for ClickHouse ordering fallback
    _CH_ROW_NUM = "_mf_row_num"

    def _ch_datetime_epoch_expr(self, col_expr: str) -> str:
        """Extract Unix epoch (seconds) from a datetime column in ClickHouse."""
        return f"toUnixTimestamp({col_expr})"

    def _ch_from_epoch(self, epoch_expr: str, is_date_only: bool) -> str:
        """Convert epoch seconds back to DateTime / Date for ClickHouse."""
        ts = f"fromUnixTimestamp({epoch_expr})"
        return f"toDate({ts})" if is_date_only else ts

    def _ch_source_with_rownum(self, qualified_table: str) -> str:
        """Return a subquery that adds _mf_row_num for ordering fallback."""
        rn = self._CH_ROW_NUM
        return (
            f"(SELECT *, ROW_NUMBER() OVER() AS {rn} "
            f"FROM {qualified_table}) AS __source"
        )

    def _ch_create_table_as(self, output_qualified: str, select_sql: str) -> str:
        """ClickHouse CREATE TABLE AS with required ENGINE clause."""
        return (
            f"CREATE TABLE {output_qualified} "
            f"ENGINE = MergeTree() ORDER BY tuple() AS\n{select_sql}"
        )

    # ------------------------------------------------------------------
    # map_feature helpers: LEFT JOIN the new feature column(s) back onto the
    # original table on full row identity (one output row per input row).
    # Marker rows in the transient registry tell our own re-runs apart
    # from foreign column collisions.
    # ------------------------------------------------------------------
    async def _projection_sql(
        self,
        table: str,
        schema: str,
        prefix: str = "",
        exclude: set[str] | None = None,
    ) -> str:
        """Return an explicit projection for source columns, excluding internal row ids."""
        exclude = exclude or set()
        internal_cols = {self._CH_ROW_NUM, "__rn"} | exclude
        column_types = await self.db.get_column_types(table, schema)
        columns = [
            SQLIdentifierSanitizer.sanitize(c)
            for c in column_types.keys()
            if c not in internal_cols
        ]
        if not columns:
            return "*"
        qualifier = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{qualifier}{self.db.quote_identifier(c)}"
            for c in columns
        )

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
            "first": ("first_value", "first"),
            "first_value": ("first_value", "first"),
            "last": ("last_value", "last"),
            "last_value": ("last_value", "last"),
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
        new_table: str = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Generic rolling aggregation with PARTITION BY group_cols."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING — unchanged)
            # ============================================================
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

            # ------ Resolve ordering ------
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            agg_specs = []
            for raw in raw_aggs:
                spec = self._ch_agg_spec(raw)
                if spec is None:
                    return self._error_response(f"Unsupported rolling agg '{raw}' for groupby")
                sql_agg, alias_key = spec
                agg_specs.append((sql_agg, alias_key))

            # ------ Build window expressions ------
            new_cols = []
            used_col_names = set()
            window_exprs = []
            suffix_group = "_".join(safe_groups)

            for sql_agg, alias_key in agg_specs:
                base_col = f"{safe_col}_rolling_{alias_key}_w{window}_by_{suffix_group}"
                col_name = base_col
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                window_exprs.append(
                    f"""
                    {sql_agg}({self.db.quote_identifier(safe_col)})
                    OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {order_sql}
                        ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                    ) AS {self.db.quote_identifier(col_name)}
                    """
                )

            window_sql = ", ".join(window_exprs)
            output_qualified = self._qualified_table(new_table_name, schema)
            source_projection = await self._projection_sql(table, schema)
            select_sql = (
                f"SELECT {source_projection},\n"
                f"    {window_sql}\n"
                f"FROM {source_expr}"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling", safe_orders,
                new_cols[0] if len(new_cols) == 1 else "rolling", new_cols,
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, new_cols,
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, new_cols,
                    backend, data_id, map_sig,
                )

            # ------ Preview ------
            preview_cols = [column, *new_cols]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            preview_cols = list(dict.fromkeys(preview_cols))
            res = await self._fetch_data(new_table_name, schema, preview_cols)

            agg_label = ", ".join(raw_aggs)
            response = self._success_response(
                f"Rolling {agg_label} on '{column}' grouped by {safe_groups} (window={window})",
                result=res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=new_cols if map_feature else [],
                new_columns=new_cols,
                window=window,
                group_cols=safe_groups,
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
    async def rolling_std_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="stddevSamp", **kwargs)
    async def rolling_var_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="varSamp", **kwargs)
    async def rolling_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, q=0.5, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_q{q}_w{window}_by_{'_'.join(safe_groups)}"
            q_col = self.db.quote_identifier(safe_col)
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)
            source_projection = await self._projection_sql(table, schema, prefix="b")

            # ClickHouse exposes quantile(q)(x); quantileCont is not available
            # in all server builds.
            quantile_subq = f"""
                SELECT quantile({q})(r.{q_col})
                FROM __base r
                WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                  AND r.{q_col} IS NOT NULL
            """

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    ({quantile_subq}) AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling_quantile", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "rolling_quantile", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling quantile group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Rolling quantile on '{column}' grouped by {safe_groups} (window={window}, q={q})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
            )

        except Exception as e:
            return self._error_response(f"rolling quantile group-by error: {str(e)}")

    async def rolling_sem_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_sem_w{window}_by_{'_'.join(safe_groups)}"
            q_col = self.db.quote_identifier(safe_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)
            source_projection = await self._projection_sql(table, schema)

            sem_expr = (
                f"(stddevSamp({q_col}) OVER (\n"
                f"    PARTITION BY {partition_sql}\n"
                f"    ORDER BY {order_sql}\n"
                f"    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW\n"
                f")\n"
                f"/ sqrt(COUNT({q_col}) OVER (\n"
                f"    PARTITION BY {partition_sql}\n"
                f"    ORDER BY {order_sql}\n"
                f"    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW\n"
                f")))"
            )

            select_sql = (
                f"SELECT {source_projection},\n"
                f"    {sem_expr} AS {self.db.quote_identifier(new_col)}\n"
                f"FROM {source_expr}"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling_sem", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "rolling_sem", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling sem group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Rolling SEM on '{column}' grouped by {safe_groups} (window={window})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
            )

        except Exception as e:
            return self._error_response(f"rolling sem group-by error: {str(e)}")

    async def rolling_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_nunique_w{window}_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)
            source_projection = await self._projection_sql(table, schema, prefix="b")

            nunique_subq = f"""
                SELECT countDistinct(r.{q_col})
                FROM __base r
                WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
            """

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    ({nunique_subq}) AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling_nunique", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "rolling_nunique", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling nunique group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Rolling nunique on '{column}' grouped by {safe_groups} (window={window})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
            )

        except Exception as e:
            return self._error_response(f"rolling nunique group-by error: {str(e)}")

    async def rolling_rank_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_rank_w{window}_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)
            source_projection = await self._projection_sql(table, schema, prefix="b")

            rank_subq = f"""
                SELECT COUNT(*)
                FROM __base r
                WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                  AND r.{q_col} <= b.{q_col}
            """

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    ({rank_subq}) AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling_rank", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "rolling_rank", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling rank group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Rolling rank on '{column}' grouped by {safe_groups} (window={window})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
            )

        except Exception as e:
            return self._error_response(f"rolling rank group-by error: {str(e)}")

    # ---------- Rolling Datetime Group‑By ----------
    async def _rolling_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, window, stat, backend, data_id, new_table=None,
        map_feature: bool = False
    ):
        """Copy of _rolling_datetime_stat with PARTITION BY added. Supports ClickHouse."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            w = int(window) - 1
            if w < 0:
                return self._error_response("window must be >= 1")

            stat_key = stat.lower()
            new_col = f"{safe_col}_rolling_{stat_key}_w{window}_by_{'_'.join(safe_groups)}"
            q_col = self.db.quote_identifier(safe_col)
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            is_date_only = await self._is_date_only_column(table, schema, safe_col)
            from_epoch_expr = self._ch_from_epoch("__agg.value_epoch", is_date_only)

            if stat_key == "mean":
                epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")
                value_sql = f"""
                    SELECT {from_epoch_expr}
                    FROM (
                        SELECT AVG({epoch_expr}) AS value_epoch
                        FROM __base r
                        WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                          AND r.{q_col} IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")
                median_epoch_sql = f"median({epoch_expr})"
                value_sql = f"""
                    SELECT {from_epoch_expr}
                    FROM (
                        SELECT {median_epoch_sql} AS value_epoch
                        FROM __base r
                        WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                          AND r.{q_col} IS NOT NULL
                    ) __agg
                """
            elif stat_key == "mode":
                value_sql = f"""
                    SELECT r.{q_col}
                    FROM __base r
                    WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                      AND r.{q_col} IS NOT NULL
                    GROUP BY r.{q_col}
                    ORDER BY COUNT(*) DESC, r.{q_col}
                    LIMIT 1
                """
            else:
                return self._error_response(f"Unsupported datetime rolling stat '{stat}'")

            except_clause = f"EXCEPT({self._CH_ROW_NUM}, __rn)" if self._CH_ROW_NUM in safe_orders else "EXCEPT(__rn)"

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT b.* {except_clause},\n"
                f"    ({value_sql}) AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "rolling_datetime", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "rolling_datetime", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Rolling datetime group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Rolling datetime {stat_key} on '{column}' grouped by {safe_groups} (window={window})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                window=window,
                group_cols=safe_groups,
            )

        except Exception as e:
            return self._error_response(f"rolling datetime group-by error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    #  EXPANDING with GROUP BY – generic engine
    # ------------------------------------------------------------------
    async def _expanding_agg_with_partition(
        self,
        table, schema, column, group_cols,
        order_by=None, agg="SUM", min_periods=1,
        backend=None, data_id=None, new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
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

            # ------ Resolve ordering ------
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

            new_cols = []
            used_col_names = set()
            numeric_aggs = []
            suffix_group = "_".join(safe_groups)

            for raw in raw_aggs:
                spec = self._ch_agg_spec(raw)
                if spec is None:
                    return self._error_response(f"Unsupported expanding aggregation '{raw}'")
                sql_agg, alias = spec

                base_col = f"{safe_col}_expanding_{alias}_by_{suffix_group}"
                col_name = base_col
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                agg_over = f"{sql_agg}({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                if min_periods > 1:
                    agg_with_null = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                else:
                    agg_with_null = agg_over

                numeric_aggs.append(f"{agg_with_null} AS {self.db.quote_identifier(col_name)}")

            window_sql = ", ".join(numeric_aggs)
            source_projection = await self._projection_sql(table, schema)
            select_sql = (
                f"SELECT {source_projection},\n"
                f"    {window_sql}\n"
                f"FROM {source_expr}"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding", safe_orders,
                new_cols[0] if len(new_cols) == 1 else "expanding", new_cols,
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, new_cols,
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, new_cols,
                    backend, data_id, map_sig,
                )

            preview_cols = [column, *new_cols]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            preview_cols = list(dict.fromkeys(preview_cols))
            res = await self._fetch_data(new_table_name, schema, preview_cols)

            response = self._success_response(
                f"Expanding {', '.join(raw_aggs)} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=new_cols if map_feature else [],
                new_columns=new_cols,
                min_periods=min_periods,
                group_cols=safe_groups,
            )
            if len(new_cols) == 1:
                response["new_column"] = new_cols[0]
            return response

        except Exception as e:
            return self._error_response(f"expanding group-by error: {str(e)}\n{traceback.format_exc()}")

    # --------------------------------------------------
    #  EXPANDING GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def expanding_std_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="stddevSamp", **kwargs)
    async def expanding_var_groupby(self, *args, group_cols=None, **kwargs):
        return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="varSamp", **kwargs)
    async def expanding_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, q=0.5, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            new_col = f"{safe_col}_expanding_q{q}_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            # ClickHouse exposes quantile(q)(x); quantileCont is not available
            # in all server builds.
            quantile_subq = f"""
                SELECT quantile({q})(r.{q_col})
                FROM __base r
                WHERE r.{q_rn} <= b.{q_rn}
                  AND r.{q_col} IS NOT NULL
            """

            if min_periods > 1:
                count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                value_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({quantile_subq}) ELSE NULL END"
            else:
                value_expr = f"({quantile_subq})"

            source_projection = await self._projection_sql(table, schema, prefix="b")

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    {value_expr} AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding_quantile", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "expanding_quantile", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding quantile group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
            return self._success_response(
                f"Expanding quantile on '{column}' grouped by {safe_groups} (q={q})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                min_periods=min_periods,
            )

        except Exception as e:
            return self._error_response(f"expanding quantile group-by error: {str(e)}")

    async def expanding_sem_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            new_col = f"{safe_col}_expanding_sem_by_{'_'.join(safe_groups)}"

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

            # ClickHouse uses stddevSamp
            sem_base = f"(stddevSamp({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame}) / sqrt({count_over}))"
            if min_periods > 1:
                sem_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {sem_base} ELSE NULL END"
            else:
                sem_expr = sem_base

            source_projection = await self._projection_sql(table, schema)
            select_sql = (
                f"SELECT {source_projection},\n"
                f"    {sem_expr} AS {self.db.quote_identifier(new_col)}\n"
                f"FROM {source_expr}"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding_sem", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "expanding_sem", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding sem group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
            return self._success_response(
                f"Expanding SEM on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                min_periods=min_periods,
            )

        except Exception as e:
            return self._error_response(f"expanding sem group-by error: {str(e)}")

    async def expanding_rank_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            new_col = f"{safe_col}_expanding_rank_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            rank_subq = f"""
                SELECT COUNT(*)
                FROM __base r
                WHERE r.{q_rn} <= b.{q_rn}
                  AND r.{q_col} <= b.{q_col}
            """
            count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"

            if min_periods > 1:
                rank_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({rank_subq}) ELSE NULL END"
            else:
                rank_expr = f"({rank_subq})"

            source_projection = await self._projection_sql(table, schema, prefix="b")

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    {rank_expr} AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding_rank", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "expanding_rank", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding rank group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
            return self._success_response(
                f"Expanding rank on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                min_periods=min_periods,
            )

        except Exception as e:
            return self._error_response(f"expanding rank group-by error: {str(e)}")

    async def expanding_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            new_col = f"{safe_col}_expanding_nunique_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            nunique_subq = f"""
                SELECT countDistinct(r.{q_col})
                FROM __base r
                WHERE r.{q_rn} <= b.{q_rn}
            """

            if min_periods > 1:
                count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                value_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({nunique_subq}) ELSE NULL END"
            else:
                value_expr = f"({nunique_subq})"

            source_projection = await self._projection_sql(table, schema, prefix="b")

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT {source_projection},\n"
                f"    {value_expr} AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding_nunique", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "expanding_nunique", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding nunique group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
            return self._success_response(
                f"Expanding nunique on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                min_periods=min_periods,
            )

        except Exception as e:
            return self._error_response(f"expanding nunique group-by error: {str(e)}")

    # --------------------------------------------------
    #  EXPANDING DATETIME GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def _expanding_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, stat, min_periods, backend, data_id, new_table=None,
        map_feature: bool = False
    ):
        """Copy of _expanding_datetime_stat with PARTITION BY. Supports ClickHouse."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            q_col = self.db.quote_identifier(safe_col)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                source_expr = self._qualified_table(table, schema)
            else:
                safe_orders = [self._CH_ROW_NUM]
                source_expr = self._ch_source_with_rownum(
                    self._qualified_table(table, schema)
                )

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
            new_col = f"{safe_col}_expanding_{stat}_by_{'_'.join(safe_groups)}"
            q_rn = self.db.quote_identifier("__rn")
            q_new_col = self.db.quote_identifier(new_col)

            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)

            is_date_only = await self._is_date_only_column(table, schema, safe_col)
            from_epoch_expr = self._ch_from_epoch("__agg.value_epoch", is_date_only)
            epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")

            if stat == "mean":
                inner = f"AVG({epoch_expr})"
            elif stat == "median":
                inner = f"median({epoch_expr})"
            elif stat == "mode":
                inner = None
            else:
                return self._error_response(f"Unsupported datetime expanding stat '{stat}'")

            if stat == "mode":
                mode_subq = f"""
                    SELECT r.{q_col}
                    FROM __base r
                    WHERE r.{q_rn} <= b.{q_rn}
                      AND r.{q_col} IS NOT NULL
                    GROUP BY r.{q_col}
                    ORDER BY COUNT(*) DESC, r.{q_col}
                    LIMIT 1
                """
                non_null_count = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                if min_periods > 1:
                    value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({mode_subq}) ELSE NULL END"
                else:
                    value_sql = f"({mode_subq})"
            else:
                non_null_count = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                value_subq = f"""
                    SELECT {from_epoch_expr}
                    FROM (SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.{q_rn} <= b.{q_rn}
                          AND r.{q_col} IS NOT NULL) __agg
                """
                if min_periods > 1:
                    value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({value_subq}) ELSE NULL END"
                else:
                    value_sql = f"({value_subq})"

            except_clause = f"EXCEPT({self._CH_ROW_NUM}, __rn)" if self._CH_ROW_NUM in safe_orders else "EXCEPT(__rn)"

            select_sql = (
                f"WITH __base AS (\n"
                f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                f"    FROM {source_expr}\n"
                f")\n"
                f"SELECT b.* {except_clause},\n"
                f"    {value_sql} AS {q_new_col}\n"
                f"FROM __base b"
            )

            create_sql = self._ch_create_table_as(output_qualified, select_sql)
            map_sig = self._map_sig(
                safe_groups, safe_col, "expanding_datetime", safe_orders,
                [new_col][0] if len([new_col]) == 1 else "expanding_datetime", [new_col],
            )
            map_collision = None
            if map_feature:
                map_collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [new_col],
                )
                if map_collision:
                    return self._error_response(
                        f"Expanding datetime group-by error: {map_collision}",
                        group_cols=safe_groups,
                    )
            await self._exec(create_sql)
            if map_feature:
                await self._apply_map(
                    table, schema, new_table_name, [new_col],
                    backend, data_id, map_sig,
                )

            preview_cols = [column, new_col]
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

            return self._success_response(
                f"Expanding datetime {stat} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[new_col] if map_feature else [],
                new_column=new_col,
                min_periods=min_periods,
                group_cols=safe_groups,
            )

        except Exception as e:
            return self._error_response(f"expanding datetime group-by error: {str(e)}\n{traceback.format_exc()}")

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
        map_feature: bool = False,
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING — unchanged)
            # ============================================================
            import asyncio

            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            if isinstance(agg, str):
                aggs = [agg]
            else:
                aggs = list(agg)
            for a in aggs:
                if a not in ("mean", "sum", "std", "var"):
                    return self._error_response(f"Unsupported ewm aggregation '{a}'")

            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            source_qualified = self._qualified_table(table, schema)

            # ---- Resolve ordering ----
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
            else:
                safe_orders = []

            # ---- Create working table with materialized row number ----
            # ClickHouse has no ctid/rowid, and ROW_NUMBER() OVER() is
            # non-deterministic across separate queries.  We materialize
            # _mf_row_num once so that both the Python fetch and the SQL
            # CTE see identical row order.
            working_table = SQLIdentifierSanitizer.sanitize(
                f"{table}__ewm_work_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
            )
            working_qualified = self._qualified_table(working_table, schema)
            await self._exec(
                f"CREATE TABLE {working_qualified} ENGINE = MergeTree() ORDER BY tuple() AS "
                f"SELECT *, ROW_NUMBER() OVER() AS {self.db.quote_identifier(self._CH_ROW_NUM)} "
                f"FROM {source_qualified}"
            )

            # Full deterministic ordering: groups + user order + _mf_row_num
            all_order_cols = safe_groups + safe_orders + [self._CH_ROW_NUM]
            full_order_sql = ", ".join(self.db.quote_identifier(c) for c in all_order_cols)

            # ---- Fetch data ----
            cols_to_fetch = [safe_col] + safe_groups + safe_orders + [self._CH_ROW_NUM]
            seen = set()
            cols_to_fetch = [c for c in cols_to_fetch if not (c in seen or seen.add(c))]

            rows = await self._fetch(
                f"SELECT {', '.join(self.db.quote_identifier(c) for c in cols_to_fetch)} "
                f"FROM {working_qualified} "
                f"ORDER BY {full_order_sql}"
            )
            df = pd.DataFrame(rows, columns=cols_to_fetch)

            # ---- Compute EWM per group (offloaded to thread) ----
            alpha_val = self._alpha_from_params(com, span, halflife, alpha)

            def _compute_all_groups():
                n_loc = len(df)
                loc_results = {f: np.full(n_loc, np.nan) for f in aggs}
                grouped_df = df.groupby(safe_groups, sort=False)
                for gk, gdf in grouped_df:
                    varr = np.array(
                        [v if v is not None else np.nan for v in gdf[safe_col]],
                        dtype=np.float64,
                    )
                    gres = GroupbyWindowOps._compute_ewm_array(
                        varr, alpha_val, adjust, ignore_na, min_periods, aggs
                    )
                    for f in aggs:
                        loc_results[f][gdf.index] = gres[f]
                return loc_results

            all_results = await asyncio.to_thread(_compute_all_groups)
            n = len(df)

            # ---- Build result table ----
            new_table_name = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(new_table_name, schema)
            new_col_names = [f"{safe_col}_ewm_{f}_by_{'_'.join(safe_groups)}" for f in aggs]

            # ---- Create staging Memory table ----
            stage_table = SQLIdentifierSanitizer.sanitize(
                f"{new_table_name}__ewm_stage_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
            )
            q_stage = self._qualified_table(stage_table, schema)

            stage_col_defs = [f"{self.db.quote_identifier('rn')} UInt64"]
            for c in new_col_names:
                stage_col_defs.append(f"{self.db.quote_identifier(c)} Nullable(Float64)")

            await self._exec(f"DROP TABLE IF EXISTS {q_stage}")
            await self._exec(
                f"CREATE TABLE {q_stage} ({', '.join(stage_col_defs)}) ENGINE = Memory"
            )

            try:
                # ---- Insert computed EWM values ----
                insert_col_names = ["rn"] + new_col_names

                def build_insert_rows():
                    rows_out = []
                    for i in range(n):
                        row = [i + 1]
                        for f in aggs:
                            val = all_results[f][i]
                            row.append(None if np.isnan(val) else float(val))
                        rows_out.append(row)
                    return rows_out

                insert_rows_data = await asyncio.to_thread(build_insert_rows)

                insert_chunk_size = 10_000
                for start in range(0, n, insert_chunk_size):
                    stop = min(start + insert_chunk_size, n)
                    chunk = insert_rows_data[start:stop]
                    await self.db.insert_rows(
                        f"{schema}.{stage_table}",
                        chunk,
                        insert_col_names,
                    )

                # ---- Create final table via LEFT JOIN ----
                ewm_select_cols = ", ".join(
                    f"e.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}"
                    for c in new_col_names
                )

                final_sql = f"""
                CREATE TABLE {output_qualified} AS
                WITH __base AS (
                    SELECT *, ROW_NUMBER() OVER(ORDER BY {full_order_sql}) AS rn
                    FROM {working_qualified}
                )
                SELECT b.* EXCEPT({self._CH_ROW_NUM}, rn),
                    {ewm_select_cols}
                FROM __base b
                LEFT JOIN {q_stage} e ON b.rn = e.{self.db.quote_identifier('rn')}
                """

                await self._exec(final_sql)
            finally:
                await self._exec(f"DROP TABLE IF EXISTS {q_stage}")

            # ---- Drop working table ----
            await self._exec(f"DROP TABLE IF EXISTS {working_qualified}")

            # ---- Preview ----
            preview_cols = [safe_col] + new_col_names
            if order_by:
                preview_cols.extend(safe_orders)
            preview_cols.extend(safe_groups)
            res = await self._fetch_data(
                new_table_name, schema, list(dict.fromkeys(preview_cols))
            )

            return self._success_response(
                f"EWM {aggs} on '{column}' grouped by {safe_groups}",
                res,
                new_table=new_table_name,
                mapped_table=table if map_feature else None,
                mapped_columns=new_col_names if map_feature else [],
                new_columns=new_col_names,
                group_cols=safe_groups,
            )

        except Exception as e:
            return self._error_response(f"ewm group-by error: {str(e)}\n{traceback.format_exc()}")
    
    async def _wait_clickhouse_mutation(self, table_qualified: str):
        """
        Wait for all pending ClickHouse mutations on the given table to finish.
        ClickHouse ALTER TABLE UPDATE is asynchronous for MergeTree.
        """
        if not isinstance(self.db, ClickHouseAdapter):
            return

        import asyncio
        # Extract just the table name from the qualified name
        # e.g., `upload`.`table__op_3` → table__op_3
        parts = table_qualified.replace('`', '').split('.')
        table_name_only = parts[-1] if len(parts) > 1 else parts[0]
        database = parts[0] if len(parts) > 1 else "currentDatabase()"

        for _ in range(120):  # timeout after ~120 seconds
            rows = await self._fetch(
                f"SELECT count() FROM system.mutations "
                f"WHERE database = '{database}' "
                f"AND table = '{table_name_only}' "
                f"AND is_done = 0"
            )
            if not rows or rows[0][0] == 0:
                return
            await asyncio.sleep(1)

        raise RuntimeError(
            f"ClickHouse mutations on {table_qualified} did not finish within timeout"
        )

    @staticmethod

    async def _apply_map(
        self, table, schema, group_table, new_columns, backend, data_id, sig,
    ):
        try:
            orig_cols = list(await self.db.get_column_types(table, schema) or {})
        except Exception:
            orig_cols = []
        if not orig_cols:
            raise ValueError(f"map_feature: could not list columns of {schema}.{table}")
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        cond = " AND ".join(
            f'o.{self.db.quote_identifier(c)} = g.{self.db.quote_identifier(c)}'
            for c in orig_cols
        )
        feats = ", ".join(
            f'g.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}'
            for c in new_columns
        )
        tmp = SQLIdentifierSanitizer.sanitize(f"{table}__map_tmp")
        tmp_q = self._qualified_table(tmp, schema)
        await self._exec(f"DROP TABLE IF EXISTS {tmp_q}")
        await self._exec(
            f"""CREATE TABLE {tmp_q}
                ENGINE = MergeTree()
                ORDER BY tuple()
                AS SELECT o.*, {feats}
                FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
        )
        await self._exec(f"EXCHANGE TABLES {orig_q} AND {tmp_q}")
        await self._exec(f"DROP TABLE {tmp_q}")
        await self._record_map_marker(table, schema, backend, data_id, sig)

