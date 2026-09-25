from __future__ import annotations

import traceback
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from memframe.core.analytix.groupby_cumulative.base import GroupbyCumulativeOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseGroupbyCumulativeOps(GroupbyCumulativeOps):
    """ClickHouse backend.

    Dialect hooks (native ``stddevPop``/``varPop``) plus two structural
    overrides: the MergeTree CTAS with the ``_mf_row_num`` ordering
    fallback, and the create-swap-``EXCHANGE`` map-back, because
    asynchronous ``UPDATE`` mutations can return stale data.
    """

    @property
    def _row_id_col(self) -> str:
        # ClickHouse has no physical row ID.
        # When order_cols is None, _groupby_cumulative_op uses a
        # ROW_NUMBER() OVER() subquery fallback instead of this.
        raise NotImplementedError(
            "ClickHouse has no physical row identifier. "
            "Provide order_cols, or rely on the ROW_NUMBER() fallback "
            "in _groupby_cumulative_op."
        )

    def _std_window(self, col_q: str) -> str:
        # ClickHouse native name: stddevPop (STDDEV_POP works as alias,
        # but using the canonical name avoids any version surprises)
        return f"stddevPop({col_q})"

    def _var_window(self, col_q: str) -> str:
        # ClickHouse native name: varPop (VAR_POP works as alias,
        # but using the canonical name avoids any version surprises)
        return f"varPop({col_q})"

    async def _apply_map(
        self,
        table: str,
        schema: str,
        group_table: str,
        new_columns: List[str],
        backend,
        data_id: str,
        sig: str,
    ) -> None:
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

    async def _groupby_cumulative_op(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        window_expr: str,
        operation_name: str,
        order_cols: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        target_type: str = "DOUBLE PRECISION",
        backend=None,
        data_id=None,
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id are required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

            # ------ Resolve ordering ------
            # ClickHouse has no physical row ID (no ctid / rowid).
            # When order_cols is None we inject a synthetic _mf_row_num
            # via ROW_NUMBER() OVER() in a subquery, then EXCLUDE it
            # from the final output so chained operations stay clean.
            if order_cols is None:
                use_row_num_fallback = True
                safe_orders = ["_mf_row_num"]
                order_sql = self.db.quote_identifier("_mf_row_num")
            else:
                use_row_num_fallback = False
                if isinstance(order_cols, str):
                    order_cols = [order_cols]
                safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_cols]
                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)

            # ------ PARTITION BY / window spec ------
            partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)

            window_spec = (
                f"PARTITION BY {partition_sql} "
                f"ORDER BY {order_sql} "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            )

            if "{window_spec}" in window_expr:
                full_window = window_expr.replace("{window_spec}", window_spec)
            else:
                full_window = f"{window_expr} OVER ({window_spec})"

            # ------ Target column name ------
            auto_tgt = f"cum_{safe_col}_{operation_name}_by_{'_'.join(safe_groups)}"
            if order_cols:
                auto_tgt = f"{auto_tgt}_order_by_{'_'.join(safe_orders)}"
            tgt = target_col or auto_tgt
            tgt_safe = SQLIdentifierSanitizer.sanitize(tgt)

            map_sig = self._map_sig(
                safe_groups, safe_col, operation_name, safe_orders,
                tgt_safe, [tgt_safe],
            )
            if map_feature:
                collision = await self._check_map_collision(
                    table, schema, backend, data_id, map_sig, [tgt_safe],
                )
                if collision:
                    return self._error_response(
                        f"Cumulative {operation_name} error: {collision}",
                        group_cols=safe_groups,
                    )

            # ------ Output table name ------
            output_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            output_qualified = self._qualified_table(output_table, schema)
            source_qualified = self._qualified_table(table, schema)

            # ------ CREATE TABLE AS SELECT ------
            # ClickHouse *requires* an ENGINE clause.
            # MergeTree with ORDER BY tuple() = no enforced sort order.
            #
            # When no order_cols were given we wrap the source in a
            # subquery that adds _mf_row_num, then strip it out with
            # SELECT * EXCEPT(_mf_row_num) so the transient result
            # stays clean for downstream chaining.
            if use_row_num_fallback:
                create_sql = (
                    f"CREATE TABLE {output_qualified} "
                    f"ENGINE = MergeTree() ORDER BY tuple() AS\n"
                    f"SELECT * EXCEPT(_mf_row_num),\n"
                    f"    {full_window} AS {self.db.quote_identifier(tgt_safe)}\n"
                    f"FROM (\n"
                    f"    SELECT *, ROW_NUMBER() OVER() AS _mf_row_num\n"
                    f"    FROM {source_qualified}\n"
                    f")"
                )
            else:
                create_sql = (
                    f"CREATE TABLE {output_qualified} "
                    f"ENGINE = MergeTree() ORDER BY tuple() AS\n"
                    f"SELECT *,\n"
                    f"    {full_window} AS {self.db.quote_identifier(tgt_safe)}\n"
                    f"FROM {source_qualified}"
                )

            await self._exec(create_sql)

            if map_feature:
                await self._apply_map(
                    table, schema, output_table, [tgt_safe],
                    backend, data_id, map_sig,
                )

            # ------ Fetch sample ------
            cols_to_fetch = [safe_col] + safe_groups
            if order_cols:
                cols_to_fetch += safe_orders
            cols_to_fetch.append(tgt_safe)
            cols_to_fetch = list(dict.fromkeys(cols_to_fetch))
            rows = await self._fetch(
                f"SELECT {', '.join(self.db.quote_identifier(c) for c in cols_to_fetch)} "
                f"FROM {output_qualified}"
            )
            sample = (
                pd.DataFrame(rows, columns=cols_to_fetch)
                if rows
                else pd.DataFrame(columns=cols_to_fetch)
            )

            order_desc = ", ".join(safe_orders) if order_cols else "default row order"
            group_desc = ", ".join(safe_groups)
            message = (
                f"Cumulative {operation_name} of '{column}' "
                f"grouped by [{group_desc}] ordered by [{order_desc}] → {tgt_safe}"
            )
            return self._success_response(
                message,
                result=sample,
                new_table=output_table,
                new_columns=[tgt_safe],
                group_cols=safe_groups,
                operation=operation_name,
                mapped_table=table if map_feature else None,
                mapped_columns=[tgt_safe] if map_feature else [],
            )

        except Exception as e:
            return self._error_response(
                f"Cumulative {operation_name} error: {str(e)}\n{traceback.format_exc()}",
                group_cols=safe_groups if 'safe_groups' in locals() else [],
            )

    async def cumsum(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = f"SUM({col_q})"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="sum",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cumprod(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        # ClickHouse supports ln/nullIf/exp — same LOG/EXP trick
        window = f"EXP(SUM(LN(NULLIF({col_q}, 0))) OVER ({{window_spec}}))"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="prod",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cummax(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = f"MAX({col_q})"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="max",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cummin(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = f"MIN({col_q})"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="min",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cummean(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = f"AVG({col_q})"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="mean",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cumcount(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = f"COUNT({col_q})"
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="count",
            order_cols=order_col,
            target_col=target_col,
            target_type="Int64",          # ClickHouse uses Int64 instead of BIGINT
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cumstd(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = self._std_window(col_q)
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="std",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def cumvar(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id=None,
        new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        window = self._var_window(col_q)
        return await self._groupby_cumulative_op(
            table, schema, column,
            group_cols=group_cols,
            window_expr=window,
            operation_name="var",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
            map_feature=map_feature,
        )
