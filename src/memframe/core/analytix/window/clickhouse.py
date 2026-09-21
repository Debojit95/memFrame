from typing import Any, Dict, List, Optional, Union
import traceback

from memframe.core.analytix.window.base import WindowOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseWindowOps(WindowOps):
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
        return "std"

    def _var_agg(self) -> str:
        return "var"

    def _ewm_row_types(self) -> tuple:
        return "UInt64", "Float64"

    def _ewm_uses_stage_table(self) -> bool:
        return True

    # --------------------------------------------------
    # Generic rolling engine (single CTAS)
    # --------------------------------------------------
    async def _rolling_agg(
        self,
        table: str,
        schema: str,
        column: str,
        order_by: Union[str, List[str]] = None,
        window: int = 3,
        agg: Union[str, List[str]] = "SUM",
        backend=None,
        data_id=None,
        new_table: Optional[str] = None) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # Normalise agg list
            if isinstance(agg, str):
                raw_aggs = [agg]
            elif isinstance(agg, (list, tuple)):
                raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
            else:
                return self._error_response("agg must be a string or list of strings")
            if not raw_aggs:
                return self._error_response("At least one aggregation function is required")

            working_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            qualified_source = self._qualified_table(table, schema)
            qualified_target = self._qualified_table(working_table, schema)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                source_sql = qualified_source
            else:
                temp_order = "__totem_order"
                order_sql = self.db.quote_identifier(temp_order)
                source_sql = f"""
                    (
                        SELECT *,
                            ROW_NUMBER() OVER () AS {order_sql}
                        FROM {qualified_source}
                    ) AS __source
                """

            w = int(window) - 1

            ch_agg_map = {
                "sum": ("sum", "sum"),
                "avg": ("avg", "mean"),
                "mean": ("avg", "mean"),
                "min": ("min", "min"),
                "max": ("max", "max"),
                "count": ("count", "count"),
                "first_value": ("first_value", "first"),
                "last_value": ("last_value", "last"),
                "first": ("first_value", "first"),
                "last": ("last_value", "last"),
                "std": ("stddevPop", "std"),
                "stddev": ("stddevPop", "std"),
                "stddev_pop": ("stddevPop", "std"),
                "stddev_samp": ("stddevSamp", "std"),
                "var": ("varSamp", "var"),
                "variance": ("varSamp", "var"),
                "var_samp": ("varSamp", "var"),
            }

            new_cols = []
            used_col_names = set()
            select_exprs = ["*"]

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in ch_agg_map:
                    sql_agg, alias_key = ch_agg_map[key]
                else:
                    return self._error_response(f"Unsupported aggregation '{raw}' for _rolling_agg")

                base_col_name = f"{safe_col}_rolling_{alias_key}_w{window}"
                col_name = base_col_name
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col_name}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                window_expr = f"""
                    {sql_agg}({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                """
                select_exprs.append(f"{window_expr} AS {self.db.quote_identifier(col_name)}")

            sql = f"""
                CREATE TABLE {qualified_target} AS
                SELECT {", ".join(select_exprs)}
                FROM {source_sql}
            """
            await self._exec(sql)

            preview_cols = new_cols + [column]
            if order_by:
                preview_cols += safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            response = self._success_response(
                f"Rolling {', '.join(raw_aggs)} on '{column}' (window={window})",
                res,
                new_table=working_table,
                new_columns=new_cols,
                window=window,
            )
            if len(new_cols) == 1:
                response["new_column"] = new_cols[0]
            return response
        except Exception as e:
            return self._error_response(f"rolling error: {str(e)}\n{traceback.format_exc()}")

    # --------------------------------------------------
    # Rolling quantile
    # --------------------------------------------------
    async def rolling_quantile(
        self,
        table,
        schema,
        column,
        order_by: Union[str, List[str]] = None,
        window=3, q=0.5,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # 1. Clone the source
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # 2. Resolve ordering
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_q{q}_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            count_over = f"""
                count({self.db.quote_identifier(safe_col)})
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """
            agg_over = f"""
                quantile({q})({self.db.quote_identifier(safe_col)})
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """
            window_sql = f"CASE WHEN {count_over} >= {int(window)} THEN {agg_over} ELSE NULL END"

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT *,
                    {window_sql} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Rolling quantile",
                res,
                new_table=result_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # Rolling SEM
    # --------------------------------------------------
    async def rolling_sem(
        self,
        table,
        schema,
        column,
        order_by: Union[str, List[str]] = None,
        window=3, backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # 1. Clone
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # 2. Ordering
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_sem_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            std_func = "stddevSamp"
            sem_expr = f"""
                ({std_func}({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                /
                sqrt(
                    count({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                ))
            """

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT *,
                    {sem_expr} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Rolling SEM",
                res,
                new_table=result_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # Rolling nunique
    # --------------------------------------------------
    async def rolling_nunique(
        self,
        table,
        schema,
        column,
        order_by: Union[str, List[str]] = None,
        window=3,
        backend=None,
        data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # --- 1. Clone ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            w = window - 1
            new_col = f"{safe_col}_rolling_nunique_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            window_sql = f"""
                uniq({self.db.quote_identifier(safe_col)})
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT *,
                    {window_sql} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Rolling nunique",
                res,
                new_table=result_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # Generic expanding engine (single CTAS)
    # --------------------------------------------------
    async def _expanding_agg(
        self,
        table: str,
        schema: str,
        column: str,
        order_by: Union[str, List[str]] = None,
        agg: Union[str, List[str]] = "SUM",
        backend=None,
        data_id=None,
        min_periods: int = 1,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)
            if isinstance(agg, str):
                raw_aggs = [agg]
            elif isinstance(agg, (list, tuple)):
                raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
            else:
                return self._error_response("agg must be a string or list of strings")
            if not raw_aggs:
                return self._error_response("At least one aggregation function is required")

            working_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            qualified_source = self._qualified_table(table, schema)
            qualified_target = self._qualified_table(working_table, schema)

            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                source_sql = qualified_source
            else:
                temp_order = "__totem_order"
                order_sql = self.db.quote_identifier(temp_order)
                source_sql = f"""
                    (
                        SELECT *,
                            ROW_NUMBER() OVER () AS {order_sql}
                        FROM {qualified_source}
                    ) AS __source
                """

            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

            ch_agg_map = {
                "sum": ("sum", "sum"),
                "avg": ("avg", "mean"),
                "mean": ("avg", "mean"),
                "min": ("min", "min"),
                "max": ("max", "max"),
                "count": ("count", "count"),
                "first": ("first_value", "first"),
                "last": ("last_value", "last"),
                "first_value": ("first_value", "first"),
                "last_value": ("last_value", "last"),
                "std": ("stddevSamp", "std"),
                "stddev": ("stddevSamp", "std"),
                "stddev_samp": ("stddevSamp", "std"),
                "var": ("varSamp", "var"),
                "variance": ("varSamp", "var"),
                "var_samp": ("varSamp", "var"),
            }
            new_cols = []
            used_col_names = set()
            select_exprs = ["*"]

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in ch_agg_map:
                    sql_agg, alias_key = ch_agg_map[key]
                else:
                    return self._error_response(f"Unsupported expanding aggregation '{raw}'")

                base_col_name = f"{safe_col}_expanding_{alias_key}"
                col_name = base_col_name
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col_name}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)

                count_over = f"count({self.db.quote_identifier(safe_col)}) OVER (ORDER BY {order_sql} {window_frame})"
                agg_over = f"{sql_agg}({self.db.quote_identifier(safe_col)}) OVER (ORDER BY {order_sql} {window_frame})"

                if min_periods > 1:
                    agg_with_null = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                else:
                    agg_with_null = agg_over

                select_exprs.append(f"{agg_with_null} AS {self.db.quote_identifier(col_name)}")

            await self._exec(f"""
                CREATE TABLE {qualified_target} AS
                SELECT {", ".join(select_exprs)}
                FROM {source_sql}
            """)

            preview_cols = new_cols + [column]
            if order_by:
                preview_cols += safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            response = self._success_response(
                f"Expanding {', '.join(raw_aggs)} on '{column}' (min_periods={min_periods})",
                res,
                new_table=working_table,
                new_columns=new_cols,
                min_periods=min_periods,
            )
            if len(new_cols) == 1:
                response["new_column"] = new_cols[0]
            return response
        except Exception as e:
            return self._error_response(f"expanding error: {str(e)}\n{traceback.format_exc()}")

    # --------------------------------------------------
    # Expanding quantile
    # --------------------------------------------------
    async def expanding_quantile(
        self, table, schema, column,
        order_by=None, q=0.5, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # --- 1. Clone source ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            new_col = f"{safe_col}_expanding_q{q}"
            qualified = self._qualified_table(ordered_table, schema)

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"count({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"

            base_func = f"quantile({q})({safe_col_quoted})"
            agg_over = f"{base_func} OVER (ORDER BY {order_sql} {window_frame})"

            if min_periods > 1:
                quantile_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
            else:
                quantile_expr = agg_over

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT *,
                    {quantile_expr} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Expanding quantile", res,
                new_table=result_table, new_column=new_col,
                q=q, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # Expanding SEM
    # --------------------------------------------------
    async def expanding_sem(
        self, table, schema, column,
        order_by=None, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # --- 1. Clone source ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            new_col = f"{safe_col}_expanding_sem"
            qualified = self._qualified_table(ordered_table, schema)

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"count({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"

            std_func = "stddevSamp"
            sem_base = f"({std_func}({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame}) / sqrt({count_over}))"

            if min_periods > 1:
                sem_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {sem_base} ELSE NULL END"
            else:
                sem_expr = sem_base

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT *,
                    {sem_expr} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Expanding SEM", res,
                new_table=result_table, new_column=new_col, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # Expanding nunique
    # --------------------------------------------------
    async def expanding_nunique(
        self, table, schema, column,
        order_by=None, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # --- 1. Clone source ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            new_col = f"{safe_col}_expanding_nunique"
            qualified = self._qualified_table(ordered_table, schema)

            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"count({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"
            count_distinct = f"uniq({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"
            if min_periods > 1:
                nunique_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {count_distinct} ELSE NULL END"
            else:
                nunique_expr = count_distinct

            sql = f"""
            CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
            SELECT *, {nunique_expr} AS {self.db.quote_identifier(new_col)}
            FROM {qualified}
            """
            await self._exec(sql)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)
            return self._success_response(
                "Expanding nunique", res,
                new_table=result_table, new_column=new_col, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))
