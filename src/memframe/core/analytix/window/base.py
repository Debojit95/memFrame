from typing import Any, Dict, List, Optional, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor
import traceback
from datetime import datetime, timezone
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
import math
import numpy as np


class WindowOps:
    """Core rolling window operations (SQL-based).

    Shared engine with DuckDB-flavoured defaults; PostgreSQL and ClickHouse
    subclass this and override the dialect hooks (and the few structurally
    divergent operations) so the generated SQL is byte-identical per backend.
    """
    TMP_ROW = "__totem_tmp_row"

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # --------------------------------------------------
    # Dialect hooks (overridden per backend)
    # --------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        # ponytail: DuckDB-flavoured default; postgres.py returns "ctid".
        return "rowid"

    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"EPOCH(CAST({column_expr} AS TIMESTAMP))"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"MEDIAN({epoch_expr})"

    def _from_epoch_expr(self, is_date_only: bool) -> str:
        expr = "TO_TIMESTAMP(__agg.value_epoch)"
        return f"CAST({expr} AS DATE)" if is_date_only else expr

    def _avg_fn(self) -> str:
        return "AVG"

    def _count_fn(self) -> str:
        return "COUNT"

    def _std_agg(self) -> str:
        return "STDDEV_SAMP"

    def _var_agg(self) -> str:
        return "VAR_SAMP"

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"QUANTILE_CONT({col_quoted}, {q})"

    def _ewm_row_types(self) -> tuple:
        return "BIGINT", "DOUBLE"

    def _ewm_uses_stage_table(self) -> bool:
        return False

    # --------------------------------------------------
    # Helpers (reuse pattern)
    # --------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    async def _add_col(self, table: str, schema: str, col_name: str, col_type: str):
        q = self._qualified_table(table, schema)
        safe = SQLIdentifierSanitizer.sanitize(col_name)
        await self._exec(f'ALTER TABLE {q} ADD COLUMN IF NOT EXISTS "{safe}" {col_type}')

    async def _fetch_data(self, table: str, schema: str, columns="*", limit: int = None):
        qualified = self._qualified_table(table, schema)

        if columns == "*":
            col_clause = "*"
        else:
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
        if limit:
            rows = await self._fetch(f"SELECT {col_clause} FROM {qualified} LIMIT {limit}")
        else:
            rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _resolve_ordering(self, table: str, schema: str, order_by: str):
        """
        Resolve ordering column.

        If order_by is None:
            → create temp table with ROW_NUMBER() to preserve current order
        """

        if order_by:
            return SQLIdentifierSanitizer.sanitize(order_by), table

        rowid_col = "__totem_rowid"
        temp_table = SQLIdentifierSanitizer.sanitize(
            f"{table}__rowid_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        )

        qualified = self._qualified_table(table, schema)

        create_sql = f"""
            CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(temp_table)} AS
            SELECT *,
                ROW_NUMBER() OVER () AS {self.db.quote_identifier(rowid_col)}
            FROM {qualified}
        """

        await self._exec(create_sql)

        return rowid_col, temp_table

    def _success_response(self, message, result, **extra):
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": result,
            **extra,
        }

    def _error_response(self, msg):
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for window operation: {self.db.__class__.__name__}"
        )

    # ---------- Transient-table helpers (mirror other ops classes) ----------
    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await backend.fetchval(sql, *args)

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self._backend_fetch_val(
            backend,
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = (max_op or 0) + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    async def _resolve_output_table_name(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)

        if new_table:
            candidate = SQLIdentifierSanitizer.sanitize(new_table)
        elif backend is not None and data_id:
            candidate = await self._generate_transient_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    async def _prepare_operation_table(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        source_table = SQLIdentifierSanitizer.sanitize(table)
        output_table = await self._resolve_output_table_name(
            source_table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

        qualified_source = self._qualified_table(source_table, safe_schema)
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source}")
        return output_table

    async def _is_date_only_column(self, table: str, schema: str, column: str) -> bool:
        try:
            col_types = await self.db.get_column_types(table, schema)
            lookup = {str(k).lower(): str(v).lower() for k, v in col_types.items()}
            detected = lookup.get(column.lower(), "")
            return "date" in detected and "time" not in detected
        except Exception:
            return False

    async def _get_column_sql_type(self, table: str, schema: str, column: str) -> Optional[str]:
        try:
            col_types = await self.db.get_column_types(table, schema)
            for col_name, col_type in col_types.items():
                if str(col_name).lower() == column.lower():
                    return str(col_type)
        except Exception:
            return None
        return None

    def _is_numeric_sql_type(self, col_type: Optional[str]) -> bool:
        if not col_type:
            return False
        normalized = col_type.lower()
        numeric_markers = (
            "int", "decimal", "numeric", "real", "double", "float",
            "serial", "number", "hugeint", "ubigint", "uinteger",
            "usmallint", "utinyint",
        )
        return any(marker in normalized for marker in numeric_markers)

    def _window_result_sql_type(
        self,
        source_type: Optional[str],
        alias_key: str,
        sql_agg: str,
    ) -> str:
        agg_key = sql_agg.upper()
        if agg_key in {"FIRST_VALUE", "LAST_VALUE"} and source_type:
            return source_type
        if agg_key in {"MIN", "MAX"} and source_type and not self._is_numeric_sql_type(source_type):
            return source_type
        if alias_key == "count":
            return "DOUBLE PRECISION"
        return "DOUBLE PRECISION"

    async def _rolling_datetime_stat(self, table: str, schema: str, column: str, order_by: Union[str, List[str]] = None, window: int = 3, stat: str = "mean", backend=None, data_id=None) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # resolve ordering: supports single column or list of columns
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                working_table = table
            else:
                safe_order, working_table = await self._resolve_ordering(
                    table, schema, order_by
                )
                safe_orders = [safe_order]

            w = int(window) - 1
            if w < 0:
                return self._error_response("window must be >= 1")

            stat_key = stat.lower()
            new_col = f"{safe_col}_rolling_{stat_key}_w{window}"

            new_table = await self._generate_transient_table_name(
                table, backend, data_id
            )
            qualified = self._qualified_table(working_table, schema)

            q_schema = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(schema))
            q_new_table = self.db.quote_identifier(new_table)
            q_col = self.db.quote_identifier(safe_col)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            q_rn = self.db.quote_identifier("__totem_roll_rn")

            is_date_only = await self._is_date_only_column(working_table, schema, safe_col)
            from_epoch_expr = self._from_epoch_expr(is_date_only)

            if stat_key == "mean":
                epoch_expr = self._datetime_epoch_expr(f"r.{q_col}")
                value_sql = f"""
                    SELECT {from_epoch_expr}
                    FROM (
                        SELECT {self._avg_fn()}({epoch_expr}) AS value_epoch
                        FROM __base r
                        WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                          AND r.{q_col} IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._datetime_epoch_expr(f"r.{q_col}")
                median_epoch_sql = self._median_epoch_sql(epoch_expr)
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

            sql = f"""
                CREATE TABLE {q_schema}.{q_new_table} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY {order_sql}) AS {q_rn}
                    FROM {qualified}
                )
                SELECT b.*,
                    ({value_sql}) AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """

            await self._exec(sql)

            if order_by:
                res = await self._fetch_data(
                    new_table, schema, [column, new_col, *safe_orders]
                )
            else:
                res = await self._fetch_data(new_table, schema, [column, new_col])

            return self._success_response(
                f"Rolling datetime {stat_key} on '{column}' (window={window})",
                res,
                new_table=new_table,
                new_column=new_col,
                window=window,
            )
        except Exception as e:
            return self._error_response(
                f"rolling datetime error: {str(e)}\n{traceback.format_exc()}"
            )

    # --------------------------------------------------
    # 🔥 GENERIC ROLLING ENGINE
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

            # --- 1. Clone table ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Resolve ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
            else:
                # simulate stable order with a temporary row-number column
                temp_row = "__totem_order"
                await self._add_col(working_table, schema, temp_row, "BIGINT")
                q = self._qualified_table(working_table, schema)
                rowid = self._row_id_col
                if rowid == "ctid":
                    await self._exec(
                        f'UPDATE {q} AS t SET "{temp_row}" = rn '
                        f'FROM (SELECT {rowid}, ROW_NUMBER() OVER () AS rn FROM {q}) sub '
                        f'WHERE t.{rowid} = sub.{rowid}'
                    )
                else:
                    await self._exec(
                        f'UPDATE {q} SET "{temp_row}" = rn '
                        f'FROM (SELECT {rowid}, ROW_NUMBER() OVER () AS rn FROM {q}) sub '
                        f'WHERE {q}.{rowid} = sub.{rowid}'
                    )
                safe_orders = [temp_row]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            w = int(window) - 1
            qualified = self._qualified_table(working_table, schema)
            source_col_type = await self._get_column_sql_type(working_table, schema, safe_col)

            # --- 3. Function mapping ---
            agg_map = {
                "sum": "SUM", "avg": "AVG", "mean": "AVG",
                "min": "MIN", "max": "MAX", "count": "COUNT",
                "first_value": "FIRST_VALUE", "last_value": "LAST_VALUE",
                "first": "FIRST_VALUE", "last": "LAST_VALUE",
            }
            new_cols = []
            used_col_names = set()

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in agg_map:
                    sql_agg = agg_map[key]
                    alias_key = key
                elif key in ("std", "stddev", "stddev_pop", "stddev_samp"):
                    sql_agg = "STDDEV_POP"
                    alias_key = "std"
                elif key in ("var", "variance", "var_samp"):
                    sql_agg = self._var_agg()
                    alias_key = "var"
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

                # Add the column
                result_type = self._window_result_sql_type(
                    source_col_type, alias_key, sql_agg
                )
                await self._add_col(working_table, schema, col_name, result_type)

                # Build the UPDATE statement
                q_col = self.db.quote_identifier(col_name)
                window_expr = f"""
                    {sql_agg}({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                """
                rid = self._row_id_col
                await self._exec(f"""
                    UPDATE {qualified} AS t
                    SET {q_col} = s.val
                    FROM (
                        SELECT {rid},
                            {window_expr} AS val
                        FROM {qualified}
                    ) AS s
                    WHERE t.{rid} = s.{rid}
                """)

            # --- 4. Clean up temporary ordering column ---
            if not order_by and safe_orders[0] == temp_row:
                await self._exec(f'ALTER TABLE {qualified} DROP COLUMN IF EXISTS "{temp_row}"')

            # --- 5. Sample and response ---
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
    # NUMERIC API
    # --------------------------------------------------
    async def rolling_sum(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="SUM", **kwargs)

    async def rolling_mean(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="mean", **kwargs)

    async def rolling_min(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="MIN", **kwargs)

    async def rolling_max(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="MAX", **kwargs)

    async def rolling_count(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="COUNT", **kwargs)

    async def rolling_std(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="std", **kwargs)

    async def rolling_var(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg=self._var_agg(), **kwargs)

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

            # --- 1. Clone the source ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Resolve ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                # use the cloned table directly
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

            # --- 3. Add column to the cloned table ---
            await self._add_col(working_table, schema, new_col, "DOUBLE PRECISION")

            # --- 4. Build window expression ---
            func = self._quantile_cont_sql(self.db.quote_identifier(safe_col), q)
            count_over = f"""
                COUNT({self.db.quote_identifier(safe_col)})
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """
            agg_over = f"""
                {func}
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """
            window_sql = f"CASE WHEN {count_over} >= {int(window)} THEN {agg_over} ELSE NULL END"

            # --- 5. Update the column using FROM (avoid CREATE TABLE AS) ---
            rid = self._row_id_col
            await self._exec(f"""
                UPDATE {qualified} AS t
                SET {self.db.quote_identifier(new_col)} = s.val
                FROM (
                    SELECT {rid},
                        {window_sql} AS val
                    FROM {qualified}
                ) AS s
                WHERE t.{rid} = s.{rid}
            """)

            # --- 6. Sample & return ---
            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            return self._success_response(
                "Rolling quantile",
                res,
                new_table=working_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

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
            w = int(window) - 1
            new_col = f"{safe_col}_rolling_sem_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            # --- 3. Add column ---
            await self._add_col(working_table, schema, new_col, "DOUBLE PRECISION")

            # --- 4. Window expression for SEM ---
            std_func = self._std_agg()
            sem_expr = f"""
                ({std_func}({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                /
                SQRT(
                    COUNT({self.db.quote_identifier(safe_col)})
                    OVER (ORDER BY {order_sql} ROWS BETWEEN {w} PRECEDING AND CURRENT ROW)
                ))
            """

            # --- 5. Update ---
            rid = self._row_id_col
            await self._exec(f"""
                UPDATE {qualified} AS t
                SET {self.db.quote_identifier(new_col)} = s.val
                FROM (
                    SELECT {rid},
                        {sem_expr} AS val
                    FROM {qualified}
                ) AS s
                WHERE t.{rid} = s.{rid}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            return self._success_response(
                "Rolling SEM",
                res,
                new_table=working_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

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
                COUNT(DISTINCT {self.db.quote_identifier(safe_col)})
                OVER (
                    ORDER BY {order_sql}
                    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                )
            """

            # Create a brand‑new table from the clone (the result table)
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

    async def rolling_rank(
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

            # --- Clone ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

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
            new_col = f"{safe_col}_rolling_rank_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            # Create the final table with rank using the correlated subquery technique.
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            sql = f"""
            CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
            WITH __base AS (
                SELECT *,
                    ROW_NUMBER() OVER (ORDER BY {order_sql}) AS __rn
                FROM {qualified}
            )
            SELECT b.*,
                (
                    SELECT COUNT(*)
                    FROM __base r
                    WHERE r.__rn BETWEEN b.__rn - {w} AND b.__rn
                    AND r.{self.db.quote_identifier(safe_col)} <= b.{self.db.quote_identifier(safe_col)}
                ) AS {self.db.quote_identifier(new_col)}
            FROM __base b
            """

            await self._exec(sql)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Rolling rank",
                res,
                new_table=result_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # CATEGORICAL API
    # --------------------------------------------------
    async def rolling_first(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="first", **kwargs)

    async def rolling_last(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="last", **kwargs)

    # --------------------------------------------------
    # DATETIME API
    # --------------------------------------------------
    async def rolling_min_datetime(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="MIN", **kwargs)

    async def rolling_max_datetime(self, *args, **kwargs):
        return await self._rolling_agg(*args, agg="MAX", **kwargs)

    async def rolling_mean_datetime(self, *args, **kwargs):
        return await self._rolling_datetime_stat(*args, stat="mean", **kwargs)

    async def rolling_median_datetime(self, *args, **kwargs):
        return await self._rolling_datetime_stat(*args, stat="median", **kwargs)

    async def rolling_mode_datetime(self, *args, **kwargs):
        return await self._rolling_datetime_stat(*args, stat="mode", **kwargs)

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

            # --- 1. Clone table ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Resolve ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
            else:
                temp_row = "__totem_order"
                await self._add_col(working_table, schema, temp_row, "BIGINT")
                q = self._qualified_table(working_table, schema)
                rowid = self._row_id_col
                if rowid == "ctid":
                    await self._exec(
                        f'UPDATE {q} AS t SET "{temp_row}" = rn '
                        f'FROM (SELECT {rowid}, ROW_NUMBER() OVER () AS rn FROM {q}) sub '
                        f'WHERE t.{rowid} = sub.{rowid}'
                    )
                else:
                    await self._exec(
                        f'UPDATE {q} SET "{temp_row}" = rn '
                        f'FROM (SELECT {rowid}, ROW_NUMBER() OVER () AS rn FROM {q}) sub '
                        f'WHERE {q}.{rowid} = sub.{rowid}'
                    )
                safe_orders = [temp_row]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            qualified = self._qualified_table(working_table, schema)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            source_col_type = await self._get_column_sql_type(working_table, schema, safe_col)

            # --- 3. Aggregation mapping ---
            agg_map = {
                "sum": "SUM", "avg": "AVG", "mean": "AVG",
                "min": "MIN", "max": "MAX", "count": "COUNT",
                "first": "FIRST_VALUE", "last": "LAST_VALUE",
                "first_value": "FIRST_VALUE", "last_value": "LAST_VALUE",
            }
            new_cols = []
            used_col_names = set()

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in agg_map:
                    sql_agg = agg_map[key]
                    alias_key = key
                elif key in ("std", "stddev", "stddev_samp"):
                    sql_agg = self._std_agg()
                    alias_key = "std"
                elif key in ("var", "variance", "var_samp"):
                    sql_agg = self._var_agg()
                    alias_key = "var"
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

                # Add column
                result_type = self._window_result_sql_type(
                    source_col_type, alias_key, sql_agg
                )
                await self._add_col(working_table, schema, col_name, result_type)

                # Build aggregation expression with min_periods
                count_over = f"COUNT({self.db.quote_identifier(safe_col)}) OVER (ORDER BY {order_sql} {window_frame})"
                agg_over = f"{sql_agg}({self.db.quote_identifier(safe_col)}) OVER (ORDER BY {order_sql} {window_frame})"

                if min_periods > 1:
                    agg_with_null = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                else:
                    agg_with_null = agg_over

                rid = self._row_id_col
                await self._exec(f"""
                    UPDATE {qualified} AS t
                    SET {self.db.quote_identifier(col_name)} = s.val
                    FROM (
                        SELECT {rid},
                            {agg_with_null} AS val
                        FROM {qualified}
                    ) AS s
                    WHERE t.{rid} = s.{rid}
                """)

            # --- 4. Clean up temporary ordering column ---
            if not order_by and safe_orders[0] == temp_row:
                await self._exec(f'ALTER TABLE {qualified} DROP COLUMN IF EXISTS "{temp_row}"')

            # --- 5. Sample and response ---
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

    async def expanding_sum(self, table, schema, column, order_by=None,
                            min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="SUM", min_periods=min_periods, **kwargs)

    async def expanding_mean(self, table, schema, column, order_by=None,
                             min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="mean", min_periods=min_periods, **kwargs)

    async def expanding_min(self, table, schema, column, order_by=None,
                            min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="MIN", min_periods=min_periods, **kwargs)

    async def expanding_max(self, table, schema, column, order_by=None,
                            min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="MAX", min_periods=min_periods, **kwargs)

    async def expanding_count(self, table, schema, column, order_by=None,
                              min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="COUNT", min_periods=min_periods, **kwargs)

    async def expanding_std(self, table, schema, column, order_by=None,
                            min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg=self._std_agg(), min_periods=min_periods, **kwargs)

    async def expanding_var(self, table, schema, column, order_by=None,
                            min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg=self._var_agg(), min_periods=min_periods, **kwargs)

    async def expanding_first(self, table, schema, column, order_by=None,
                              min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="first", min_periods=min_periods, **kwargs)

    async def expanding_last(self, table, schema, column, order_by=None,
                             min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="last", min_periods=min_periods, **kwargs)

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

            # --- 3. Add column to clone ---
            await self._add_col(working_table, schema, new_col, "DOUBLE PRECISION")

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"COUNT({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"

            base_func = self._quantile_cont_sql(safe_col_quoted, q)
            agg_over = f"{base_func} OVER (ORDER BY {order_sql} {window_frame})"

            if min_periods > 1:
                quantile_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
            else:
                quantile_expr = agg_over

            # --- 4. UPDATE via FROM subquery ---
            rid = self._row_id_col
            await self._exec(f"""
                UPDATE {qualified} AS t
                SET {self.db.quote_identifier(new_col)} = s.val
                FROM (
                    SELECT {rid},
                        {quantile_expr} AS val
                    FROM {qualified}
                ) AS s
                WHERE t.{rid} = s.{rid}
            """)

            # --- 5. Sample & response ---
            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            return self._success_response(
                "Expanding quantile", res,
                new_table=working_table, new_column=new_col,
                q=q, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

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

            # --- 3. Add column ---
            await self._add_col(working_table, schema, new_col, "DOUBLE PRECISION")

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"COUNT({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"

            std_func = self._std_agg()
            sem_base = f"({std_func}({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame}) / SQRT({count_over}))"

            if min_periods > 1:
                sem_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {sem_base} ELSE NULL END"
            else:
                sem_expr = sem_base

            # --- 4. UPDATE ---
            rid = self._row_id_col
            await self._exec(f"""
                UPDATE {qualified} AS t
                SET {self.db.quote_identifier(new_col)} = s.val
                FROM (
                    SELECT {rid},
                        {sem_expr} AS val
                    FROM {qualified}
                ) AS s
                WHERE t.{rid} = s.{rid}
            """)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(working_table, schema, preview_cols)

            return self._success_response(
                "Expanding SEM", res,
                new_table=working_table, new_column=new_col, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def expanding_rank(
        self, table, schema, column,
        order_by=None, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None):
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
            new_col = f"{safe_col}_expanding_rank"
            qualified = self._qualified_table(ordered_table, schema)

            # --- 3. Result table name ---
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            safe_col_quoted = self.db.quote_identifier(safe_col)
            rank_subq = f"""
                SELECT COUNT(*)
                FROM __base r
                WHERE r.__rn <= b.__rn
                AND r.{safe_col_quoted} <= b.{safe_col_quoted}
            """
            count_subq = f"SELECT COUNT(r.{safe_col_quoted}) FROM __base r WHERE r.__rn <= b.__rn"

            if min_periods > 1:
                rank_expr = f"""
                    CASE WHEN ({count_subq}) >= {min_periods}
                        THEN ({rank_subq})
                        ELSE NULL
                    END
                """
            else:
                rank_expr = f"({rank_subq})"

            sql = f"""
            CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
            WITH __base AS (
                SELECT *,
                    ROW_NUMBER() OVER (ORDER BY {order_sql}) AS __rn
                FROM {qualified}
            )
            SELECT b.*, {rank_expr} AS {self.db.quote_identifier(new_col)}
            FROM __base b
            """
            await self._exec(sql)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Expanding rank", res,
                new_table=result_table, new_column=new_col, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

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

            # --- 3. DuckDB path ---
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            safe_col_quoted = self.db.quote_identifier(safe_col)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = f"COUNT({safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"
            count_distinct = f"COUNT(DISTINCT {safe_col_quoted}) OVER (ORDER BY {order_sql} {window_frame})"
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

    # ==============================================================
    # EXPANDING DATETIME STATS
    # ==============================================================
    async def _expanding_datetime_stat(
        self, table, schema, column,
        order_by=None, stat="mean", min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            # --- 1. Clone source table ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Resolve ordering ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                ordered_table = working_table          # use clone directly
            else:
                safe_order, ordered_table = await self._resolve_ordering(
                    working_table, schema, None
                )
                safe_orders = [safe_order]

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            new_col = f"{safe_col}_expanding_{stat}"
            qualified = self._qualified_table(ordered_table, schema)

            # --- 3. Determine result table name ---
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            safe_col_quoted = self.db.quote_identifier(safe_col)
            is_date_only = await self._is_date_only_column(ordered_table, schema, safe_col)

            epoch_expr = self._datetime_epoch_expr(f"r.{safe_col_quoted}")
            from_epoch_expr = self._from_epoch_expr(is_date_only)

            # --- 4. Build inner aggregation subquery ---
            if stat == "mean":
                inner = f"{self._avg_fn()}({epoch_expr})"
            elif stat == "median":
                inner = self._median_epoch_sql(epoch_expr)
            elif stat == "mode":
                # mode is handled differently (value itself)
                inner = None
            else:
                return self._error_response(f"Unsupported datetime expanding stat '{stat}'")

            # --- 5. Build the value expression with min_periods ---
            non_null_count = f"SELECT {self._count_fn()}(r.{safe_col_quoted}) FROM __base r WHERE r.__rn <= b.__rn"

            if stat == "mode":
                mode_subq = f"""
                    SELECT r.{safe_col_quoted}
                    FROM __base r
                    WHERE r.__rn <= b.__rn
                    AND r.{safe_col_quoted} IS NOT NULL
                    GROUP BY r.{safe_col_quoted}
                    ORDER BY COUNT(*) DESC, r.{safe_col_quoted}
                    LIMIT 1
                """
                if min_periods > 1:
                    value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({mode_subq}) ELSE NULL END"
                else:
                    value_sql = f"({mode_subq})"
            else:
                value_subq = f"""
                    SELECT {from_epoch_expr}
                    FROM (SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                        AND r.{safe_col_quoted} IS NOT NULL) __agg
                """
                if min_periods > 1:
                    value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({value_subq}) ELSE NULL END"
                else:
                    value_sql = f"({value_subq})"

            # --- 6. Create the result table from the cloned data ---
            sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY {order_sql}) AS __rn
                    FROM {qualified}
                )
                SELECT b.*, {value_sql} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            await self._exec(sql)

            # --- 7. Sample & response ---
            if order_by:
                preview_cols = [new_col, column] + safe_orders
            else:
                preview_cols = [new_col, column]
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                f"Expanding datetime {stat} on '{column}' (min_periods={min_periods})",
                res, new_table=result_table, new_column=new_col, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(f"expanding datetime error: {str(e)}\n{traceback.format_exc()}")

    async def expanding_min_datetime(self, table, schema, column, order_by=None,
                                     min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="MIN", min_periods=min_periods, **kwargs)

    async def expanding_max_datetime(self, table, schema, column, order_by=None,
                                     min_periods=1, **kwargs):
        return await self._expanding_agg(table, schema, column, order_by=order_by,
                                         agg="MAX", min_periods=min_periods, **kwargs)

    async def expanding_mean_datetime(self, table, schema, column, order_by=None,
                                      min_periods=1, **kwargs):
        return await self._expanding_datetime_stat(table, schema, column, order_by=order_by,
                                                   stat="mean", min_periods=min_periods, **kwargs)

    async def expanding_median_datetime(self, table, schema, column, order_by=None,
                                        min_periods=1, **kwargs):
        return await self._expanding_datetime_stat(table, schema, column, order_by=order_by,
                                                   stat="median", min_periods=min_periods, **kwargs)

    async def expanding_mode_datetime(self, table, schema, column, order_by=None,
                                      min_periods=1, **kwargs):
        return await self._expanding_datetime_stat(table, schema, column, order_by=order_by,
                                                   stat="mode", min_periods=min_periods, **kwargs)

    # ------------------------------------------------------------------
    # EWM (Exponentially Weighted Moving)
    # ------------------------------------------------------------------
    @staticmethod
    def _alpha_from_params(com=None, span=None, halflife=None, alpha=None, default_alpha=0.5):
        """
        Compute smoothing factor α from one of com/span/halflife/alpha.
        If none are provided, returns `default_alpha` (instead of raising).
        """
        # Count explicitly passed parameters
        given = [p for p in (com, span, halflife, alpha) if p is not None]
        if not given:
            # No decay specified → fallback to default
            return default_alpha
        if len(given) != 1:
            raise ValueError("Exactly one of com, span, halflife, or alpha must be provided")
        if alpha is not None:
            if not (0 < alpha <= 1):
                raise ValueError("alpha must be in (0, 1]")
            return alpha
        if com is not None:
            if com < 0:
                raise ValueError("com must be >= 0")
            return 1 / (1 + com)
        if span is not None:
            if span < 1:
                raise ValueError("span must be >= 1")
            return 2 / (span + 1)
        # halflife
        if halflife <= 0:
            raise ValueError("halflife must be > 0")
        return 1 - math.exp(-math.log(2) / halflife)

    async def ewm(
        self,
        table: str,
        schema: str,
        column: str,
        order_by=None,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
        agg="mean",
        backend=None,
        data_id=None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
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

            # --- 1. Clone source table ---
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            # --- 2. Resolve ordering on the clone ---
            if order_by:
                if isinstance(order_by, (list, tuple)):
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                else:
                    safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                qualified = self._qualified_table(working_table, schema)
            else:
                # simulate stable order on the clone
                safe_order, ordered_table = await self._resolve_ordering(working_table, schema, None)
                safe_orders = [safe_order]
                qualified = self._qualified_table(ordered_table, schema)

            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)

            # --- 3. Fetch ordered values from the clone ---
            rows = await self._fetch(
                f"""
                SELECT {self.db.quote_identifier(safe_col)} AS v
                FROM {qualified}
                ORDER BY {order_sql}
                """
            )
            vals = [r["v"] for r in rows]

            # --- 4. Compute alpha ---
            a = self._alpha_from_params(com, span, halflife, alpha)
            arr = np.array([v if v is not None else np.nan for v in vals], dtype=np.float64)

            n = len(arr)
            results = {f: np.full(n, np.nan) for f in aggs}

            # --- 5. Highly Optimized O(n) EWM Calculation ---
            def compute_ewm_ignore_na_false(Z, a, adjust, min_periods, res_dict):
                n_Z = len(Z)
                if n_Z == 0:
                    return

                if adjust:
                    S_num = 0.0
                    S_den = 0.0
                    S_w2 = 0.0
                    S_x2w = 0.0
                    valid_seen = 0

                    for t in range(n_Z):
                        # Decay the states by time step
                        S_num *= (1.0 - a)
                        S_den *= (1.0 - a)
                        S_w2 *= (1.0 - a)**2
                        S_x2w *= (1.0 - a)

                        if not np.isnan(Z[t]):
                            valid_seen += 1
                            S_num += Z[t]
                            S_den += 1.0
                            S_w2 += 1.0
                            S_x2w += Z[t] * Z[t]

                            if valid_seen >= min_periods and S_den > 0:
                                mean_val = S_num / S_den
                                if "mean" in res_dict:
                                    res_dict["mean"][t] = mean_val
                                if "sum" in res_dict:
                                    res_dict["sum"][t] = S_num
                                if "var" in res_dict or "std" in res_dict:
                                    if valid_seen < 2:
                                        var_unbiased = np.nan
                                    else:
                                        var_pop = (S_x2w / S_den) - (mean_val * mean_val)
                                        denom = S_den - (S_w2 / S_den)
                                        if denom <= 0.0:
                                            var_unbiased = np.nan
                                        else:
                                            var_unbiased = var_pop * (S_den / denom)
                                    if not np.isnan(var_unbiased):
                                        # Floating-point cancellation can produce a tiny
                                        # negative value for a mathematically non-negative
                                        # variance.
                                        var_unbiased = max(var_unbiased, 0.0)
                                    if "var" in res_dict:
                                        res_dict["var"][t] = var_unbiased
                                    if "std" in res_dict:
                                        res_dict["std"][t] = np.sqrt(var_unbiased) if not np.isnan(var_unbiased) else np.nan
                else:
                    state = 0.0
                    S_w2 = 0.0
                    S_x2w = 0.0
                    valid_seen = 0
                    has_state = False

                    for t in range(n_Z):
                        if not np.isnan(Z[t]):
                            valid_seen += 1
                            if not has_state:
                                state = Z[t]
                                S_w2 = 1.0
                                S_x2w = Z[t] * Z[t]
                                has_state = True
                            else:
                                state = (1.0 - a) * state + a * Z[t]
                                S_w2 = (1.0 - a)**2 * S_w2 + a**2
                                S_x2w = (1.0 - a) * S_x2w + a * (Z[t] * Z[t])

                            if valid_seen >= min_periods:
                                mean_val = state
                                if "mean" in res_dict:
                                    res_dict["mean"][t] = mean_val
                                if "sum" in res_dict:
                                    res_dict["sum"][t] = state
                                if "var" in res_dict or "std" in res_dict:
                                    if valid_seen < 2:
                                        var_unbiased = np.nan
                                    else:
                                        var_pop = S_x2w - (mean_val * mean_val)
                                        denom = 1.0 - S_w2
                                        if denom <= 0.0:
                                            var_unbiased = np.nan
                                        else:
                                            var_unbiased = var_pop / denom
                                    if not np.isnan(var_unbiased):
                                        var_unbiased = max(var_unbiased, 0.0)
                                    if "var" in res_dict:
                                        res_dict["var"][t] = var_unbiased
                                    if "std" in res_dict:
                                        res_dict["std"][t] = np.sqrt(var_unbiased) if not np.isnan(var_unbiased) else np.nan

            if ignore_na:
                valid_mask = ~np.isnan(arr)
                V = arr[valid_mask]
                temp_results = {f: np.full(len(V), np.nan) for f in aggs}
                await asyncio.to_thread(
                    compute_ewm_ignore_na_false,
                    V, a, adjust, min_periods, temp_results,
                )
                for f in aggs:
                    results[f][valid_mask] = temp_results[f]
            else:
                await asyncio.to_thread(
                    compute_ewm_ignore_na_false,
                    arr, a, adjust, min_periods, results,
                )

            # --- 6. Build result table name ---
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            q_schema = self.db.quote_identifier(schema)
            q_new = self.db.quote_identifier(result_table)

            col_defs = ["rn"] + [f"{safe_col}_ewm_{f}" for f in aggs]

            # VALUES casts must use the active database's type names.
            row_type, value_type = self._ewm_row_types()

            cols_sql = ", ".join(self.db.quote_identifier(c) for c in col_defs)
            ewm_select_cols = ", ".join(
                f"e.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}"
                for c in col_defs[1:]
            )

            if self._ewm_uses_stage_table():
                stage_table = SQLIdentifierSanitizer.sanitize(
                    f"{result_table}__ewm_stage_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
                )
                q_stage = self._qualified_table(stage_table, schema)
                nullable_value_type = f"Nullable({value_type})"
                stage_cols = ", ".join(
                    [f"{self.db.quote_identifier('rn')} {row_type}"]
                    + [
                        f"{self.db.quote_identifier(c)} {nullable_value_type}"
                        for c in col_defs[1:]
                    ]
                )

                await self._exec(f"DROP TABLE IF EXISTS {q_stage}")
                await self._exec(f"CREATE TABLE {q_stage} ({stage_cols}) ENGINE = Memory")

                try:
                    def build_insert_rows(start: int, stop: int) -> List[List[Any]]:
                        rows_out = []
                        for i in range(start, stop):
                            row = [i + 1]
                            for f in aggs:
                                val = results[f][i]
                                row.append(None if np.isnan(val) else float(val))
                            rows_out.append(row)
                        return rows_out

                    insert_chunk_size = 10_000
                    for start in range(0, n, insert_chunk_size):
                        stop = min(start + insert_chunk_size, n)
                        insert_rows = await asyncio.to_thread(build_insert_rows, start, stop)
                        await self.db.insert_rows(
                            f"{schema}.{stage_table}",
                            insert_rows,
                            col_defs,
                        )

                    final_sql = f"""
                    CREATE TABLE {q_schema}.{q_new} AS
                    WITH __base AS (
                        SELECT *,
                            ROW_NUMBER() OVER (ORDER BY {order_sql}) AS rn
                        FROM {qualified}
                    )
                    SELECT b.*,
                        {ewm_select_cols}
                    FROM __base b
                    LEFT JOIN {q_stage} e
                    ON b.rn = e.{self.db.quote_identifier("rn")}
                    """

                    await self._exec(final_sql)
                finally:
                    await self._exec(f"DROP TABLE IF EXISTS {q_stage}")
            else:
                # Build CTE with base (from clone) and ewm values
                base_cte = f"""
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY {order_sql}) AS rn
                    FROM {qualified}
                ),
                __ewm AS (
                    SELECT *
                    FROM (VALUES
                """

                def build_values_chunk(start: int, stop: int) -> str:
                    values_sql_parts = []
                    for i in range(start, stop):
                        parts = [f"CAST({i + 1} AS {row_type})"]
                        for f in aggs:
                            val = results[f][i]
                            if np.isnan(val):
                                parts.append(f"CAST(NULL AS {value_type})")
                            else:
                                parts.append(f"CAST({float(val)} AS {value_type})")
                        values_sql_parts.append("(" + ", ".join(parts) + ")")
                    return ",\n".join(values_sql_parts)

                def build_values_sql() -> str:
                    # Formatting large VALUES payloads is independent by row.
                    # Use bounded chunks to avoid thread overhead on small data.
                    chunk_size = 5_000
                    ranges = [
                        (start, min(start + chunk_size, n))
                        for start in range(0, n, chunk_size)
                    ]
                    if len(ranges) <= 1:
                        return build_values_chunk(0, n)

                    with ThreadPoolExecutor(max_workers=min(4, len(ranges))) as pool:
                        chunks = pool.map(lambda bounds: build_values_chunk(*bounds), ranges)
                        return ",\n".join(chunks)

                values_sql = await asyncio.to_thread(build_values_sql)

                final_sql = f"""
                CREATE TABLE {q_schema}.{q_new} AS
                {base_cte}
                {values_sql}
                ) AS t({cols_sql})
                )
                SELECT b.*,
                    {ewm_select_cols}
                FROM __base b
                LEFT JOIN __ewm e
                ON b.rn = e.{self.db.quote_identifier("rn")}
                """

                await self._exec(final_sql)

            # --- 7. Preview ---
            preview_cols = [f"{safe_col}_ewm_{f}" for f in aggs] + [column]
            if order_by:
                preview_cols += safe_orders
            res_df = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                f"EWM {aggs} on '{column}'",
                res_df,
                new_table=result_table,
                new_columns=[f"{safe_col}_ewm_{f}" for f in aggs],
            )
        except Exception as e:
            return self._error_response(
                f"ewm error: {str(e)}\n{traceback.format_exc()}"
            )
