"""
Core group‑by cumulative operations executed directly on the database.
Adds columns with window functions partitioned by group columns.
Now creates a new table for every operation (consistent with other ops).
Supports DuckDB, PostgreSQL, and ClickHouse.
"""

from __future__ import annotations
import traceback
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from datetime import datetime, timezone

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class GroupbyCumulativeOps:
    """
    Low‑level group‑by cumulative aggregation using SQL window functions.
    Supports DuckDB, PostgreSQL, and ClickHouse.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Backend‑specific row identifier (used only for ordering fallback)
    # ------------------------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        if isinstance(self.db, PostgresAdapter):
            return "ctid"
        elif isinstance(self.db, DuckDBAdapter):
            return "rowid"
        elif isinstance(self.db, ClickHouseAdapter):                     # ← ADD
            # ClickHouse has no physical row ID.
            # When order_cols is None, _groupby_cumulative_op uses a
            # ROW_NUMBER() OVER() subquery fallback instead of this.
            raise NotImplementedError(
                "ClickHouse has no physical row identifier. "
                "Provide order_cols, or rely on the ROW_NUMBER() fallback "
                "in _groupby_cumulative_op."
            )
        else:
            raise self._unsupported_backend_error()

    # ------------------------------------------------------------------
    # Core helpers (mirror WindowOps)
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    # ---------- Transient table helpers ----------
    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await self._fetchval(sql, *args)

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

    # ------------------------------------------------------------------
    # Response builders (consistent format)
    # ------------------------------------------------------------------
    def _success_response(
        self, message: str,
        result: pd.DataFrame,
        new_table: str,
        new_columns: List[str],
        group_cols: List[str],
        **extra
    ) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": result,
            "new_table": new_table,
            "new_columns": new_columns,
            "group_cols": group_cols,
            **extra,
        }

    def _error_response(self, error_message: str, **extra) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "result": None,
            "new_table": None,
            "new_columns": [],
            **extra,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for groupby cumulative operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Generic cumulative operation – now creates a new table
    # ------------------------------------------------------------------
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
    ) -> Dict[str, Any]:
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING — unchanged)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id are required")

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                # Resolve ordering (may be None -> fallback to physical rowid)
                if order_cols is None:
                    order_sql = self._row_id_col
                    safe_orders = [order_sql]
                else:
                    if isinstance(order_cols, str):
                        order_cols = [order_cols]
                    safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_cols]
                    order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)

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

                auto_tgt = f"cum_{safe_col}_{operation_name}_by_{'_'.join(safe_groups)}"
                if order_cols:
                    auto_tgt = f"{auto_tgt}_order_by_{'_'.join(safe_orders)}"
                tgt = target_col or auto_tgt
                tgt_safe = SQLIdentifierSanitizer.sanitize(tgt)

                output_table = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(output_table, schema)
                source_qualified = self._qualified_table(table, schema)

                create_sql = f"""
                CREATE TABLE {output_qualified} AS
                SELECT *,
                    {full_window} AS {self.db.quote_identifier(tgt_safe)}
                FROM {source_qualified}
                """
                await self._exec(create_sql)

                cols_to_fetch = [safe_col] + safe_groups
                if order_cols:
                    cols_to_fetch += safe_orders
                cols_to_fetch.append(tgt_safe)
                cols_to_fetch = list(dict.fromkeys(cols_to_fetch))
                rows = await self._fetch(
                    f"SELECT {', '.join(self.db.quote_identifier(c) for c in cols_to_fetch)} "
                    f"FROM {output_qualified}"
                )
                sample = pd.DataFrame(rows, columns=cols_to_fetch) if rows else pd.DataFrame(columns=cols_to_fetch)

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
                )

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
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
                )

            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(
                f"Cumulative {operation_name} error: {str(e)}\n{traceback.format_exc()}",
                group_cols=safe_groups if 'safe_groups' in locals() else [],
            )

    # ==================================================================
    #  PUBLIC GROUP‑BY CUMULATIVE METHODS
    # ==================================================================
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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
            window = f"COUNT({col_q})"
            return await self._groupby_cumulative_op(
                table, schema, column,
                group_cols=group_cols,
                window_expr=window,
                operation_name="count",
                order_cols=order_col,
                target_col=target_col,
                target_type="BIGINT",
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
            window = f"STDDEV_POP({col_q})"
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
            # ClickHouse native name: stddevPop (STDDEV_POP works as alias,
            # but using the canonical name avoids any version surprises)
            window = f"stddevPop({col_q})"
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
            )
        else:
            raise self._unsupported_backend_error()

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
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
            window = f"VAR_POP({col_q})"
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
            )
        elif isinstance(self.db, ClickHouseAdapter):                    # ← ADD
            col_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
            # ClickHouse native name: varPop (VAR_POP works as alias,
            # but using the canonical name avoids any version surprises)
            window = f"varPop({col_q})"
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
            )
        else:
            raise self._unsupported_backend_error()