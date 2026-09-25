"""
Core group‑by cumulative operations executed directly on the database.
Adds columns with window functions partitioned by group columns.
Now creates a new table for every operation (consistent with other ops).
Supports DuckDB, PostgreSQL, and ClickHouse.
"""

from __future__ import annotations
import json
import traceback
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from datetime import datetime, timezone

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class GroupbyCumulativeOps:
    """
    Low‑level group‑by cumulative aggregation using SQL window functions.
    Supports DuckDB, PostgreSQL, and ClickHouse.

    All shared helpers and the operational methods are defined once here
    on DuckDB-flavoured defaults; postgres.py overrides only the map-back
    table swap, clickhouse.py overrides the ``stddevPop``/``varPop``
    spellings plus two structural overrides (MergeTree CTAS with the
    ``_mf_row_num`` fallback, and the create-swap-``EXCHANGE`` map-back,
    because asynchronous ``UPDATE`` mutations can return stale data).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Backend‑specific row identifier (used only for ordering fallback)
    # ------------------------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        # ponytail: DuckDB-flavoured default; postgres.py returns "ctid",
        # clickhouse.py raises (no physical row id — the ROW_NUMBER()
        # fallback in _groupby_cumulative_op is used instead).
        return "rowid"

    # ------------------------------------------------------------------
    # Window-function dialect hooks (DuckDB-flavoured defaults)
    # ------------------------------------------------------------------
    def _std_window(self, col_q: str) -> str:
        return f"STDDEV_POP({col_q})"

    def _var_window(self, col_q: str) -> str:
        return f"VAR_POP({col_q})"

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
    # map_feature helpers: LEFT JOIN the new feature column back onto the
    # original table on full row identity (one output row per input row).
    # Marker rows in the transient registry tell our own re-runs apart
    # from foreign column collisions.
    # ------------------------------------------------------------------
    def _map_sig(
        self,
        group_cols: List[str],
        column: str,
        operation_name: str,
        order_cols: Optional[List[str]],
        target_col: str,
        new_columns: List[str],
    ) -> str:
        return json.dumps(
            {
                "group_cols": group_cols,
                "column": column,
                "operation": operation_name,
                "order_cols": order_cols or [],
                "target_col": target_col,
                "new_columns": new_columns,
            },
            sort_keys=True,
        )

    async def _check_map_collision(
        self,
        table: str,
        schema: str,
        backend,
        data_id: str,
        sig: str,
        new_columns: List[str],
    ) -> Optional[str]:
        """Drop our own re-run columns; error on foreign collisions."""
        try:
            orig_cols = set(await self.db.get_column_types(table, schema) or {})
        except Exception:
            orig_cols = set()
        clashes = [c for c in new_columns if c in orig_cols]
        if not clashes:
            return None
        rows = await self._fetch(
            f"""SELECT kwargs FROM {backend.transient_registry_table}
                WHERE data_id = {backend.placeholder(1)}
                  AND operation_type = 'groupby_cum_map'
                  AND generated_table_name = {backend.placeholder(2)}""",
            data_id,
            table,
        )
        for row in rows or []:
            try:
                # ponytail: adapters disagree (dicts on DuckDB, tuples on
                # Postgres) — read both shapes.
                stored = row.get("kwargs") if isinstance(row, dict) else row[0]
                if json.loads(stored or "{}") == json.loads(sig):
                    for col in clashes:
                        await self._exec(
                            f"ALTER TABLE {self._qualified_table(table, schema)} "
                            f"DROP COLUMN {self.db.quote_identifier(col)}"
                        )
                    return None
            except Exception:
                continue
        return (
            f"map_feature: column(s) {clashes} already exist on "
            f"{schema}.{table} and were not created by a previous identical "
            f"group-by cumulative mapping. Drop or rename them first."
        )

    async def _record_map_marker(
        self,
        table: str,
        schema: str,
        backend,
        data_id: str,
        sig: str,
    ) -> None:
        max_op = await self._backend_fetch_val(
            backend,
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        opidx = (max_op or 0) + 1
        await self._exec(
            f"""INSERT INTO {backend.transient_registry_table}
                (data_id, opidx, operation_type, class_name, method_name,
                 args, kwargs, generated_table_name, is_deep_cache, schema)
                VALUES ({backend.placeholder(1)}, {backend.placeholder(2)},
                        'groupby_cum_map', 'GroupbyCumulativeOps', 'map_feature',
                        {backend.placeholder(3)}, {backend.placeholder(4)},
                        {backend.placeholder(5)}, {backend.placeholder(6)},
                        {backend.placeholder(7)})""",
            data_id, opidx, "", sig, table, False, schema,
        )

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
        await self._exec(
            f"""CREATE OR REPLACE TABLE {orig_q} AS
                SELECT o.*, {feats}
                FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
        )
        await self._record_map_marker(table, schema, backend, data_id, sig)

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
    # Generic cumulative operation – creates a new table
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
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        try:
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

            if map_feature:
                await self._apply_map(
                    table, schema, output_table, [tgt_safe],
                    backend, data_id, map_sig,
                )

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
                mapped_table=table if map_feature else None,
                mapped_columns=[tgt_safe] if map_feature else [],
            )

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
            target_type="BIGINT",
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
        backend=None,
        data_id=None,
        new_table=None,
        target_col: Optional[str] = None,
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
