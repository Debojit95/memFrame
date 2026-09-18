from __future__ import annotations

import traceback
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from memframe.core.analytix._response import fail, ok
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class CumulativeOps:
    """
    Shared infrastructure for cumulative operations executed on the database.

    All 8 operational methods and the shared helpers are defined once here
    on DuckDB-flavoured defaults; postgres.py overrides the row-identifier
    hook (``ctid``), clickhouse.py overrides the function-name / column-type
    hooks (``stddevPop``/``varPop``, ``UInt64``) plus one structural override
    (``_cumulative_op`` computes the window inline in a single CTAS because
    asynchronous ``UPDATE`` mutations can return stale data).
    """

    # ------------------------------------------------------------------
    # Dialect hooks (overridden per backend)
    # ------------------------------------------------------------------
    _std_fn = "STDDEV_POP"
    _var_fn = "VAR_POP"
    _count_col_type = "BIGINT"

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Backend‑specific row identifier
    # ------------------------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        # ponytail: DuckDB-flavoured default; postgres.py returns "ctid",
        # clickhouse.py returns the synthetic "_ch_rowid".
        return "rowid"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _fetch_sample(
        self, table: str, schema: str, columns: List[str]
    ) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if not columns:
            return pd.DataFrame()
        sanitized = [
            SQLIdentifierSanitizer.sanitize(str(c), allow_qualified=False)
            for c in columns
        ]
        col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    async def _add_column_if_not_exists(
        self, table: str, schema: str, column: str, col_type: str = "DOUBLE PRECISION"
    ) -> None:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        try:
            await self._exec(
                f"SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1"
            )
        except Exception:
            await self._exec(
                f"ALTER TABLE {qualified} ADD COLUMN "
                f"{self.db.quote_identifier(safe_col)} {col_type}"
            )

    # ------------------------------------------------------------------
    # Transient table helpers
    # ------------------------------------------------------------------
    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await backend.fetchval(sql, *args)

    async def _generate_transient_table_name(
        self, base_table: str, backend, data_id: str
    ) -> str:
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
            candidate = await self._generate_transient_table_name(
                safe_table, backend, data_id
            )
        else:
            candidate = (
                f"{safe_table}__op_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
            )

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
        qualified_target = f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}"

        await self._exec(
            f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source}"
        )

        return output_table

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for cumulative operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Window-expression helpers (shared by base and clickhouse override)
    # ------------------------------------------------------------------
    def _resolve_order_columns(
        self, order_cols: Union[str, List[str], None]
    ) -> List[str]:
        user_provided_order = bool(order_cols)
        if user_provided_order:
            resolved = order_cols if isinstance(order_cols, list) else [order_cols]
        else:
            resolved = self._resolve_order(order_cols)
        return [SQLIdentifierSanitizer.sanitize(c) for c in resolved]

    def _window_spec(self, safe_orders: List[str]) -> str:
        order_parts = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
        return (
            f"ORDER BY {order_parts} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        )

    def _complete_window(self, window_expr: str, window_spec: str) -> str:
        if "{window_spec}" in window_expr:
            return window_expr.replace("{window_spec}", window_spec)
        return f"{window_expr} OVER ({window_spec})"

    def _target_col(
        self, column: str, operation_name: str, target_col: Optional[str]
    ) -> str:
        tgt = target_col or f"cum_{column}_{operation_name}"
        return SQLIdentifierSanitizer.sanitize(tgt)

    # ------------------------------------------------------------------
    # Generic cumulative operation (PostgreSQL / DuckDB):
    # Clone → ALTER ADD COLUMN → UPDATE ... FROM (subquery using ctid/rowid)
    # ------------------------------------------------------------------
    async def _cumulative_op(
        self,
        table: str,
        schema: str,
        column: str,
        window_expr: str,
        operation_name: str,
        order_cols: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        target_type: str = "DOUBLE PRECISION",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new transient table with a cumulative column."""
        try:
            # 1. Clone source table into a working transient table
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            user_provided_order = bool(order_cols)
            safe_orders = self._resolve_order_columns(order_cols)
            display_orders = safe_orders if user_provided_order else []
            # Build ORDER BY clause
            window_spec = self._window_spec(safe_orders)
            # Complete window expression
            full_window = self._complete_window(window_expr, window_spec)

            tgt_safe = self._target_col(column, operation_name, target_col)
            await self._add_column_if_not_exists(
                working_table, schema, tgt_safe, target_type
            )

            rid = self._row_id_col
            schema_q = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(schema))
            table_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(working_table)
            )

            sql = f"""
                UPDATE {schema_q}.{table_q} AS t
                SET {self.db.quote_identifier(tgt_safe)} = s.val
                FROM (
                    SELECT {rid},
                           {full_window} AS val
                    FROM {schema_q}.{table_q}
                ) AS s
                WHERE t.{rid} = s.{rid}
            """
            await self._exec(sql)

            cols_to_fetch = [column] + display_orders + [tgt_safe]
            sample = await self._fetch_sample(working_table, schema, cols_to_fetch)

            if user_provided_order:
                order_parts = ", ".join(
                    self.db.quote_identifier(c) for c in safe_orders
                )
                message = (
                    f"Cumulative {operation_name} of '{column}' "
                    f"ordered by {order_parts} → {tgt_safe}"
                )
            else:
                message = (
                    f"Cumulative {operation_name} of '{column}' "
                    f"(default row order) → {tgt_safe}"
                )

            return ok(
                message,
                [column] + display_orders,
                [tgt_safe],
                sample,
                operation=operation_name,
                new_table=working_table,
            )

        except Exception as e:
            return fail(
                f"Cumulative {operation_name} error: {str(e)}\n"
                f"{traceback.format_exc()}",
            )

    # ------------------------------------------------------------------
    # Resolve ordering – defaults to row identifier
    # ------------------------------------------------------------------
    def _resolve_order(self, order_col: Union[str, List[str], None]) -> List[str]:
        if order_col is None:
            return [self._row_id_col]
        if isinstance(order_col, str):
            return [order_col]
        return order_col if order_col else [self._row_id_col]

    # ------------------------------------------------------------------
    # Window-expression builders per operation (hook points)
    # ------------------------------------------------------------------
    def _col_q(self, column: str) -> str:
        return self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))

    def _window_sum(self, column: str) -> str:
        return f"SUM({self._col_q(column)})"

    def _window_prod(self, column: str) -> str:
        return f"EXP(SUM(LN(NULLIF({self._col_q(column)}, 0))) OVER ({{window_spec}}))"

    def _window_max(self, column: str) -> str:
        return f"MAX({self._col_q(column)})"

    def _window_min(self, column: str) -> str:
        return f"MIN({self._col_q(column)})"

    def _window_mean(self, column: str) -> str:
        return f"AVG({self._col_q(column)})"

    def _window_count(self, column: str) -> str:
        return f"COUNT({self._col_q(column)})"

    def _window_std(self, column: str) -> str:
        return f"{self._std_fn}({self._col_q(column)})"

    def _window_var(self, column: str) -> str:
        return f"{self._var_fn}({self._col_q(column)})"

    # ==================================================================
    #  PUBLIC CUMULATIVE METHODS
    # ==================================================================
    async def cumsum(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_sum(column),
            operation_name="sum",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cumprod(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_prod(column),
            operation_name="prod",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cummax(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_max(column),
            operation_name="max",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cummin(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_min(column),
            operation_name="min",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cummean(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_mean(column),
            operation_name="mean",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cumcount(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_count(column),
            operation_name="count",
            order_cols=order_col,
            target_col=target_col,
            target_type=self._count_col_type,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cumstd(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_std(column),
            operation_name="std",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

    async def cumvar(
        self,
        table: str,
        schema: str,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._cumulative_op(
            table=table,
            schema=schema,
            column=column,
            window_expr=self._window_var(column),
            operation_name="var",
            order_cols=order_col,
            target_col=target_col,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )
