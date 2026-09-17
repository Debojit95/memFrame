from __future__ import annotations
import traceback
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class DataCumulativeOps:
    """
    Core cumulative operations executed on the database.
    Every method creates a new transient table, adds the cumulative
    column there, and returns a standardised response with the new table
    name (identical pattern to other core classes).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Backend‑specific row identifier
    # ------------------------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        if isinstance(self.db, PostgresAdapter):
            return "ctid"
        elif isinstance(self.db, DuckDBAdapter):
            return "rowid"
        # ClickHouse has no native rowid — use synthetic _ch_rowid
        # (only reached if caller uses the PG/DuckDB code path)
        elif isinstance(self.db, ClickHouseAdapter):
            return "_ch_rowid"
        else:
            raise self._unsupported_backend_error()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _fetch_sample(self, table: str, schema: str,
                            columns: List[str]) -> pd.DataFrame:
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
        self, table: str, schema: str, column: str,
        col_type: str = "DOUBLE PRECISION"
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
            candidate = f"{safe_table}__op_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"

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

        if isinstance(self.db, ClickHouseAdapter):
            await self._exec(
                f"CREATE TABLE {qualified_target} "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {qualified_source}"
            )
        else:
            await self._exec(f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source}")

        return output_table

    # ------------------------------------------------------------------
    # Response builders
    # ------------------------------------------------------------------
    def _success_response(
        self, message: str, involved_cols: List[str], generated_cols: List[str],
        sample_df: pd.DataFrame, **extra
    ) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols,
            "generated_cols": generated_cols,
            "result": sample_df,
            **extra,
        }

    def _error_response(
        self, error_message: str,
        involved_cols: List[str] = None,
        generated_cols: List[str] = None
    ) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for cumulative operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Generic cumulative operation
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
        """
        Create a new transient table with a cumulative column.

        PostgreSQL / DuckDB:
            Clone → ALTER ADD COLUMN → UPDATE ... FROM (subquery using ctid/rowid)

        ClickHouse:
            Single CTAS with window function computed inline.
            (Avoids async UPDATE mutations which can return stale data.)
        """
        try:
            # ==============================================================
            #  PostgreSQL / DuckDB  (UNCHANGED)
            # ==============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                # 1. Clone source table into a working transient table
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                user_provided_order = bool(order_cols)
                resolved_order_cols = self._resolve_order(order_cols)
                safe_orders = [
                    SQLIdentifierSanitizer.sanitize(c) for c in resolved_order_cols
                ]
                display_orders = safe_orders if user_provided_order else []
                # Build ORDER BY clause
                order_parts = ", ".join(
                    self.db.quote_identifier(c) for c in safe_orders
                )
                window_spec = (
                    f"ORDER BY {order_parts} "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                )
                # Complete window expression
                if "{window_spec}" in window_expr:
                    full_window = window_expr.replace("{window_spec}", window_spec)
                else:
                    full_window = f"{window_expr} OVER ({window_spec})"

                tgt = target_col or f"cum_{column}_{operation_name}"
                tgt_safe = SQLIdentifierSanitizer.sanitize(tgt)
                await self._add_column_if_not_exists(
                    working_table, schema, tgt_safe, target_type)

                rid = self._row_id_col
                schema_q = self.db.quote_identifier(
                    SQLIdentifierSanitizer.sanitize(schema))
                table_q = self.db.quote_identifier(
                    SQLIdentifierSanitizer.sanitize(working_table))

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
                    message = (
                        f"Cumulative {operation_name} of '{column}' "
                        f"ordered by {order_parts} → {tgt_safe}"
                    )
                else:
                    message = (
                        f"Cumulative {operation_name} of '{column}' "
                        f"(default row order) → {tgt_safe}"
                    )

                return self._success_response(
                    message,
                    [column] + display_orders,
                    [tgt_safe],
                    sample,
                    operation=operation_name,
                    new_table=working_table,
                )

            # ==============================================================
            #  ClickHouse  (NEW)
            # ==============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                # ── 1. Resolve table name ──
                working_table = await self._resolve_output_table_name(
                    table, schema, backend=backend,
                    data_id=data_id, new_table=new_table,
                )

                user_provided_order = bool(order_cols)

                # ── 2. Resolve ORDER BY for window ──
                if user_provided_order:
                    resolved = (
                        order_cols if isinstance(order_cols, list) else [order_cols]
                    )
                    safe_orders = [
                        SQLIdentifierSanitizer.sanitize(c) for c in resolved
                    ]
                    order_sql = ", ".join(
                        self.db.quote_identifier(c) for c in safe_orders
                    )
                    display_orders = safe_orders
                else:
                    # No user order → synthetic rowid via subquery
                    order_sql = "_ch_rowid"
                    display_orders = []

                # ── 3. Build window spec & full expression ──
                window_spec = (
                    f"ORDER BY {order_sql} "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                )

                if "{window_spec}" in window_expr:
                    full_window = window_expr.replace("{window_spec}", window_spec)
                else:
                    full_window = f"{window_expr} OVER ({window_spec})"

                # ── 4. Build CTAS ──
                tgt = target_col or f"cum_{column}_{operation_name}"
                tgt_safe = SQLIdentifierSanitizer.sanitize(tgt)
                tgt_q = self.db.quote_identifier(tgt_safe)

                qualified_source = self._qualified_table(table, schema)
                qualified_target = (
                    f"{self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(schema))}"
                    f".{self.db.quote_identifier(working_table)}"
                )

                if user_provided_order:
                    # Direct CTAS — no synthetic rowid needed
                    create_sql = f"""
                        CREATE TABLE {qualified_target}
                        ENGINE = MergeTree()
                        ORDER BY tuple()
                        AS SELECT *, {full_window} AS {tgt_q}
                        FROM {qualified_source}
                    """
                else:
                    # Subquery adds _ch_rowid via ROW_NUMBER()
                    # so the window function has a deterministic order
                    create_sql = f"""
                        CREATE TABLE {qualified_target}
                        ENGINE = MergeTree()
                        ORDER BY tuple()
                        AS
                        SELECT *, {full_window} AS {tgt_q}
                        FROM (
                            SELECT *, ROW_NUMBER() OVER() AS _ch_rowid
                            FROM {qualified_source}
                        )
                    """

                await self._exec(create_sql)

                # ── 5. Sample & response ──
                cols_to_fetch = [column] + display_orders + [tgt_safe]
                sample = await self._fetch_sample(
                    working_table, schema, cols_to_fetch
                )

                if user_provided_order:
                    order_display = ", ".join(display_orders)
                    message = (
                        f"Cumulative {operation_name} of '{column}' "
                        f"ordered by {order_display} → {tgt_safe}"
                    )
                else:
                    message = (
                        f"Cumulative {operation_name} of '{column}' "
                        f"(default row order) → {tgt_safe}"
                    )

                return self._success_response(
                    message,
                    [column] + display_orders,
                    [tgt_safe],
                    sample,
                    operation=operation_name,
                    new_table=working_table,
                )

            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(
                f"Cumulative {operation_name} error: {str(e)}\n"
                f"{traceback.format_exc()}",
            )

    # ------------------------------------------------------------------
    # Resolve ordering – defaults to row identifier
    # ------------------------------------------------------------------
    def _resolve_order(
        self, order_col: Union[str, List[str], None]
    ) -> List[str]:
        if order_col is None:
            return [self._row_id_col]
        if isinstance(order_col, str):
            return [order_col]
        return order_col if order_col else [self._row_id_col]

    # ==================================================================
    #  PUBLIC CUMULATIVE METHODS
    # ==================================================================
    async def cumsum(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"SUM({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="sum",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"SUM({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="sum",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cumprod(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"EXP(SUM(LN(NULLIF({col_q}, 0))) OVER ({{window_spec}}))"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="prod",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"EXP(SUM(LN(NULLIF({col_q}, 0))) OVER ({{window_spec}}))"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="prod",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cummax(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"MAX({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="max",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"MAX({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="max",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cummin(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"MIN({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="min",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"MIN({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="min",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cummean(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"AVG({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="mean",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"AVG({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="mean",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cumcount(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"COUNT({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="count",
                order_cols=order_col, target_col=target_col,
                target_type="BIGINT",
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"COUNT({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="count",
                order_cols=order_col, target_col=target_col,
                target_type="UInt64",
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cumstd(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"STDDEV_POP({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="std",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"stddevPop({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="std",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()

    async def cumvar(
        self, table: str, schema: str, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        backend=None, data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"VAR_POP({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="var",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        elif isinstance(self.db, ClickHouseAdapter):
            col_q = self.db.quote_identifier(
                SQLIdentifierSanitizer.sanitize(column))
            window = f"varPop({col_q})"
            return await self._cumulative_op(
                table=table, schema=schema, column=column,
                window_expr=window, operation_name="var",
                order_cols=order_col, target_col=target_col,
                backend=backend, data_id=data_id, new_table=new_table,
            )
        else:
            raise self._unsupported_backend_error()