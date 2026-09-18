from __future__ import annotations

import traceback
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from memframe.core.analytix._response import fail, ok
from memframe.core.analytix.cumulative.base import CumulativeOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseCumulativeOps(CumulativeOps):
    """ClickHouse backend.

    Dialect hooks (native ``stddevPop``/``varPop``, ``UInt64`` count type,
    synthetic ``_ch_rowid``) plus one structural override: the window is
    computed inline in a single CTAS because asynchronous ``UPDATE``
    mutations can return stale data.
    """

    _std_fn = "stddevPop"
    _var_fn = "varPop"
    _count_col_type = "UInt64"

    @property
    def _row_id_col(self) -> str:
        # ClickHouse has no native rowid — use synthetic _ch_rowid
        return "_ch_rowid"

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
        """Single CTAS with the window function computed inline."""
        try:
            # ── 1. Resolve table name ──
            working_table = await self._resolve_output_table_name(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )

            user_provided_order = bool(order_cols)

            # ── 2. Resolve ORDER BY for window ──
            if user_provided_order:
                resolved = order_cols if isinstance(order_cols, list) else [order_cols]
                safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in resolved]
                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                display_orders = safe_orders
            else:
                # No user order → synthetic rowid via subquery
                order_sql = self._row_id_col
                display_orders = []

            # ── 3. Build window spec & full expression ──
            window_spec = (
                f"ORDER BY {order_sql} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            )
            full_window = self._complete_window(window_expr, window_spec)

            # ── 4. Build CTAS ──
            tgt_safe = self._target_col(column, operation_name, target_col)
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
                        SELECT *, ROW_NUMBER() OVER() AS {self._row_id_col}
                        FROM {qualified_source}
                    )
                """

            await self._exec(create_sql)

            # ── 5. Sample & response ──
            cols_to_fetch = [column] + display_orders + [tgt_safe]
            sample: pd.DataFrame = await self._fetch_sample(
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
