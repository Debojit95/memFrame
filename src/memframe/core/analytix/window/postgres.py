from collections import deque
from typing import List, Optional, Union

from memframe.core.analytix.window.base import WindowOps
from memframe.utils.helper import SQLIdentifierSanitizer


class PostgresWindowOps(WindowOps):
    """PostgreSQL window engine.

    Overrides the dialect hooks plus the two operations whose SQL is
    structurally different from the shared engine (windowed quantile via a
    correlated subquery, and rolling nunique via a Python fallback).
    """

    # --------------------------------------------------
    # Dialect hooks
    # --------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        return "ctid"

    def _datetime_epoch_expr(self, column_expr: str) -> str:
        return f"EXTRACT(EPOCH FROM {column_expr})"

    def _median_epoch_sql(self, epoch_expr: str) -> str:
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})"

    def _std_agg(self) -> str:
        return "STDDEV"

    def _var_agg(self) -> str:
        return "VARIANCE"

    def _quantile_cont_sql(self, col_quoted: str, q) -> str:
        return f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {col_quoted})"

    def _ewm_row_types(self) -> tuple:
        return "BIGINT", "DOUBLE PRECISION"

    # --------------------------------------------------
    # Row-id helpers (used by the Python fallbacks)
    # --------------------------------------------------
    async def _ensure_row_id(self, table: str, schema: str, order_col: Union[str, List[str]]):
        """Add a temporary row-id column with a sequential row number ordered by the provided keys."""
        q = self._qualified_table(table, schema)
        if isinstance(order_col, (list, tuple)):
            safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_col]
        else:
            safe_orders = [SQLIdentifierSanitizer.sanitize(order_col)]
        order_sql = ", ".join(f'"{c}"' for c in safe_orders)
        await self._add_col(table, schema, self.TMP_ROW, "BIGINT")

        await self._exec(
            f'UPDATE {q} AS t SET "{self.TMP_ROW}" = rn FROM '
            f'(SELECT ctid, ROW_NUMBER() OVER (ORDER BY {order_sql}) AS rn FROM {q}) sub '
            f'WHERE t.ctid = sub.ctid'
        )

    async def _drop_tmp_row(self, table: str, schema: str):
        q = self._qualified_table(table, schema)
        await self._exec(f'ALTER TABLE {q} DROP COLUMN IF EXISTS "{self.TMP_ROW}"')

    # --------------------------------------------------
    # Rolling quantile (ordered-set aggregate cannot take a window frame)
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

            # --- 3. Postgres: ordered-set aggregates reject OVER() with a
            # ---    frame, so compute the quantile per-row in a CTAS.
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            q_col = self.db.quote_identifier(safe_col)
            q_rn = self.db.quote_identifier("__totem_quant_rn")
            sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY {order_sql}) AS {q_rn}
                    FROM {qualified}
                )
                SELECT b.*,
                    CASE WHEN (
                        SELECT COUNT(r.{q_col})
                        FROM __base r
                        WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                    ) >= {int(window)}
                    THEN (
                        SELECT PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY r.{q_col})
                        FROM __base r
                        WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                    )
                    ELSE NULL END AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            await self._exec(sql)

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
    # Rolling nunique (Python fallback)
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
            new_col = f"{safe_col}_rolling_nunique_w{window}"

            qualified = self._qualified_table(ordered_table, schema)

            # --- Postgres fallback (Python) ---
            rows = await self._fetch(
                f"SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} ORDER BY {order_sql}"
            )
            window_q = deque(maxlen=window)
            result_vals = []
            for r in rows:
                v = r[safe_col]
                window_q.append(v)
                result_vals.append(len(set(window_q)))

            # We still create a result table (cloned) and add the column, then update it
            result_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            await self._exec(f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(result_table)} AS
                SELECT * FROM {qualified}
            """)
            await self._add_col(result_table, schema, new_col, "INTEGER")

            # Update using a temporary row‑id (we reuse _ensure_row_id pattern)
            await self._ensure_row_id(result_table, schema, safe_orders)
            q_res = self._qualified_table(result_table, schema)
            # Fetch the ordered row ids
            rows_new = await self._fetch(
                f'SELECT "{self.TMP_ROW}" FROM {q_res} ORDER BY {order_sql}'
            )
            row_ids = [r[self.TMP_ROW] for r in rows_new]

            CHUNK = 500
            for i in range(0, len(result_vals), CHUNK):
                chunk_counts = result_vals[i:i+CHUNK]
                chunk_ids = row_ids[i:i+CHUNK]
                flat = []
                clauses = []
                for j, (cnt, rid) in enumerate(zip(chunk_counts, chunk_ids)):
                    flat.extend([cnt, rid])
                    clauses.append(f"(${2*j+1}::int, ${2*j+2}::bigint)")
                vsql = ", ".join(clauses)
                await self._exec(
                    f'UPDATE {q_res} SET "{new_col}" = v.cnt '
                    f'FROM (VALUES {vsql}) AS v(cnt, id) '
                    f'WHERE "{self.TMP_ROW}" = v.id',
                    *flat,
                )

            await self._drop_tmp_row(result_table, schema)

            preview_cols = [new_col, column] + safe_orders
            res = await self._fetch_data(result_table, schema, preview_cols)

            return self._success_response(
                "Rolling nunique (python fallback)",
                res,
                new_table=result_table,
                new_column=new_col,
            )
        except Exception as e:
            return self._error_response(str(e))
