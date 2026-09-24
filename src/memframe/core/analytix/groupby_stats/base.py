"""
Core group-by aggregation operations.
Generates a GROUP BY query, stores the result in a new table,
and returns a preview DataFrame with metadata.
Supports DuckDB, PostgreSQL, and ClickHouse.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Any
import json
import traceback
from datetime import datetime, timezone
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class GroupByStatsOps:
    """
    Low‑level group‑by aggregations. Uses the provided DatabaseAdapter.
    Supports DuckDB, PostgreSQL, and ClickHouse.

    All shared helpers and both operational methods are defined once here
    on DuckDB-flavoured defaults; postgres.py overrides the median and
    epoch-extraction hooks, clickhouse.py overrides the function-name
    hooks (``stddevPop``/``varPop``, ``topK`` mode, ``toUnixTimestamp``)
    plus two structural overrides (MergeTree CTAS and the
    create-swap-``EXCHANGE`` map-back, because asynchronous ``UPDATE``
    mutations can return stale data).
    """

    # ------------------------------------------------------------------
    # Dialect hooks (overridden per backend)
    # ------------------------------------------------------------------
    _count_fn = "COUNT(*)"

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Helpers (mirroring other core ops)
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
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    # ---------- Transient-table helpers (mirror WindowOps) ----------
    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await self.db.fetchval(sql, *args)

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
    # Aggregate-function dialect hooks (DuckDB-flavoured defaults)
    # ------------------------------------------------------------------
    def _std_expr(self, qcol: str) -> str:
        return f"STDDEV_POP({qcol})"

    def _var_expr(self, qcol: str) -> str:
        return f"VAR_POP({qcol})"

    def _sem_expr(self, qcol: str) -> str:
        return f"STDDEV_POP({qcol}) / SQRT(COUNT({qcol}))"

    def _product_expr(self, qcol: str) -> str:
        return f"EXP(SUM(LN(NULLIF({qcol}, 0))))"

    def _median_expr(self, qcol: str) -> str:
        return f"MEDIAN({qcol})"

    def _mode_expr(self, qcol: str) -> str:
        return f"MODE({qcol})"

    def _elapsed_seconds_expr(self, qcol: str) -> str:
        return f"EPOCH(MAX(CAST({qcol} AS TIMESTAMP))) - EPOCH(MIN(CAST({qcol} AS TIMESTAMP)))"

    # ------------------------------------------------------------------
    # Structural hooks (table creation / map-back)
    # ------------------------------------------------------------------
    def _create_table_sql(self, output_table: str, schema: str, select_sql: str) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        output = SQLIdentifierSanitizer.sanitize(output_table)
        return f"""
                CREATE TABLE {self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output)} AS
                {select_sql}
                """

    async def _apply_map(
        self,
        table: str,
        schema: str,
        group_table: str,
        safe_group_cols: List[str],
        new_columns: List[str],
        backend,
        data_id: str,
        sig: str,
    ) -> None:
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        cond = " AND ".join(
            f'o.{self.db.quote_identifier(c)} = g.{self.db.quote_identifier(c)}'
            for c in safe_group_cols
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
    # map_feature helpers: LEFT JOIN group results back onto the original
    # table (new feature columns only). Marker rows in the transient
    # registry tell our own re-runs apart from foreign column collisions.
    # ------------------------------------------------------------------
    def _map_sig(
        self,
        group_cols: List[str],
        agg_spec: Dict[str, List[str]],
        new_columns: List[str],
    ) -> str:
        return json.dumps(
            {"group_cols": group_cols, "agg_spec": agg_spec, "new_columns": new_columns},
            sort_keys=True,
        )

    async def _check_map_collision(
        self,
        table: str,
        schema: str,
        backend,
        data_id: str,
        safe_group_cols: List[str],
        agg_spec: Dict[str, List[str]],
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
        sig = self._map_sig(safe_group_cols, agg_spec, new_columns)
        rows = await self._fetch(
            f"""SELECT kwargs FROM {backend.transient_registry_table}
                WHERE data_id = {backend.placeholder(1)}
                  AND operation_type = 'groupby_map'
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
            f"group-by mapping. Drop or rename them first."
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
                        'groupby_map', 'GroupByStatsOps', 'map_feature',
                        {backend.placeholder(3)}, {backend.placeholder(4)},
                        {backend.placeholder(5)}, {backend.placeholder(6)},
                        {backend.placeholder(7)})""",
            data_id, opidx, "", sig, table, False, schema,
        )

    # ------------------------------------------------------------------
    # Response wrappers
    # ------------------------------------------------------------------
    def _success_response(self, message: str, result: pd.DataFrame, new_table: str,
                          new_columns: List[str], group_cols: List[str], **extra) -> Dict[str, Any]:
        resp = {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": result,
            "new_table": new_table,
            "new_columns": new_columns,
            "group_cols": group_cols,
            **extra,
        }
        return resp

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
            f"Unsupported database backend for groupby stats operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Aggregate SQL generator
    # ------------------------------------------------------------------
    def _agg_expr(self, col: str, stat: str) -> str:
        """
        Returns a tuple (sql_expr, alias) for the given column and stat.
        Alias is automatically generated as `{col}_{stat}`.
        """
        qcol = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(col))
        alias = f"{col}_{stat}"

        # Stats that use identical SQL across ALL three backends
        common_aggs = {
            "count": f"COUNT({qcol})",
            "sum": f"SUM({qcol})",
            "min": f"MIN({qcol})",
            "max": f"MAX({qcol})",
            "avg": f"AVG({qcol})",
            "mean": f"AVG({qcol})",
            "nunique": f"COUNT(DISTINCT {qcol})",
            "range": f"MAX({qcol}) - MIN({qcol})",
        }

        if stat in common_aggs:
            return f"{common_aggs[stat]} AS {self.db.quote_identifier(alias)}"

        # std / var / sem / product / median / mode -- backend-specific
        # spellings live in the dialect hooks above.
        backend_aggs = {
            "std": self._std_expr(qcol),
            "var": self._var_expr(qcol),
            "sem": self._sem_expr(qcol),
            "product": self._product_expr(qcol),
            "median": self._median_expr(qcol),
            "mode": self._mode_expr(qcol),
        }

        if stat in backend_aggs:
            return f"{backend_aggs[stat]} AS {self.db.quote_identifier(alias)}"

        raise ValueError(f"Unsupported group-by stat: {stat}")

    # ------------------------------------------------------------------
    # Shared aggregation flow
    # ------------------------------------------------------------------
    def _build_agg_parts(
        self, agg_spec: Dict[str, List[str]]
    ) -> tuple[list, list]:
        select_parts = []
        new_columns = []
        for col, stats in agg_spec.items():
            if not stats:
                continue
            for stat in stats:
                expr = self._agg_expr(col, stat)
                select_parts.append(expr)
                new_columns.append(f"{col}_{stat}")
        return select_parts, new_columns

    async def _fetch_preview(
        self, output_table: str, schema: str, preview_cols: List[str]
    ) -> pd.DataFrame:
        rows = await self._fetch(
            f"""SELECT {', '.join(self.db.quote_identifier(c) for c in preview_cols)}
                FROM {self._qualified_table(output_table, schema)}"""
        )
        return pd.DataFrame(rows, columns=preview_cols) if rows else pd.DataFrame(columns=preview_cols)

    # ------------------------------------------------------------------
    # Main aggregation method (CREATES A TABLE)
    # ------------------------------------------------------------------
    async def group_aggregate(
        self,
        table: str,
        schema: str,
        group_cols: List[str],
        agg_spec: Dict[str, List[str]],
        backend=None,
        data_id=None,
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute GROUP BY, store results in a new table, and return a preview.
        With map_feature=True the new feature columns are additionally
        LEFT JOINed back onto the original table (no extra result table).
        """
        try:
            if backend is None or data_id is None:
                raise ValueError("backend and data_id are required")

            if not group_cols:
                raise ValueError("At least one group-by column is required.")
            if not agg_spec:
                raise ValueError("agg_spec must contain at least one column -> stats mapping.")

            safe_group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            quoted_group_cols = [self.db.quote_identifier(c) for c in safe_group_cols]

            # Build aggregate expressions
            select_parts, new_columns = self._build_agg_parts(agg_spec)

            if not select_parts:
                raise ValueError("No valid aggregate expressions generated.")

            map_sig = self._map_sig(safe_group_cols, agg_spec, new_columns)
            if map_feature:
                collision = await self._check_map_collision(
                    table, schema, backend, data_id,
                    safe_group_cols, agg_spec, new_columns,
                )
                if collision:
                    return self._error_response(
                        f"Group-by aggregation failed: {collision}",
                        group_cols=safe_group_cols,
                    )

            qualified_source = self._qualified_table(table, schema)
            group_clause = ", ".join(quoted_group_cols)
            select_clause = ", ".join(quoted_group_cols + select_parts)

            # Resolve output table name (supports chaining via new_table)
            output_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            create_sql = self._create_table_sql(
                output_table,
                schema,
                f"""SELECT {select_clause}
                FROM {qualified_source}
                GROUP BY {group_clause}
                """,
            )
            await self._exec(create_sql)

            if map_feature:
                await self._apply_map(
                    table, schema, output_table,
                    safe_group_cols, new_columns, backend, data_id, map_sig,
                )

            # Fetch preview (all rows)
            preview_cols = safe_group_cols + new_columns
            df = await self._fetch_preview(output_table, schema, preview_cols)

            return self._success_response(
                f"Group-by aggregation on {list(agg_spec.keys())}",
                df,
                new_table=output_table,
                new_columns=new_columns,
                group_cols=safe_group_cols,
                agg_spec=agg_spec,
                mapped_table=table if map_feature else None,
                mapped_columns=new_columns if map_feature else [],
            )

        except Exception as e:
            return self._error_response(
                f"Group-by aggregation failed: {str(e)}\n{traceback.format_exc()}",
                group_cols=group_cols if 'group_cols' in locals() else [],
            )

    # ------------------------------------------------------------------
    # Event rate
    # ------------------------------------------------------------------
    async def group_event_rate(
        self,
        table: str,
        schema: str,
        group_cols: List[str],
        datetime_col: str,
        unit: str = "day",
        backend=None,
        data_id=None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                raise ValueError("backend and data_id are required")

            safe_group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            safe_datetime = SQLIdentifierSanitizer.sanitize(datetime_col)
            quoted_group_cols = [self.db.quote_identifier(c) for c in safe_group_cols]
            qcol = self.db.quote_identifier(safe_datetime)
            qualified = self._qualified_table(table, schema)

            units = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}
            secs = units.get(unit.lower(), 86400)

            output_table = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            elapsed_seconds = self._elapsed_seconds_expr(qcol)

            create_sql = self._create_table_sql(
                output_table,
                schema,
                f"""SELECT {', '.join(quoted_group_cols)},
                       {self._count_fn} AS cnt,
                       CASE WHEN MAX({qcol}) = MIN({qcol}) THEN 0.0
                            ELSE {self._count_fn} / (({elapsed_seconds}) / {secs})
                       END AS event_rate
                FROM {qualified}
                WHERE {qcol} IS NOT NULL
                GROUP BY {', '.join(quoted_group_cols)}
                """,
            )
            await self._exec(create_sql)

            new_columns = ["cnt", "event_rate"]
            preview_cols = safe_group_cols + new_columns
            df = await self._fetch_preview(output_table, schema, preview_cols)

            return self._success_response(
                f"Event rate (per {unit}) grouped by {safe_group_cols}",
                df,
                new_table=output_table,
                new_columns=new_columns,
                group_cols=safe_group_cols,
            )

        except Exception as e:
            return self._error_response(
                f"Event rate failed: {str(e)}\n{traceback.format_exc()}",
                group_cols=group_cols if 'group_cols' in locals() else [],
            )
