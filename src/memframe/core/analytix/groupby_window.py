"""
Group‑by window operations (rolling, expanding, ewm).
Extends DataWindowOps so all existing SQL building is reused.
Now uses `_resolve_output_table_name` for consistent naming and
supports optional `new_table` for chaining.
Supports DuckDB, PostgreSQL, and ClickHouse.
"""

from __future__ import annotations
from datetime import datetime, timezone
import json
import traceback
from typing import Any, Dict, List, Union
import pandas as pd
import numpy as np
import math

from memframe.core.analytix.window import WindowOps
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class GroupbyWindowOps(WindowOps):
    """
    Adds PARTITION BY support to all windowed aggregations.
    Supports DuckDB, PostgreSQL, and ClickHouse.
    """

    # Internal row-number column used for ClickHouse ordering fallback
    _CH_ROW_NUM = "_mf_row_num"

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for groupby window operation: {self.db.__class__.__name__}"
        )

    def _error_response(self, msg, **extra):
        # ponytail: the pre-split engine accepted extras (e.g. group_cols);
        # keep that shape on top of the narrowed base signature.
        resp = super()._error_response(msg)
        resp.update(extra)
        return resp

    # ------------------------------------------------------------------
    #  ClickHouse helpers
    # ------------------------------------------------------------------
    def _ch_datetime_epoch_expr(self, col_expr: str) -> str:
        """Extract Unix epoch (seconds) from a datetime column in ClickHouse."""
        return f"toUnixTimestamp({col_expr})"

    def _ch_from_epoch(self, epoch_expr: str, is_date_only: bool) -> str:
        """Convert epoch seconds back to DateTime / Date for ClickHouse."""
        ts = f"fromUnixTimestamp({epoch_expr})"
        return f"toDate({ts})" if is_date_only else ts

    def _ch_source_with_rownum(self, qualified_table: str) -> str:
        """Return a subquery that adds _mf_row_num for ordering fallback."""
        rn = self._CH_ROW_NUM
        return (
            f"(SELECT *, ROW_NUMBER() OVER() AS {rn} "
            f"FROM {qualified_table}) AS __source"
        )

    def _ch_create_table_as(self, output_qualified: str, select_sql: str) -> str:
        """ClickHouse CREATE TABLE AS with required ENGINE clause."""
        return (
            f"CREATE TABLE {output_qualified} "
            f"ENGINE = MergeTree() ORDER BY tuple() AS\n{select_sql}"
        )

    # ------------------------------------------------------------------
    # map_feature helpers: LEFT JOIN the new feature column(s) back onto the
    # original table on full row identity (one output row per input row).
    # Marker rows in the transient registry tell our own re-runs apart
    # from foreign column collisions.
    # ------------------------------------------------------------------
    def _map_sig(
        self,
        group_cols,
        column,
        operation_name,
        order_cols,
        target_col,
        new_columns,
    ):
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
        self, table, schema, backend, data_id, sig, new_columns,
    ):
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
                  AND operation_type = 'groupby_win_map'
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
            f"group-by window mapping. Drop or rename them first."
        )

    async def _record_map_marker(self, table, schema, backend, data_id, sig):
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
                        'groupby_win_map', 'GroupbyWindowOps', 'map_feature',
                        {backend.placeholder(3)}, {backend.placeholder(4)},
                        {backend.placeholder(5)}, {backend.placeholder(6)},
                        {backend.placeholder(7)})""",
            data_id, opidx, "", sig, table, False, schema,
        )

    async def _apply_map(
        self, table, schema, group_table, new_columns, backend, data_id, sig,
    ):
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
        if isinstance(self.db, ClickHouseAdapter):
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
        elif isinstance(self.db, PostgresAdapter):
            tmp = SQLIdentifierSanitizer.sanitize(f"{table}__map_tmp")
            tmp_q = self._qualified_table(tmp, schema)
            await self._exec(f"DROP TABLE IF EXISTS {tmp_q}")
            await self._exec(
                f"""CREATE TABLE {tmp_q} AS
                    SELECT o.*, {feats}
                    FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
            )
            await self._exec(f"DROP TABLE {orig_q}")
            await self._exec(
                f"ALTER TABLE {tmp_q} RENAME TO {self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(table))}"
            )
        else:
            await self._exec(
                f"""CREATE OR REPLACE TABLE {orig_q} AS
                    SELECT o.*, {feats}
                    FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
            )
        await self._record_map_marker(table, schema, backend, data_id, sig)

    async def _projection_sql(
        self,
        table: str,
        schema: str,
        prefix: str = "",
        exclude: set[str] | None = None,
    ) -> str:
        """Return an explicit projection for source columns, excluding internal row ids."""
        exclude = exclude or set()
        internal_cols = {self._CH_ROW_NUM, "__rn"} | exclude
        column_types = await self.db.get_column_types(table, schema)
        columns = [
            SQLIdentifierSanitizer.sanitize(c)
            for c in column_types.keys()
            if c not in internal_cols
        ]
        if not columns:
            return "*"
        qualifier = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{qualifier}{self.db.quote_identifier(c)}"
            for c in columns
        )

    def _ch_agg_spec(self, raw: str) -> tuple[str, str] | None:
        """Normalize public and backend-specific aggregation aliases for ClickHouse."""
        key = raw.strip().lower()
        agg_map = {
            "sum": ("sum", "sum"),
            "avg": ("avg", "mean"),
            "mean": ("avg", "mean"),
            "min": ("min", "min"),
            "max": ("max", "max"),
            "count": ("count", "count"),
            "first": ("first_value", "first"),
            "first_value": ("first_value", "first"),
            "last": ("last_value", "last"),
            "last_value": ("last_value", "last"),
            "std": ("stddevSamp", "std"),
            "stddev": ("stddevSamp", "std"),
            "stddev_samp": ("stddevSamp", "std"),
            "stddevsamp": ("stddevSamp", "std"),
            "var": ("varSamp", "var"),
            "variance": ("varSamp", "var"),
            "var_samp": ("varSamp", "var"),
            "varsamp": ("varSamp", "var"),
        }
        return agg_map.get(key)

    def _sql_agg_spec(self, raw: str) -> tuple[str, str] | None:
        """Normalize public and backend-specific aggregation aliases for Postgres/DuckDB."""
        key = raw.strip().lower()
        agg_map = {
            "sum": ("SUM", "sum"),
            "avg": ("AVG", "mean"),
            "mean": ("AVG", "mean"),
            "min": ("MIN", "min"),
            "max": ("MAX", "max"),
            "count": ("COUNT", "count"),
            "first": ("FIRST_VALUE", "first"),
            "first_value": ("FIRST_VALUE", "first"),
            "last": ("LAST_VALUE", "last"),
            "last_value": ("LAST_VALUE", "last"),
        }
        if key in agg_map:
            return agg_map[key]
        if key in ("std", "stddev", "stddev_samp"):
            if isinstance(self.db, PostgresAdapter):
                return ("STDDEV", "std")
            if isinstance(self.db, DuckDBAdapter):
                return ("STDDEV_SAMP", "std")
            raise self._unsupported_backend_error()
        if key in ("var", "variance", "var_samp"):
            if isinstance(self.db, PostgresAdapter):
                return ("VARIANCE", "var")
            if isinstance(self.db, DuckDBAdapter):
                return ("VAR_SAMP", "var")
            raise self._unsupported_backend_error()
        return None

    # ------------------------------------------------------------------
    #  ROLLING with GROUP BY – generic engine
    # ------------------------------------------------------------------
    async def _rolling_agg_with_partition(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: List[str],
        order_by: Union[str, List[str]] = None,
        window: int = 3,
        agg: Union[str, List[str]] = "SUM",
        backend=None,
        data_id=None,
        new_table: str = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Generic rolling aggregation with PARTITION BY group_cols."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING — unchanged)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if isinstance(agg, str):
                    raw_aggs = [agg]
                else:
                    raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
                if not raw_aggs:
                    return self._error_response("At least one aggregation function is required")

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1

                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                agg_specs = []
                for raw in raw_aggs:
                    spec = self._sql_agg_spec(raw)
                    if spec is None:
                        return self._error_response(f"Unsupported rolling agg '{raw}' for groupby")
                    sql_agg, alias_key = spec
                    agg_specs.append((sql_agg, alias_key))

                new_cols = []
                used_col_names = set()
                window_exprs = []
                suffix_group = "_".join(safe_groups)

                for sql_agg, alias_key in agg_specs:
                    base_col = f"{safe_col}_rolling_{alias_key}_w{window}_by_{suffix_group}"
                    col_name = base_col
                    suffix = 2
                    while col_name in used_col_names:
                        col_name = f"{base_col}_{suffix}"
                        suffix += 1
                    used_col_names.add(col_name)
                    new_cols.append(col_name)

                    window_exprs.append(
                        f"""
                        {sql_agg}({self.db.quote_identifier(safe_col)})
                        OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                            ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                        ) AS {self.db.quote_identifier(col_name)}
                        """
                    )

                window_sql = ", ".join(window_exprs)

                create_sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *,
                        {window_sql}
                    FROM {qualified}
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling", safe_orders,
                    new_cols[0] if len(new_cols) == 1 else "rolling", new_cols,
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, new_cols,
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, new_cols,
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, *new_cols]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                preview_cols = list(dict.fromkeys(preview_cols))
                res = await self._fetch_data(new_table_name, schema, preview_cols)

                agg_label = ", ".join(raw_aggs)
                response = self._success_response(
                    f"Rolling {agg_label} on '{column}' grouped by {safe_groups} (window={window})",
                    result=res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_cols if map_feature else [],
                    new_columns=new_cols,
                    window=window,
                    group_cols=safe_groups,
                )
                if len(new_cols) == 1:
                    response["new_column"] = new_cols[0]
                return response

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if isinstance(agg, str):
                    raw_aggs = [agg]
                else:
                    raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
                if not raw_aggs:
                    return self._error_response("At least one aggregation function is required")

                # ------ Resolve ordering ------
                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                agg_specs = []
                for raw in raw_aggs:
                    spec = self._ch_agg_spec(raw)
                    if spec is None:
                        return self._error_response(f"Unsupported rolling agg '{raw}' for groupby")
                    sql_agg, alias_key = spec
                    agg_specs.append((sql_agg, alias_key))

                # ------ Build window expressions ------
                new_cols = []
                used_col_names = set()
                window_exprs = []
                suffix_group = "_".join(safe_groups)

                for sql_agg, alias_key in agg_specs:
                    base_col = f"{safe_col}_rolling_{alias_key}_w{window}_by_{suffix_group}"
                    col_name = base_col
                    suffix = 2
                    while col_name in used_col_names:
                        col_name = f"{base_col}_{suffix}"
                        suffix += 1
                    used_col_names.add(col_name)
                    new_cols.append(col_name)

                    window_exprs.append(
                        f"""
                        {sql_agg}({self.db.quote_identifier(safe_col)})
                        OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                            ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                        ) AS {self.db.quote_identifier(col_name)}
                        """
                    )

                window_sql = ", ".join(window_exprs)
                output_qualified = self._qualified_table(new_table_name, schema)
                source_projection = await self._projection_sql(table, schema)
                select_sql = (
                    f"SELECT {source_projection},\n"
                    f"    {window_sql}\n"
                    f"FROM {source_expr}"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling", safe_orders,
                    new_cols[0] if len(new_cols) == 1 else "rolling", new_cols,
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, new_cols,
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, new_cols,
                        backend, data_id, map_sig,
                    )

                # ------ Preview ------
                preview_cols = [column, *new_cols]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                preview_cols = list(dict.fromkeys(preview_cols))
                res = await self._fetch_data(new_table_name, schema, preview_cols)

                agg_label = ", ".join(raw_aggs)
                response = self._success_response(
                    f"Rolling {agg_label} on '{column}' grouped by {safe_groups} (window={window})",
                    result=res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_cols if map_feature else [],
                    new_columns=new_cols,
                    window=window,
                    group_cols=safe_groups,
                )
                if len(new_cols) == 1:
                    response["new_column"] = new_cols[0]
                return response

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"rolling group-by error: {str(e)}\n{traceback.format_exc()}"
            )

    # --------------------------------------------------
    #  ROLLING GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def rolling_sum_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="SUM", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="SUM", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_mean_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="AVG", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="AVG", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_min_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MIN", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MIN", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_max_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MAX", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="MAX", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_count_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="COUNT", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="COUNT", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_std_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            if isinstance(self.db, PostgresAdapter):
                func = "STDDEV"
            elif isinstance(self.db, DuckDBAdapter):
                func = "STDDEV_SAMP"
            else:
                raise self._unsupported_backend_error()
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg=func, **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="stddevSamp", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_var_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            if isinstance(self.db, PostgresAdapter):
                func = "VARIANCE"
            elif isinstance(self.db, DuckDBAdapter):
                func = "VAR_SAMP"
            else:
                raise self._unsupported_backend_error()
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg=func, **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="varSamp", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_first_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="FIRST_VALUE", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="FIRST_VALUE", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_last_groupby(self, table, schema, column, group_cols, order_by=None, window=3, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="LAST_VALUE", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(table, schema, column, group_cols, order_by, window, agg="LAST_VALUE", **kwargs)
        else:
            raise self._unsupported_backend_error()

    # ---------- Rolling Specials ----------
    async def rolling_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, q=0.5, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_q{q}_w{window}_by_{'_'.join(safe_groups)}"

                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                if isinstance(self.db, PostgresAdapter):
                    q_col = self.db.quote_identifier(safe_col)
                    q_rn = self.db.quote_identifier("__rn")
                    q_new_col = self.db.quote_identifier(new_col)
                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    WITH __base AS (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}
                        FROM {qualified}
                    )
                    SELECT b.*,
                        (
                            SELECT PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY r.{q_col})
                            FROM __base r
                            WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                              AND r.{q_col} IS NOT NULL
                        ) AS {q_new_col}
                    FROM __base b
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "rolling_quantile", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "rolling_quantile", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Rolling quantile group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                elif isinstance(self.db, DuckDBAdapter):
                    func = f"QUANTILE_CONT({self.db.quote_identifier(safe_col)}, {q})"

                    window_sql = f"""
                        {func}
                        OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                            ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                        )
                    """

                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *,
                        {window_sql} AS {self.db.quote_identifier(new_col)}
                    FROM {qualified}
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "rolling_quantile", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "rolling_quantile", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Rolling quantile group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                else:
                    raise self._unsupported_backend_error()

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling quantile on '{column}' grouped by {safe_groups} (window={window}, q={q})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            # ============================================================
            # ClickHouse  (NEW — uses correlated subquery)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_q{q}_w{window}_by_{'_'.join(safe_groups)}"
                q_col = self.db.quote_identifier(safe_col)
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)
                source_projection = await self._projection_sql(table, schema, prefix="b")

                # ClickHouse exposes quantile(q)(x); quantileCont is not available
                # in all server builds.
                quantile_subq = f"""
                    SELECT quantile({q})(r.{q_col})
                    FROM __base r
                    WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                      AND r.{q_col} IS NOT NULL
                """

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    ({quantile_subq}) AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_quantile", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_quantile", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling quantile group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling quantile on '{column}' grouped by {safe_groups} (window={window}, q={q})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"rolling quantile group-by error: {str(e)}")

    async def rolling_sem_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_sem_w{window}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                if isinstance(self.db, PostgresAdapter):
                    std_func = "STDDEV"
                elif isinstance(self.db, DuckDBAdapter):
                    std_func = "STDDEV_SAMP"
                else:
                    raise self._unsupported_backend_error()
                sem_expr = f"""
                    ({std_func}({self.db.quote_identifier(safe_col)})
                    OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {order_sql}
                        ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                    )
                    /
                    SQRT(
                        COUNT({self.db.quote_identifier(safe_col)})
                        OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                            ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                        )
                    ))
                """

                sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                SELECT *, {sem_expr} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_sem", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_sem", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling sem group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling SEM on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_sem_w{window}_by_{'_'.join(safe_groups)}"
                q_col = self.db.quote_identifier(safe_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)
                source_projection = await self._projection_sql(table, schema)

                sem_expr = (
                    f"(stddevSamp({q_col}) OVER (\n"
                    f"    PARTITION BY {partition_sql}\n"
                    f"    ORDER BY {order_sql}\n"
                    f"    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW\n"
                    f")\n"
                    f"/ sqrt(COUNT({q_col}) OVER (\n"
                    f"    PARTITION BY {partition_sql}\n"
                    f"    ORDER BY {order_sql}\n"
                    f"    ROWS BETWEEN {w} PRECEDING AND CURRENT ROW\n"
                    f")))"
                )

                select_sql = (
                    f"SELECT {source_projection},\n"
                    f"    {sem_expr} AS {self.db.quote_identifier(new_col)}\n"
                    f"FROM {source_expr}"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_sem", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_sem", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling sem group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling SEM on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"rolling sem group-by error: {str(e)}")

    async def rolling_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_nunique_w{window}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                if isinstance(self.db, DuckDBAdapter):
                    nunique_expr = f"""
                        COUNT(DISTINCT {self.db.quote_identifier(safe_col)})
                        OVER (
                            PARTITION BY {partition_sql}
                            ORDER BY {order_sql}
                            ROWS BETWEEN {w} PRECEDING AND CURRENT ROW
                        )
                    """
                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *, {nunique_expr} AS {self.db.quote_identifier(new_col)}
                    FROM {qualified}
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "rolling_nunique", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "rolling_nunique", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Rolling nunique group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                elif isinstance(self.db, PostgresAdapter):
                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    WITH __base AS (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS __rn
                        FROM {qualified}
                    )
                    SELECT b.*,
                        (
                            SELECT COUNT(DISTINCT r.{self.db.quote_identifier(safe_col)})
                            FROM __base r
                            WHERE r.__rn BETWEEN b.__rn - {w} AND b.__rn
                        ) AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "rolling_nunique", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "rolling_nunique", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Rolling nunique group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                else:
                    raise self._unsupported_backend_error()

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling nunique on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            # ============================================================
            # ClickHouse  (NEW — correlated subquery, like Postgres)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_nunique_w{window}_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)
                source_projection = await self._projection_sql(table, schema, prefix="b")

                nunique_subq = f"""
                    SELECT countDistinct(r.{q_col})
                    FROM __base r
                    WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                """

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    ({nunique_subq}) AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_nunique", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_nunique", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling nunique group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling nunique on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"rolling nunique group-by error: {str(e)}")

    async def rolling_rank_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_rank_w{window}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS __rn
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
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_rank", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_rank", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling rank group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling rank on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            # ============================================================
            # ClickHouse  (NEW — correlated subquery)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                new_col = f"{safe_col}_rolling_rank_w{window}_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)
                source_projection = await self._projection_sql(table, schema, prefix="b")

                rank_subq = f"""
                    SELECT COUNT(*)
                    FROM __base r
                    WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                      AND r.{q_col} <= b.{q_col}
                """

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    ({rank_subq}) AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_rank", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_rank", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling rank group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling rank on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"rolling rank group-by error: {str(e)}")

    # ---------- Rolling Datetime Group‑By ----------
    async def rolling_min_datetime_groupby(self, *args, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(*args, agg="MIN", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(*args, agg="MIN", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_max_datetime_groupby(self, *args, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_agg_with_partition(*args, agg="MAX", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_agg_with_partition(*args, agg="MAX", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def rolling_mean_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="mean", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="mean", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def rolling_median_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="median", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="median", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def rolling_mode_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, window=3, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="mode", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._rolling_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, window, stat="mode", backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def _rolling_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, window, stat, backend, data_id, new_table=None,
        map_feature: bool = False
    ):
        """Copy of _rolling_datetime_stat with PARTITION BY added. Supports ClickHouse."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                if w < 0:
                    return self._error_response("window must be >= 1")

                stat_key = stat.lower()
                new_col = f"{safe_col}_rolling_{stat_key}_w{window}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                q_schema = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(schema))
                q_new_table = self.db.quote_identifier(new_table_name)
                q_col = self.db.quote_identifier(safe_col)
                q_rn = self.db.quote_identifier("__totem_roll_rn")

                is_date_only = await self._is_date_only_column(working_table, schema, safe_col)
                from_epoch_expr = "TO_TIMESTAMP(__agg.value_epoch)"
                if is_date_only:
                    from_epoch_expr = f"CAST({from_epoch_expr} AS DATE)"

                if stat_key == "mean":
                    epoch_expr = self._datetime_epoch_expr(f"r.{q_col}")
                    value_sql = f"""
                        SELECT {from_epoch_expr}
                        FROM (
                            SELECT AVG({epoch_expr}) AS value_epoch
                            FROM __base r
                            WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                              AND r.{q_col} IS NOT NULL
                        ) __agg
                    """
                elif stat_key == "median":
                    epoch_expr = self._datetime_epoch_expr(f"r.{q_col}")
                    if isinstance(self.db, PostgresAdapter):
                        median_epoch_sql = f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})"
                    elif isinstance(self.db, DuckDBAdapter):
                        median_epoch_sql = f"MEDIAN({epoch_expr})"
                    else:
                        raise self._unsupported_backend_error()
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
                            ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}
                        FROM {qualified}
                    )
                    SELECT b.*,
                        ({value_sql}) AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_datetime", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_datetime", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling datetime group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling datetime {stat_key} on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    window=window,
                    group_cols=safe_groups,
                )

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                w = int(window) - 1
                if w < 0:
                    return self._error_response("window must be >= 1")

                stat_key = stat.lower()
                new_col = f"{safe_col}_rolling_{stat_key}_w{window}_by_{'_'.join(safe_groups)}"
                q_col = self.db.quote_identifier(safe_col)
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                is_date_only = await self._is_date_only_column(table, schema, safe_col)
                from_epoch_expr = self._ch_from_epoch("__agg.value_epoch", is_date_only)

                if stat_key == "mean":
                    epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")
                    value_sql = f"""
                        SELECT {from_epoch_expr}
                        FROM (
                            SELECT AVG({epoch_expr}) AS value_epoch
                            FROM __base r
                            WHERE r.{q_rn} BETWEEN b.{q_rn} - {w} AND b.{q_rn}
                              AND r.{q_col} IS NOT NULL
                        ) __agg
                    """
                elif stat_key == "median":
                    epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")
                    median_epoch_sql = f"median({epoch_expr})"
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

                except_clause = f"EXCEPT({self._CH_ROW_NUM}, __rn)" if self._CH_ROW_NUM in safe_orders else "EXCEPT(__rn)"

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT b.* {except_clause},\n"
                    f"    ({value_sql}) AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "rolling_datetime", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "rolling_datetime", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Rolling datetime group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Rolling datetime {stat_key} on '{column}' grouped by {safe_groups} (window={window})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    window=window,
                    group_cols=safe_groups,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"rolling datetime group-by error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    #  EXPANDING with GROUP BY – generic engine
    # ------------------------------------------------------------------
    async def _expanding_agg_with_partition(
        self,
        table, schema, column, group_cols,
        order_by=None, agg="SUM", min_periods=1,
        backend=None, data_id=None, new_table=None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if isinstance(agg, str):
                    raw_aggs = [agg]
                else:
                    raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
                if not raw_aggs:
                    return self._error_response("At least one aggregation function is required")

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                new_cols = []
                used_col_names = set()
                numeric_aggs = []
                suffix_group = "_".join(safe_groups)

                for raw in raw_aggs:
                    spec = self._sql_agg_spec(raw)
                    if spec is None:
                        return self._error_response(f"Unsupported expanding aggregation '{raw}'")
                    sql_agg, alias = spec

                    base_col = f"{safe_col}_expanding_{alias}_by_{suffix_group}"
                    col_name = base_col
                    suffix = 2
                    while col_name in used_col_names:
                        col_name = f"{base_col}_{suffix}"
                        suffix += 1
                    used_col_names.add(col_name)
                    new_cols.append(col_name)

                    agg_over = f"{sql_agg}({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                    if min_periods > 1:
                        agg_with_null = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                    else:
                        agg_with_null = agg_over

                    numeric_aggs.append(f"{agg_with_null} AS {self.db.quote_identifier(col_name)}")

                window_sql = ", ".join(numeric_aggs)

                create_sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *, {window_sql}
                    FROM {qualified}
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding", safe_orders,
                    new_cols[0] if len(new_cols) == 1 else "expanding", new_cols,
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, new_cols,
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, new_cols,
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, *new_cols]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                preview_cols = list(dict.fromkeys(preview_cols))
                res = await self._fetch_data(new_table_name, schema, preview_cols)

                response = self._success_response(
                    f"Expanding {', '.join(raw_aggs)} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_cols if map_feature else [],
                    new_columns=new_cols,
                    min_periods=min_periods,
                    group_cols=safe_groups,
                )
                if len(new_cols) == 1:
                    response["new_column"] = new_cols[0]
                return response

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if isinstance(agg, str):
                    raw_aggs = [agg]
                else:
                    raw_aggs = [a for a in agg if isinstance(a, str) and a.strip()]
                if not raw_aggs:
                    return self._error_response("At least one aggregation function is required")

                # ------ Resolve ordering ------
                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                new_cols = []
                used_col_names = set()
                numeric_aggs = []
                suffix_group = "_".join(safe_groups)

                for raw in raw_aggs:
                    spec = self._ch_agg_spec(raw)
                    if spec is None:
                        return self._error_response(f"Unsupported expanding aggregation '{raw}'")
                    sql_agg, alias = spec

                    base_col = f"{safe_col}_expanding_{alias}_by_{suffix_group}"
                    col_name = base_col
                    suffix = 2
                    while col_name in used_col_names:
                        col_name = f"{base_col}_{suffix}"
                        suffix += 1
                    used_col_names.add(col_name)
                    new_cols.append(col_name)

                    agg_over = f"{sql_agg}({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                    if min_periods > 1:
                        agg_with_null = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                    else:
                        agg_with_null = agg_over

                    numeric_aggs.append(f"{agg_with_null} AS {self.db.quote_identifier(col_name)}")

                window_sql = ", ".join(numeric_aggs)
                source_projection = await self._projection_sql(table, schema)
                select_sql = (
                    f"SELECT {source_projection},\n"
                    f"    {window_sql}\n"
                    f"FROM {source_expr}"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding", safe_orders,
                    new_cols[0] if len(new_cols) == 1 else "expanding", new_cols,
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, new_cols,
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, new_cols,
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, *new_cols]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                preview_cols = list(dict.fromkeys(preview_cols))
                res = await self._fetch_data(new_table_name, schema, preview_cols)

                response = self._success_response(
                    f"Expanding {', '.join(raw_aggs)} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_cols if map_feature else [],
                    new_columns=new_cols,
                    min_periods=min_periods,
                    group_cols=safe_groups,
                )
                if len(new_cols) == 1:
                    response["new_column"] = new_cols[0]
                return response

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding group-by error: {str(e)}\n{traceback.format_exc()}")

    # --------------------------------------------------
    #  EXPANDING GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def expanding_sum_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="SUM", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="SUM", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_mean_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="AVG", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="AVG", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_min_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_max_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_count_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="COUNT", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="COUNT", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_std_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            if isinstance(self.db, PostgresAdapter):
                func = "STDDEV"
            elif isinstance(self.db, DuckDBAdapter):
                func = "STDDEV_SAMP"
            else:
                raise self._unsupported_backend_error()
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg=func, **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="stddevSamp", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_var_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            if isinstance(self.db, PostgresAdapter):
                func = "VARIANCE"
            elif isinstance(self.db, DuckDBAdapter):
                func = "VAR_SAMP"
            else:
                raise self._unsupported_backend_error()
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg=func, **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="varSamp", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_first_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="FIRST_VALUE", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="FIRST_VALUE", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_last_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="LAST_VALUE", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="LAST_VALUE", **kwargs)
        else:
            raise self._unsupported_backend_error()

    # ---------- Expanding Specials ----------
    async def expanding_quantile_groupby(
        self, table, schema, column, group_cols, order_by=None, q=0.5, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_q{q}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                if isinstance(self.db, PostgresAdapter):
                    q_rn = self.db.quote_identifier("__rn")
                    q_new_col = self.db.quote_identifier(new_col)
                    quantile_subq = f"""
                        SELECT PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY r.{safe_col_quoted})
                        FROM __base r
                        WHERE r.{q_rn} <= b.{q_rn}
                          AND r.{safe_col_quoted} IS NOT NULL
                    """
                    if min_periods > 1:
                        count_subq = f"SELECT COUNT(r.{safe_col_quoted}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                        quantile_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({quantile_subq}) ELSE NULL END"
                    else:
                        quantile_expr = f"({quantile_subq})"

                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    WITH __base AS (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}
                        FROM {qualified}
                    )
                    SELECT b.*,
                        {quantile_expr} AS {q_new_col}
                    FROM __base b
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "expanding_quantile", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "expanding_quantile", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Expanding quantile group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                elif isinstance(self.db, DuckDBAdapter):
                    base_func = f"QUANTILE_CONT({safe_col_quoted}, {q})"

                    agg_over = f"{base_func} OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"
                    if min_periods > 1:
                        quantile_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {agg_over} ELSE NULL END"
                    else:
                        quantile_expr = agg_over

                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *, {quantile_expr} AS {self.db.quote_identifier(new_col)}
                    FROM {qualified}
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "expanding_quantile", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "expanding_quantile", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Expanding quantile group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                else:
                    raise self._unsupported_backend_error()

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding quantile on '{column}' grouped by {safe_groups} (q={q})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            # ============================================================
            # ClickHouse  (NEW — correlated subquery)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                        
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_q{q}_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                # ClickHouse exposes quantile(q)(x); quantileCont is not available
                # in all server builds.
                quantile_subq = f"""
                    SELECT quantile({q})(r.{q_col})
                    FROM __base r
                    WHERE r.{q_rn} <= b.{q_rn}
                      AND r.{q_col} IS NOT NULL
                """

                if min_periods > 1:
                    count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                    value_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({quantile_subq}) ELSE NULL END"
                else:
                    value_expr = f"({quantile_subq})"

                source_projection = await self._projection_sql(table, schema, prefix="b")

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    {value_expr} AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_quantile", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_quantile", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding quantile group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding quantile on '{column}' grouped by {safe_groups} (q={q})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding quantile group-by error: {str(e)}")

    async def expanding_sem_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_sem_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                if isinstance(self.db, PostgresAdapter):
                    std_func = "STDDEV"
                elif isinstance(self.db, DuckDBAdapter):
                    std_func = "STDDEV_SAMP"
                else:
                    raise self._unsupported_backend_error()
                sem_base = f"({std_func}({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame}) / SQRT({count_over}))"
                if min_periods > 1:
                    sem_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {sem_base} ELSE NULL END"
                else:
                    sem_expr = sem_base

                sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                SELECT *, {sem_expr} AS {self.db.quote_identifier(new_col)}
                FROM {qualified}
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_sem", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_sem", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding sem group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )
                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding SEM on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_sem_by_{'_'.join(safe_groups)}"

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                # ClickHouse uses stddevSamp
                sem_base = f"(stddevSamp({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame}) / sqrt({count_over}))"
                if min_periods > 1:
                    sem_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {sem_base} ELSE NULL END"
                else:
                    sem_expr = sem_base

                source_projection = await self._projection_sql(table, schema)
                select_sql = (
                    f"SELECT {source_projection},\n"
                    f"    {sem_expr} AS {self.db.quote_identifier(new_col)}\n"
                    f"FROM {source_expr}"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_sem", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_sem", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding sem group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding SEM on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding sem group-by error: {str(e)}")

    async def expanding_rank_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_rank_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
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
                    rank_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({rank_subq}) ELSE NULL END"
                else:
                    rank_expr = f"({rank_subq})"

                sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS __rn
                    FROM {qualified}
                )
                SELECT b.*, {rank_expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_rank", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_rank", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding rank group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding rank on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            # ============================================================
            # ClickHouse  (NEW — correlated subquery)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_rank_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                rank_subq = f"""
                    SELECT COUNT(*)
                    FROM __base r
                    WHERE r.{q_rn} <= b.{q_rn}
                      AND r.{q_col} <= b.{q_col}
                """
                count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"

                if min_periods > 1:
                    rank_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({rank_subq}) ELSE NULL END"
                else:
                    rank_expr = f"({rank_subq})"

                source_projection = await self._projection_sql(table, schema, prefix="b")

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    {rank_expr} AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_rank", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_rank", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding rank group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding rank on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding rank group-by error: {str(e)}")

    async def expanding_nunique_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_nunique_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                safe_col_quoted = self.db.quote_identifier(safe_col)
                window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                count_over = f"COUNT({safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"

                if isinstance(self.db, DuckDBAdapter):
                    count_distinct = f"COUNT(DISTINCT {safe_col_quoted}) OVER (PARTITION BY {partition_sql} ORDER BY {order_sql} {window_frame})"
                    if min_periods > 1:
                        nunique_expr = f"CASE WHEN {count_over} >= {min_periods} THEN {count_distinct} ELSE NULL END"
                    else:
                        nunique_expr = count_distinct
                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    SELECT *, {nunique_expr} AS {self.db.quote_identifier(new_col)}
                    FROM {qualified}
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "expanding_nunique", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "expanding_nunique", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Expanding nunique group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                elif isinstance(self.db, PostgresAdapter):
                    sql = f"""
                    CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                    WITH __base AS (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS __rn
                        FROM {qualified}
                    )
                    SELECT b.*,
                        (
                            SELECT COUNT(DISTINCT r.{safe_col_quoted})
                            FROM __base r
                            WHERE r.__rn <= b.__rn
                        ) AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                    """
                    map_sig = self._map_sig(
                        safe_groups, safe_col, "expanding_nunique", safe_orders,
                        [new_col][0] if len([new_col]) == 1 else "expanding_nunique", [new_col],
                    )
                    map_collision = None
                    if map_feature:
                        map_collision = await self._check_map_collision(
                            table, schema, backend, data_id, map_sig, [new_col],
                        )
                        if map_collision:
                            return self._error_response(
                                f"Expanding nunique group-by error: {map_collision}",
                                group_cols=safe_groups,
                            )
                    await self._exec(sql)
                    if map_feature:
                        await self._apply_map(
                            table, schema, new_table_name, [new_col],
                            backend, data_id, map_sig,
                        )
                else:
                    raise self._unsupported_backend_error()

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding nunique on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            # ============================================================
            # ClickHouse  (NEW — correlated subquery)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_nunique_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                nunique_subq = f"""
                    SELECT countDistinct(r.{q_col})
                    FROM __base r
                    WHERE r.{q_rn} <= b.{q_rn}
                """

                if min_periods > 1:
                    count_subq = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                    value_expr = f"CASE WHEN ({count_subq}) >= {min_periods} THEN ({nunique_subq}) ELSE NULL END"
                else:
                    value_expr = f"({nunique_subq})"

                source_projection = await self._projection_sql(table, schema, prefix="b")

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT {source_projection},\n"
                    f"    {value_expr} AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_nunique", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_nunique", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding nunique group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))
                return self._success_response(
                    f"Expanding nunique on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding nunique group-by error: {str(e)}")

    # --------------------------------------------------
    #  EXPANDING DATETIME GROUP‑BY CONVENIENCE METHODS
    # --------------------------------------------------
    async def expanding_min_datetime_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MIN", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_max_datetime_groupby(self, *args, group_cols=None, **kwargs):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_agg_with_partition(*args, group_cols=group_cols, agg="MAX", **kwargs)
        else:
            raise self._unsupported_backend_error()

    async def expanding_mean_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="mean", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="mean", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def expanding_median_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="median", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="median", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def expanding_mode_datetime_groupby(
        self, table, schema, column, group_cols, order_by=None, min_periods=1, backend=None, data_id=None, new_table=None,
        map_feature: bool = False
    ):
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="mode", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        elif isinstance(self.db, ClickHouseAdapter):
            return await self._expanding_datetime_stat_with_partition(
                table, schema, column, group_cols, order_by, stat="mode", min_periods=min_periods, backend=backend, data_id=data_id, new_table=new_table,
                map_feature=map_feature
            )
        else:
            raise self._unsupported_backend_error()

    async def _expanding_datetime_stat_with_partition(
        self, table, schema, column, group_cols, order_by, stat, min_periods, backend, data_id, new_table=None,
        map_feature: bool = False
    ):
        """Copy of _expanding_datetime_stat with PARTITION BY. Supports ClickHouse."""
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_{stat}_by_{'_'.join(safe_groups)}"
                qualified = self._qualified_table(working_table, schema)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                safe_col_quoted = self.db.quote_identifier(safe_col)
                is_date_only = await self._is_date_only_column(working_table, schema, safe_col)
                from_epoch_expr = "TO_TIMESTAMP(__agg.value_epoch)"
                if is_date_only:
                    from_epoch_expr = f"CAST({from_epoch_expr} AS DATE)"

                epoch_expr = self._datetime_epoch_expr(f"r.{safe_col_quoted}")

                if stat == "mean":
                    inner = f"AVG({epoch_expr})"
                elif stat == "median":
                    if isinstance(self.db, PostgresAdapter):
                        inner = f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})"
                    elif isinstance(self.db, DuckDBAdapter):
                        inner = f"MEDIAN({epoch_expr})"
                    else:
                        raise self._unsupported_backend_error()
                elif stat == "mode":
                    inner = None
                else:
                    return self._error_response(f"Unsupported datetime expanding stat '{stat}'")

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
                    non_null_count = f"SELECT COUNT(r.{safe_col_quoted}) FROM __base r WHERE r.__rn <= b.__rn"
                    if min_periods > 1:
                        value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({mode_subq}) ELSE NULL END"
                    else:
                        value_sql = f"({mode_subq})"
                else:
                    non_null_count = f"SELECT COUNT(r.{safe_col_quoted}) FROM __base r WHERE r.__rn <= b.__rn"
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

                sql = f"""
                CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table_name)} AS
                WITH __base AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {partition_sql} ORDER BY {order_sql}) AS __rn
                    FROM {qualified}
                )
                SELECT b.*, {value_sql} AS {self.db.quote_identifier(new_col)}
                FROM __base b
                """
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_datetime", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_datetime", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding datetime group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Expanding datetime {stat} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                    group_cols=safe_groups,
                )

            # ============================================================
            # ClickHouse  (NEW)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                if backend is None or data_id is None:
                    return self._error_response("backend and data_id required")
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                q_col = self.db.quote_identifier(safe_col)

                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    source_expr = self._qualified_table(table, schema)
                else:
                    safe_orders = [self._CH_ROW_NUM]
                    source_expr = self._ch_source_with_rownum(
                        self._qualified_table(table, schema)
                    )

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                new_col = f"{safe_col}_expanding_{stat}_by_{'_'.join(safe_groups)}"
                q_rn = self.db.quote_identifier("__rn")
                q_new_col = self.db.quote_identifier(new_col)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                output_qualified = self._qualified_table(new_table_name, schema)

                is_date_only = await self._is_date_only_column(table, schema, safe_col)
                from_epoch_expr = self._ch_from_epoch("__agg.value_epoch", is_date_only)
                epoch_expr = self._ch_datetime_epoch_expr(f"r.{q_col}")

                if stat == "mean":
                    inner = f"AVG({epoch_expr})"
                elif stat == "median":
                    inner = f"median({epoch_expr})"
                elif stat == "mode":
                    inner = None
                else:
                    return self._error_response(f"Unsupported datetime expanding stat '{stat}'")

                if stat == "mode":
                    mode_subq = f"""
                        SELECT r.{q_col}
                        FROM __base r
                        WHERE r.{q_rn} <= b.{q_rn}
                          AND r.{q_col} IS NOT NULL
                        GROUP BY r.{q_col}
                        ORDER BY COUNT(*) DESC, r.{q_col}
                        LIMIT 1
                    """
                    non_null_count = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                    if min_periods > 1:
                        value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({mode_subq}) ELSE NULL END"
                    else:
                        value_sql = f"({mode_subq})"
                else:
                    non_null_count = f"SELECT COUNT(r.{q_col}) FROM __base r WHERE r.{q_rn} <= b.{q_rn}"
                    value_subq = f"""
                        SELECT {from_epoch_expr}
                        FROM (SELECT {inner} AS value_epoch
                            FROM __base r
                            WHERE r.{q_rn} <= b.{q_rn}
                              AND r.{q_col} IS NOT NULL) __agg
                    """
                    if min_periods > 1:
                        value_sql = f"CASE WHEN ({non_null_count}) >= {min_periods} THEN ({value_subq}) ELSE NULL END"
                    else:
                        value_sql = f"({value_subq})"

                except_clause = f"EXCEPT({self._CH_ROW_NUM}, __rn)" if self._CH_ROW_NUM in safe_orders else "EXCEPT(__rn)"

                select_sql = (
                    f"WITH __base AS (\n"
                    f"    SELECT *, ROW_NUMBER() OVER(PARTITION BY {partition_sql} ORDER BY {order_sql}) AS {q_rn}\n"
                    f"    FROM {source_expr}\n"
                    f")\n"
                    f"SELECT b.* {except_clause},\n"
                    f"    {value_sql} AS {q_new_col}\n"
                    f"FROM __base b"
                )

                create_sql = self._ch_create_table_as(output_qualified, select_sql)
                map_sig = self._map_sig(
                    safe_groups, safe_col, "expanding_datetime", safe_orders,
                    [new_col][0] if len([new_col]) == 1 else "expanding_datetime", [new_col],
                )
                map_collision = None
                if map_feature:
                    map_collision = await self._check_map_collision(
                        table, schema, backend, data_id, map_sig, [new_col],
                    )
                    if map_collision:
                        return self._error_response(
                            f"Expanding datetime group-by error: {map_collision}",
                            group_cols=safe_groups,
                        )
                await self._exec(create_sql)
                if map_feature:
                    await self._apply_map(
                        table, schema, new_table_name, [new_col],
                        backend, data_id, map_sig,
                    )

                preview_cols = [column, new_col]
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"Expanding datetime {stat} on '{column}' grouped by {safe_groups} (min_periods={min_periods})",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=[new_col] if map_feature else [],
                    new_column=new_col,
                    min_periods=min_periods,
                    group_cols=safe_groups,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"expanding datetime group-by error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    #  EWM with GROUP BY
    # ------------------------------------------------------------------
    async def ewm_groupby(
        self,
        table, schema, column, group_cols,
        order_by=None,
        com=None, span=None, halflife=None, alpha=None,
        adjust=True, ignore_na=False, min_periods=0,
        agg="mean",
        backend=None, data_id=None, new_table=None,
        map_feature: bool = False,
    ):
        try:
            # ============================================================
            # DuckDB / PostgreSQL  (EXISTING — unchanged)
            # ============================================================
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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

                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                    working_table = table
                else:
                    safe_order, working_table = await self._resolve_ordering(table, schema, None)
                    safe_orders = [safe_order]

                order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)
                qualified = self._qualified_table(working_table, schema)

                cols_to_fetch = [safe_col] + safe_groups + safe_orders
                rows = await self._fetch(
                    f"SELECT {', '.join(self.db.quote_identifier(c) for c in cols_to_fetch)} FROM {qualified} "
                    f"ORDER BY {partition_sql}, {order_sql}"
                )
                df = pd.DataFrame(rows, columns=cols_to_fetch)

                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                q_schema = self.db.quote_identifier(schema)
                q_new = self.db.quote_identifier(new_table_name)
                await self._exec(f"CREATE TABLE {q_schema}.{q_new} AS SELECT * FROM {qualified}")

                new_cols = []
                for a in aggs:
                    col_name = f"{safe_col}_ewm_{a}_by_{'_'.join(safe_groups)}"
                    await self._add_col(new_table_name, schema, col_name, "DOUBLE PRECISION")
                    new_cols.append(col_name)

                alpha_val = self._alpha_from_params(com, span, halflife, alpha)
                await self._ensure_row_id(new_table_name, schema, order_col=safe_groups + safe_orders)
                grouped = df.groupby(safe_groups, sort=False)
                chunk_size = 500
                for group_keys, group_df in grouped:
                    conds = " AND ".join(
                        f"{self.db.quote_identifier(c)} IS NOT DISTINCT FROM {self.db.placeholder(i+1)}"
                        for i, c in enumerate(safe_groups)
                    )
                    vals = list(group_keys) if isinstance(group_keys, tuple) else [group_keys]
                    fetch_rows = await self._fetch(
                        f"SELECT {self.db.quote_identifier(self.TMP_ROW)}, {self.db.quote_identifier(safe_col)} "
                        f"FROM {self._qualified_table(new_table_name, schema)} "
                        f"WHERE {conds} "
                        f"ORDER BY {order_sql}",
                        *vals
                    )
                    row_ids = [r[self.TMP_ROW] for r in fetch_rows]
                    vals_arr = [r[safe_col] if r[safe_col] is not None else np.nan for r in fetch_rows]
                    results = self._compute_ewm_array(
                        np.array(vals_arr, dtype=np.float64), alpha_val, adjust, ignore_na, min_periods, aggs
                    )
                    for i in range(0, len(row_ids), chunk_size):
                        chunk_ids = row_ids[i:i+chunk_size]
                        chunks_results = {k: results[k][i:i+chunk_size] for k in aggs}
                        flat = []
                        set_parts = []
                        for j, rid in enumerate(chunk_ids):
                            placeholders = []
                            flat.append(rid)
                            row_placeholder = self.db.placeholder(len(flat))
                            if isinstance(self.db, PostgresAdapter):
                                row_placeholder = f"{row_placeholder}::bigint"
                            placeholders.append(row_placeholder)
                            for f in aggs:
                                val = chunks_results[f][j]
                                flat.append(None if np.isnan(val) else float(val))
                                value_placeholder = self.db.placeholder(len(flat))
                                if isinstance(self.db, PostgresAdapter):
                                    value_placeholder = f"{value_placeholder}::double precision"
                                placeholders.append(value_placeholder)
                            set_parts.append(f"({', '.join(placeholders)})")
                        if not set_parts:
                            continue
                        value_list = ", ".join(set_parts)
                        row_id_alias = "__memframe_row_id"
                        col_list = ", ".join(
                            f"{self.db.quote_identifier(new_cols[idx])} = CAST(v.{self.db.quote_identifier(new_cols[idx])} AS DOUBLE PRECISION)"
                            for idx, f in enumerate(aggs)
                        )
                        value_aliases = [row_id_alias, *new_cols]
                        sql_update = f"""
                        UPDATE {q_schema}.{q_new} SET
                            {col_list}
                        FROM (VALUES {value_list}) AS v({', '.join(self.db.quote_identifier(c) for c in value_aliases)})
                        WHERE {self.db.quote_identifier(self.TMP_ROW)} = CAST(v.{self.db.quote_identifier(row_id_alias)} AS BIGINT)
                        """
                        await self._exec(sql_update, *flat)

                await self._drop_tmp_row(new_table_name, schema)

                preview_cols = [safe_col] + new_cols
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(new_table_name, schema, list(dict.fromkeys(preview_cols)))

                return self._success_response(
                    f"EWM {aggs} on '{column}' grouped by {safe_groups}",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_cols if map_feature else [],
                    new_columns=new_cols,
                    group_cols=safe_groups,
                )

            # ============================================================
            # ClickHouse  (FIXED — staging table + LEFT JOIN)
            # ============================================================
            elif isinstance(self.db, ClickHouseAdapter):
                import asyncio

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

                safe_groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                source_qualified = self._qualified_table(table, schema)

                # ---- Resolve ordering ----
                if order_by:
                    if isinstance(order_by, (list, tuple)):
                        safe_orders = [SQLIdentifierSanitizer.sanitize(c) for c in order_by]
                    else:
                        safe_orders = [SQLIdentifierSanitizer.sanitize(order_by)]
                else:
                    safe_orders = []

                # ---- Create working table with materialized row number ----
                # ClickHouse has no ctid/rowid, and ROW_NUMBER() OVER() is
                # non-deterministic across separate queries.  We materialize
                # _mf_row_num once so that both the Python fetch and the SQL
                # CTE see identical row order.
                working_table = SQLIdentifierSanitizer.sanitize(
                    f"{table}__ewm_work_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
                )
                working_qualified = self._qualified_table(working_table, schema)
                await self._exec(
                    f"CREATE TABLE {working_qualified} ENGINE = MergeTree() ORDER BY tuple() AS "
                    f"SELECT *, ROW_NUMBER() OVER() AS {self.db.quote_identifier(self._CH_ROW_NUM)} "
                    f"FROM {source_qualified}"
                )

                # Full deterministic ordering: groups + user order + _mf_row_num
                all_order_cols = safe_groups + safe_orders + [self._CH_ROW_NUM]
                full_order_sql = ", ".join(self.db.quote_identifier(c) for c in all_order_cols)
                partition_sql = ", ".join(self.db.quote_identifier(c) for c in safe_groups)

                # ---- Fetch data ----
                cols_to_fetch = [safe_col] + safe_groups + safe_orders + [self._CH_ROW_NUM]
                seen = set()
                cols_to_fetch = [c for c in cols_to_fetch if not (c in seen or seen.add(c))]

                rows = await self._fetch(
                    f"SELECT {', '.join(self.db.quote_identifier(c) for c in cols_to_fetch)} "
                    f"FROM {working_qualified} "
                    f"ORDER BY {full_order_sql}"
                )
                df = pd.DataFrame(rows, columns=cols_to_fetch)

                # ---- Compute EWM per group (offloaded to thread) ----
                alpha_val = self._alpha_from_params(com, span, halflife, alpha)

                def _compute_all_groups():
                    n_loc = len(df)
                    loc_results = {f: np.full(n_loc, np.nan) for f in aggs}
                    grouped_df = df.groupby(safe_groups, sort=False)
                    for gk, gdf in grouped_df:
                        varr = np.array(
                            [v if v is not None else np.nan for v in gdf[safe_col]],
                            dtype=np.float64,
                        )
                        gres = GroupbyWindowOps._compute_ewm_array(
                            varr, alpha_val, adjust, ignore_na, min_periods, aggs
                        )
                        for f in aggs:
                            loc_results[f][gdf.index] = gres[f]
                    return loc_results

                all_results = await asyncio.to_thread(_compute_all_groups)
                n = len(df)

                # ---- Build result table ----
                new_table_name = await self._resolve_output_table_name(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                q_schema = self.db.quote_identifier(schema)
                q_new = self.db.quote_identifier(new_table_name)
                output_qualified = self._qualified_table(new_table_name, schema)
                new_col_names = [f"{safe_col}_ewm_{f}_by_{'_'.join(safe_groups)}" for f in aggs]

                # ---- Create staging Memory table ----
                stage_table = SQLIdentifierSanitizer.sanitize(
                    f"{new_table_name}__ewm_stage_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
                )
                q_stage = self._qualified_table(stage_table, schema)

                stage_col_defs = [f"{self.db.quote_identifier('rn')} UInt64"]
                for c in new_col_names:
                    stage_col_defs.append(f"{self.db.quote_identifier(c)} Nullable(Float64)")

                await self._exec(f"DROP TABLE IF EXISTS {q_stage}")
                await self._exec(
                    f"CREATE TABLE {q_stage} ({', '.join(stage_col_defs)}) ENGINE = Memory"
                )

                try:
                    # ---- Insert computed EWM values ----
                    insert_col_names = ["rn"] + new_col_names

                    def build_insert_rows():
                        rows_out = []
                        for i in range(n):
                            row = [i + 1]
                            for f in aggs:
                                val = all_results[f][i]
                                row.append(None if np.isnan(val) else float(val))
                            rows_out.append(row)
                        return rows_out

                    insert_rows_data = await asyncio.to_thread(build_insert_rows)

                    insert_chunk_size = 10_000
                    for start in range(0, n, insert_chunk_size):
                        stop = min(start + insert_chunk_size, n)
                        chunk = insert_rows_data[start:stop]
                        await self.db.insert_rows(
                            f"{schema}.{stage_table}",
                            chunk,
                            insert_col_names,
                        )

                    # ---- Create final table via LEFT JOIN ----
                    ewm_select_cols = ", ".join(
                        f"e.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}"
                        for c in new_col_names
                    )

                    final_sql = f"""
                    CREATE TABLE {output_qualified} AS
                    WITH __base AS (
                        SELECT *, ROW_NUMBER() OVER(ORDER BY {full_order_sql}) AS rn
                        FROM {working_qualified}
                    )
                    SELECT b.* EXCEPT({self._CH_ROW_NUM}, rn),
                        {ewm_select_cols}
                    FROM __base b
                    LEFT JOIN {q_stage} e ON b.rn = e.{self.db.quote_identifier('rn')}
                    """

                    await self._exec(final_sql)
                finally:
                    await self._exec(f"DROP TABLE IF EXISTS {q_stage}")

                # ---- Drop working table ----
                await self._exec(f"DROP TABLE IF EXISTS {working_qualified}")

                # ---- Preview ----
                preview_cols = [safe_col] + new_col_names
                if order_by:
                    preview_cols.extend(safe_orders)
                preview_cols.extend(safe_groups)
                res = await self._fetch_data(
                    new_table_name, schema, list(dict.fromkeys(preview_cols))
                )

                return self._success_response(
                    f"EWM {aggs} on '{column}' grouped by {safe_groups}",
                    res,
                    new_table=new_table_name,
                    mapped_table=table if map_feature else None,
                    mapped_columns=new_col_names if map_feature else [],
                    new_columns=new_col_names,
                    group_cols=safe_groups,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"ewm group-by error: {str(e)}\n{traceback.format_exc()}")
    
    async def _wait_clickhouse_mutation(self, table_qualified: str):
        """
        Wait for all pending ClickHouse mutations on the given table to finish.
        ClickHouse ALTER TABLE UPDATE is asynchronous for MergeTree.
        """
        if not isinstance(self.db, ClickHouseAdapter):
            return

        import asyncio
        # Extract just the table name from the qualified name
        # e.g., `upload`.`table__op_3` → table__op_3
        parts = table_qualified.replace('`', '').split('.')
        table_name_only = parts[-1] if len(parts) > 1 else parts[0]
        database = parts[0] if len(parts) > 1 else "currentDatabase()"

        for _ in range(120):  # timeout after ~120 seconds
            rows = await self._fetch(
                f"SELECT count() FROM system.mutations "
                f"WHERE database = '{database}' "
                f"AND table = '{table_name_only}' "
                f"AND is_done = 0"
            )
            if not rows or rows[0][0] == 0:
                return
            await asyncio.sleep(1)

        raise RuntimeError(
            f"ClickHouse mutations on {table_qualified} did not finish within timeout"
        )

    @staticmethod
    def _compute_ewm_array(arr, alpha, adjust, ignore_na, min_periods, aggs):
        """Reusable Python EWM computation; identical logic to DataWindowOps.ewm but returns dict of arrays."""
        n = len(arr)
        res = {k: np.full(n, np.nan) for k in aggs}
        valid_count = 0
        S = 0.0
        W = 0.0
        Z = 0.0
        Q = 0.0
        prev_mean = None
        prev_var = 0.0
        for i in range(n):
            x = arr[i]
            if np.isnan(x):
                if not ignore_na:
                    continue
                else:
                    for k in aggs:
                        res[k][i] = np.nan
                    continue
            valid_count += 1
            if adjust:
                S = x + (1 - alpha) * S
                W = 1 + (1 - alpha) * W
                Z = 1 + (1 - alpha) ** 2 * Z
                Q = x * x + (1 - alpha) * Q
                if valid_count >= min_periods:
                    mean = S / W
                    if "mean" in aggs:
                        res["mean"][i] = mean
                    if "sum" in aggs:
                        res["sum"][i] = S
                    if "var" in aggs or "std" in aggs:
                        denom = W - Z / W
                        if denom <= 0:
                            var = np.nan
                        else:
                            var = max((Q - (S * S) / W) / denom, 0)
                        if "var" in aggs:
                            res["var"][i] = var
                        if "std" in aggs:
                            res["std"][i] = math.sqrt(var) if not np.isnan(var) else np.nan
            else:
                if prev_mean is None:
                    prev_mean = x
                    if valid_count >= min_periods:
                        if "mean" in aggs:
                            res["mean"][i] = x
                        if "sum" in aggs:
                            res["sum"][i] = x
                        if "var" in aggs:
                            res["var"][i] = 0.0
                        if "std" in aggs:
                            res["std"][i] = 0.0
                    continue
                prev_var = (1 - alpha) * (prev_var + alpha * (x - prev_mean) ** 2)
                prev_mean = (1 - alpha) * prev_mean + alpha * x
                if valid_count >= min_periods:
                    if "mean" in aggs:
                        res["mean"][i] = prev_mean
                    if "sum" in aggs:
                        res["sum"][i] = x + (1 - alpha) * (res["sum"][i-1] if i>0 else 0)
                    if "var" in aggs:
                        res["var"][i] = prev_var
                    if "std" in aggs:
                        res["std"][i] = math.sqrt(prev_var)
        return res
