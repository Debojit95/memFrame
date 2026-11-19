from typing import Any, Dict, List, Optional, Union
from contextlib import nullcontext
import traceback
from datetime import datetime, timezone

from memframe.core.analytix._fetch import fetch_df as _fetch_df_rows
from memframe.core.analytix._generated import assert_owned_columns
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
import math


class WindowOps:
    """Core rolling window operations (SQL-based).

    Shared engine with DuckDB-flavoured defaults; PostgreSQL and ClickHouse
    subclass this and override the dialect hooks (and the few structurally
    divergent operations) so the generated SQL is byte-identical per backend.
    """
    # Test/tuning hook: set to force a block size (e.g. 4 in unit tests).
    EWM_BLOCK_SIZE: Optional[int] = None

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # --------------------------------------------------
    # Dialect hooks (overridden per backend)
    # --------------------------------------------------
    @property
    def _row_id_col(self) -> str:
        # ponytail: DuckDB-flavoured default; postgres.py returns "ctid".
        return "_mf_idx"

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
        if limit is None:
            limit = getattr(self.db, "preview_rows", None)
        sql = f"SELECT {col_clause} FROM {qualified}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return await _fetch_df_rows(self.db, sql)

    async def _resolve_ordering(self, table: str, schema: str, order_by: str):
        """Resolve the ordering expression.

        With no explicit ``order_by`` the immutable positional index is the
        stable row order, so no temporary column/table is needed.
        """
        if order_by:
            return SQLIdentifierSanitizer.sanitize(order_by), table
        return "_mf_idx", table

    # --------------------------------------------------
    # Feature-table helpers (narrow sort/pass + merge back)
    # --------------------------------------------------
    def _order_keys(self, order_by) -> List[str]:
        """Ordering keys: explicit columns, else the immutable upload index."""
        if not order_by:
            return ["_mf_idx"]
        keys = order_by if isinstance(order_by, (list, tuple)) else [order_by]
        safe = [SQLIdentifierSanitizer.sanitize(c) for c in keys]
        if "_mf_idx" not in safe:
            safe.append("_mf_idx")
        return safe

    def _std_pop_agg(self) -> str:
        return "STDDEV_POP"

    def _std_samp_agg(self) -> str:
        return "STDDEV_SAMP"

    def _var_pop_agg(self) -> str:
        return "VAR_POP"

    def _var_samp_agg(self) -> str:
        return "VAR_SAMP"

    @staticmethod
    def _resolve_ddof(ddof) -> int:
        if isinstance(ddof, bool) or not isinstance(ddof, int) or ddof < 0:
            raise ValueError("ddof must be an integer >= 0")
        return ddof

    def _std_over(self, col, order_sql, frame, part="", ddof=1) -> str:
        """Windowed standard deviation for any ddof (0/1 native)."""
        over = f"OVER ({part}ORDER BY {order_sql} {frame})"
        if ddof == 0:
            return f"{self._std_pop_agg()}({col}) {over}"
        if ddof == 1:
            return f"{self._std_samp_agg()}({col}) {over}"
        return f"SQRT({self._var_over(col, order_sql, frame, part, ddof)})"

    def _var_over(self, col, order_sql, frame, part="", ddof=1) -> str:
        """Windowed variance for any ddof (0/1 native, else the moment form)."""
        over = f"OVER ({part}ORDER BY {order_sql} {frame})"
        if ddof == 0:
            return f"{self._var_pop_agg()}({col}) {over}"
        if ddof == 1:
            return f"{self._var_samp_agg()}({col}) {over}"
        n = f"{self._count_expr(col)} {over}"
        s1 = f"SUM({col}) {over}"
        s2 = f"SUM(({col}) * ({col})) {over}"
        moment = (
            f"(({s2}) - ({s1}) * ({s1}) / ({n})) / (({n}) - {ddof})"
        )
        # pandas needs n > ddof; NULL otherwise (no negative variances).
        return f"CASE WHEN ({n}) > {ddof} THEN {moment} ELSE NULL END"

    @staticmethod
    def _reject_win_type(win_type):
        if win_type:
            raise NotImplementedError(
                "win_type applies to sum/mean only"
            )

    @staticmethod
    def _parse_window(window):
        """Return ("rows", n, tag) or ("time", seconds, tag)."""
        import datetime as _dt

        import pandas as pd

        if isinstance(window, bool):
            raise ValueError("window must be an int, timedelta or offset string")
        if isinstance(window, int):
            if window < 1:
                raise ValueError("window must be >= 1")
            return "rows", window, window
        if isinstance(window, _dt.timedelta):
            secs = window.total_seconds()
        elif isinstance(window, str):
            try:
                secs = pd.Timedelta(window).total_seconds()
            except ValueError as exc:
                raise ValueError(f"invalid window {window!r}") from exc
        else:
            raise ValueError(
                "window must be an int, timedelta or offset string"
            )
        if secs <= 0:
            raise ValueError("window must be positive")
        return "time", secs, f"{secs:g}s"

    def _reject_time_window(self, window):
        kind, _secs, _tag = self._parse_window(window)
        if kind == "time":
            raise NotImplementedError(
                "time-based windows support sum/mean/min/max/count/std/var"
            )

    def _epoch_expr(self, column_expr: str) -> str:
        return f"date_part('epoch', {column_expr})"

    async def _rolling_time_agg(
        self, table, schema, column, order_by, window, raw_aggs,
        min_periods, ddof, backend, data_id, new_table, group_cols=None,
        closed=None, center=False,
    ):
        """Correlated time-window aggregation (strict left edge, row order)."""
        spec = self._window_spec(window, center, closed)
        if spec["kind"] != "time":
            raise ValueError("window is not time-based")
        tag = spec["tag"]
        if isinstance(order_by, (list, tuple)):
            return self._error_response(
                "time-based windows need a single datetime order_by column"
            )
        if not order_by:
            return self._error_response(
                "time-based windows require order_by (the datetime column)"
            )
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        safe_groups = [
            SQLIdentifierSanitizer.sanitize(c) for c in (group_cols or [])
        ]
        orders = self._order_keys(order_by)
        order_type = await self._get_column_sql_type(table, schema, orders[0])
        if order_type and not any(
            marker in str(order_type).upper() for marker in ("DATE", "TIME")
        ):
            return self._error_response(
                "time-based windows need a datetime order_by column"
            )
        q = self.db.quote_identifier
        order_sql = ", ".join(q(c) for c in orders)
        qual = self._qualified_table(table, schema)

        aliases = {
            "sum": "sum", "mean": "mean", "avg": "mean", "min": "min",
            "max": "max", "count": "count", "std": "std", "stddev": "std",
            "stddev_samp": "std", "var": "var", "variance": "var",
            "var_samp": "var", "first": "first", "last": "last",
            "first_value": "first", "last_value": "last",
        }
        keys = [aliases.get(str(a).strip().lower()) for a in raw_aggs]
        if any(k is None for k in keys):
            return self._error_response(
                "time-based windows support "
                "sum/mean/min/max/count/std/var/first/last"
            )
        if not keys:
            return self._error_response(
                "At least one aggregation function is required"
            )

        group_cols_sql = ", ".join(q(c) for c in safe_groups)
        select_groups = f"{group_cols_sql}, " if safe_groups else ""
        partition = (
            f"PARTITION BY {group_cols_sql} " if safe_groups else ""
        )
        base = f"""__base AS (
                SELECT {q("_mf_idx")}, {select_groups}{q(safe_col)} AS v,
                       ROW_NUMBER() OVER ({partition}ORDER BY {order_sql}) AS __rn,
                       {self._epoch_expr(q(orders[0]))} AS __ord
                FROM {qual}
            )"""
        group_eq = ""
        if safe_groups:
            group_eq = "".join(
                f" AND r.{q(c)} = b.{q(c)}" for c in safe_groups
            )

        suffix = f"_by_{'_'.join(safe_groups)}" if safe_groups else ""
        frame_where = f"{self._frame_predicate(spec)}{group_eq}"

        def agg_sql(expr: str) -> str:
            return f"(SELECT {expr} FROM __base r WHERE {frame_where})"

        cnt = agg_sql("COUNT(r.v)")
        rows = agg_sql("COUNT(r.__rn)")
        source_type = await self._get_column_sql_type(table, schema, safe_col)
        float_type = self._ewm_float_type()
        exprs = []
        names = []
        col_types = []
        for key in keys:
            col_name = f"{safe_col}_rolling_{key}_w{tag}{suffix}"
            if key == "sum":
                value = agg_sql("SUM(r.v)")
            elif key == "mean":
                value = agg_sql("AVG(r.v)")
            elif key == "count":
                value = cnt
            elif key in ("min", "max"):
                value = agg_sql(f"{key.upper()}(r.v)")
            elif key in ("first", "last"):
                value = self._ordered_value_sql(frame_where, key)
            elif key == "std":
                value = agg_sql(self._agg_std("r.v", ddof, "COUNT(r.v)"))
            else:
                value = agg_sql(self._agg_var("r.v", ddof, "COUNT(r.v)"))
            gate_count = rows if key == "count" else cnt
            gated = self._min_periods_gate(value, gate_count, min_periods)
            if key == "sum" and min_periods == 0:
                gated = f"COALESCE({gated}, 0)"
            names.append(col_name)
            if key in ("first", "last"):
                col_types.append(
                    self._window_result_sql_type(
                        source_type, key, "FIRST_VALUE",
                    )
                )
            else:
                col_types.append(float_type)
            exprs.append(f"{gated} AS {q(col_name)}")

        feature_sql = f"""
            WITH {base}
            SELECT b.{q("_mf_idx")} AS {q("_mf_idx")},
                {", ".join(exprs)}
            FROM __base b
        """
        await self._merge_features(
            table, schema, feature_sql, names, col_types,
            backend=backend, data_id=data_id,
        )
        preview_cols = [column, *names]
        if order_by:
            preview_cols += [c for c in orders if c != "_mf_idx"]
        preview_cols += safe_groups
        preview_cols = list(dict.fromkeys(preview_cols))
        res = await self._preview_data(table, schema, preview_cols)
        response = self._success_response(
            f"Rolling {', '.join(keys)} on '{column}' (window={window})",
            res, new_table=table, new_columns=names, window=window,
            min_periods=min_periods, source_updated=True,
        )
        if len(names) == 1:
            response["new_column"] = names[0]
        return response

    def _agg_std(self, expr, ddof, count_sql):
        if ddof == 0:
            return f"{self._std_pop_agg()}({expr})"
        if ddof == 1:
            return f"{self._std_samp_agg()}({expr})"
        return f"SQRT({self._agg_var(expr, ddof, count_sql)})"

    def _agg_var(self, expr, ddof, count_sql):
        if ddof == 0:
            return f"{self._var_pop_agg()}({expr})"
        if ddof == 1:
            return f"{self._var_samp_agg()}({expr})"
        moment = (
            f"((SUM(({expr}) * ({expr})) - SUM({expr}) * SUM({expr}) / "
            f"{count_sql}) / NULLIF({count_sql} - {ddof}, 0))"
        )
        return f"CASE WHEN {count_sql} > {ddof} THEN {moment} ELSE NULL END"

    def _time_order_sql(self, order_by, q_col):
        """Ordering expression + frame for an epoch-based time window."""
        if isinstance(order_by, (list, tuple)):
            raise ValueError(
                "time-based windows need a single datetime order_by column"
            )
        if not order_by:
            raise ValueError(
                "time-based windows require order_by (the datetime column)"
            )
        col = SQLIdentifierSanitizer.sanitize(order_by)
        if isinstance(self.db, ClickHouseAdapter):
            return f"toUnixTimestamp({self.db.quote_identifier(col)})"
        return self.db.quote_identifier(col)

    _WIN_TYPES = (
        "boxcar", "triang", "bartlett", "parzen", "bohman", "blackman",
        "blackmanharris", "barthann", "nuttall", "hann", "hamming", "cosine",
    )
    _WIN_MAX = 512

    @classmethod
    def _window_weights(cls, win_type, window) -> list:
        """Closed-form scipy window weights (oldest row first)."""
        import math

        key = (win_type or "").lower()
        if key not in cls._WIN_TYPES:
            raise NotImplementedError(
                f"win_type={win_type!r} is not supported; supported: "
                f"{list(cls._WIN_TYPES)}"
            )
        w = int(window)
        if w < 1:
            raise ValueError("window must be >= 1")
        if w > cls._WIN_MAX:
            raise ValueError(
                f"win_type windows are capped at {cls._WIN_MAX} rows"
            )
        if w == 1:
            return [1.0]
        n = w - 1

        def cos(k, mult=1.0):
            return math.cos(mult * 2.0 * math.pi * k / n)

        out = []
        for i in range(w):
            if key == "boxcar":
                v = 1.0
            elif key == "triang":
                denom = (w + 1) / 2.0 if w % 2 else w / 2.0
                v = 1.0 - abs(i - n / 2.0) / denom
            elif key == "bartlett":
                v = 1.0 - abs(i - n / 2.0) / (n / 2.0)
            elif key == "hann":
                v = 0.5 - 0.5 * cos(i)
            elif key == "hamming":
                v = 0.54 - 0.46 * cos(i)
            elif key == "blackman":
                v = 0.42 - 0.5 * cos(i) + 0.08 * cos(i, 2.0)
            elif key == "blackmanharris":
                v = (
                    0.35875 - 0.48829 * cos(i) + 0.14128 * cos(i, 2.0)
                    - 0.01168 * cos(i, 3.0)
                )
            elif key == "barthann":
                v = (
                    0.62 - 0.48 * abs(i / n - 0.5)
                    - 0.38 * cos(i)
                )
            elif key == "nuttall":
                v = (
                    0.3635819 - 0.4891775 * cos(i) + 0.1365995 * cos(i, 2.0)
                    - 0.0106411 * cos(i, 3.0)
                )
            elif key == "cosine":
                v = math.sin(math.pi * (i + 0.5) / w)
            elif key == "bohman":
                if i in (0, w - 1):
                    v = 0.0  # scipy clips the radial endpoints
                else:
                    x = abs(2.0 * i / n - 1.0)
                    v = (
                        (1.0 - x) * math.cos(math.pi * x)
                        + math.sin(math.pi * x) / math.pi
                    )
            else:  # parzen
                u = abs(i - n / 2.0) / (w / 2.0)
                if u <= 0.5:
                    v = 1.0 - 6.0 * u * u * (1.0 - u)
                else:
                    v = 2.0 * (1.0 - u) ** 3
            out.append(v)
        return out

    async def _rolling_weighted(
        self, table, schema, column, order_by, window, raw_aggs,
        min_periods, ddof, win_type, backend, data_id, new_table,
        group_cols=None, center=False, closed=None,
    ):
        """Weighted rolling aggregates via a weights join (win_type)."""
        self._reject_time_window(window)
        if int(window) < 1:
            return self._error_response("window must be >= 1")
        if center or closed not in (None, "right"):
            raise NotImplementedError(
                "win_type supports the default frame only "
                "(center=False, closed='right')"
            )
        weights = self._window_weights(win_type, window)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        safe_groups = [
            SQLIdentifierSanitizer.sanitize(c) for c in (group_cols or [])
        ]
        orders = self._order_keys(order_by)
        w = int(window) - 1

        aliases = {"sum": "sum", "mean": "mean", "avg": "mean"}
        keys = [aliases.get(str(a).strip().lower()) for a in raw_aggs]
        bad = [a for a in raw_aggs if aliases.get(str(a).strip().lower()) is None]
        if bad:
            return self._error_response(
                f"win_type supports sum/mean only, not {bad} "
                "(pandas computes weighted std/var with rotating weights)"
            )

        if safe_groups:
            base = self._group_base_cte(
                table, schema, safe_col, safe_groups, orders,
            )
            group_eq = self._group_eq(safe_groups)
        else:
            base = self._window_base_cte(table, schema, safe_col, orders)
            group_eq = ""

        # Oldest frame row (offset w) carries weights[0].
        rows = [
            f"SELECT {w - i} AS __off, {weights[i]!r} AS __wt"
            for i in range(len(weights))
        ]
        weight_cte = "\n                    UNION ALL ".join(rows)

        suffix = f"_by_{'_'.join(safe_groups)}" if safe_groups else ""
        sw = "SUM(CASE WHEN r.v IS NOT NULL THEN w.__wt END)"
        swx = "SUM(w.__wt * r.v)"
        cnt = "SUM(CASE WHEN r.v IS NOT NULL THEN 1 ELSE 0 END)"
        exprs = []
        names = []
        for alias in keys:
            col_name = f"{safe_col}_rolling_{alias}_w{window}{suffix}"
            if alias == "sum":
                value = swx
            else:  # mean
                value = f"({swx} / NULLIF({sw}, 0))"
            value = self._min_periods_gate(value, f"({cnt})", min_periods)
            names.append(col_name)
            exprs.append(f"{value} AS {self.db.quote_identifier(col_name)}")

        feature_sql = f"""
            WITH {base},
            __w AS (
                    {weight_cte}
            )
            SELECT b.{self.db.quote_identifier("_mf_idx")} AS {self.db.quote_identifier("_mf_idx")},
                {", ".join(exprs)}
            FROM __base b
            JOIN __base r
              ON r.__rn BETWEEN b.__rn - {w} AND b.__rn{group_eq}
            JOIN __w w ON w.__off = b.__rn - r.__rn
            GROUP BY b.{self.db.quote_identifier("_mf_idx")}
        """
        await self._merge_features(
            table, schema, feature_sql, names,
            [self._ewm_float_type()] * len(names),
            backend=backend, data_id=data_id,
        )
        preview_cols = [column, *names]
        if order_by:
            preview_cols += [c for c in orders if c != "_mf_idx"]
        preview_cols += safe_groups
        preview_cols = list(dict.fromkeys(preview_cols))
        res = await self._preview_data(table, schema, preview_cols)
        response = self._success_response(
            f"Rolling {', '.join(keys)} on '{column}' "
            f"(window={window}, win_type={win_type})",
            res, new_table=table, new_columns=names,
            window=window, min_periods=min_periods, win_type=win_type,
            source_updated=True,
        )
        if len(names) == 1:
            response["new_column"] = names[0]
        return response

    _QUANTILE_INTERPOLATIONS = (
        "linear", "lower", "higher", "nearest", "midpoint",
    )

    @classmethod
    def _resolve_interpolation(cls, interpolation) -> str:
        key = (interpolation or "linear").lower()
        if key not in cls._QUANTILE_INTERPOLATIONS:
            raise ValueError(
                f"interpolation must be one of "
                f"{cls._QUANTILE_INTERPOLATIONS}"
            )
        if key != "linear":
            # ponytail: non-linear interpolation needs per-row element
            # selection, which the backends cannot do portably yet.
            raise NotImplementedError(
                f"interpolation={key!r} is not implemented yet; "
                "only 'linear' is supported"
            )
        return key

    _RANK_METHODS = ("average", "min", "max", "first", "dense")

    def _rank_sql(self, frame_where, count_sql, method="average",
                  ascending=True, pct=False) -> str:
        """Rank of the current row inside the frame, pandas semantics."""
        method = (method or "average").lower()
        if method not in self._RANK_METHODS:
            raise ValueError(
                f"rank method must be one of {self._RANK_METHODS}"
            )

        def cnt(cond: str) -> str:
            return (
                f"(SELECT COUNT(*) FROM __base r WHERE {frame_where} "
                f"AND {cond})"
            )

        lt, leq = ("<", "<=") if ascending else (">", ">=")
        if method == "min":
            value = f"({cnt(f'r.v {lt} b.v')} + 1)"
        elif method == "max":
            value = cnt(f"r.v {leq} b.v")
        elif method == "average":
            value = (
                f"({cnt(f'r.v {lt} b.v')} "
                f"+ ({cnt('r.v = b.v')} + 1) / 2.0)"
            )
        elif method == "first":
            value = (
                f"({cnt(f'r.v {lt} b.v')} "
                f"+ {cnt('r.v = b.v AND r.__rn < b.__rn')} + 1)"
            )
        else:  # dense
            value = (
                f"(SELECT COUNT(DISTINCT r.v) FROM __base r "
                f"WHERE {frame_where} AND r.v {leq} b.v)"
            )
        if pct:
            value = f"({value} / NULLIF({count_sql}, 0))"
        return f"CASE WHEN b.v IS NULL THEN NULL ELSE {value} END"

    def _rank_type(self, method, pct):
        method = (method or "average").lower()
        if pct or method == "average":
            return self._ewm_float_type()
        return self._ewm_int_type()

    def _sem_over(self, col, order_sql, frame, part="", ddof=1) -> str:
        """pandas: sem(ddof) is std(sample) / sqrt(n - ddof)."""
        over = f"OVER ({part}ORDER BY {order_sql} {frame})"
        n = f"{self._count_expr(col)} {over}"
        std = self._std_over(col, order_sql, frame, part, 1)
        denom = (
            f"CASE WHEN ({n}) > {ddof} THEN SQRT(({n}) - {ddof}) ELSE NULL END"
        )
        return f"({std} / {denom})"

    def _pop_std_agg(self) -> str:
        # Rolling std is population; expanding std is sample (see _std_agg).
        return "STDDEV_POP"

    def _count_expr(self, column_expr: str) -> str:
        return f"COUNT({column_expr})"

    async def _preview_data(self, table: str, schema: str, columns) -> Any:
        """Preview in _mf_idx order so it matches the cache replay."""
        qualified = self._qualified_table(table, schema)
        safe = list(dict.fromkeys(SQLIdentifierSanitizer.sanitize(c) for c in columns))
        col_sql = ", ".join(self.db.quote_identifier(c) for c in safe)
        limit = getattr(self.db, "preview_rows", None)
        sql = (
            f"SELECT {col_sql} FROM {qualified} "
            f'ORDER BY {self.db.quote_identifier("_mf_idx")}'
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        return await _fetch_df_rows(self.db, sql)

    async def _merge_features(
        self,
        table: str,
        schema: str,
        select_sql: str,
        columns: List[str],
        column_types: List[str],
        backend=None,
        data_id=None,
    ) -> None:
        """Add generated columns to the source and backfill them by _mf_idx."""
        try:
            existing = set(await self.db.get_column_types(table, schema) or {})
        except Exception:
            existing = set()
        added = [c for c in columns if c not in existing]
        qualified = self._qualified_table(table, schema)
        assignments = ", ".join(
            f"{self.db.quote_identifier(c)} = s.{self.db.quote_identifier(c)}"
            for c in columns
        )
        rid = self.db.quote_identifier(self._row_id_col)
        try:
            for col, col_type in zip(columns, column_types):
                await self._add_col(table, schema, col, col_type)
            await self._exec(
                f"UPDATE {qualified} AS t SET {assignments} "
                f"FROM ({select_sql}) AS s WHERE t.{rid} = s.{rid}"
            )
        except Exception:
            # Never leave a half-created column behind: it would look like a
            # foreign collision on the next run.
            for col in added:
                await self._exec(
                    f"ALTER TABLE {qualified} "
                    f"DROP COLUMN IF EXISTS {self.db.quote_identifier(col)}"
                )
            raise

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

    # ---------- Cache-table helpers (mirror other ops classes) ----------
    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await backend.fetchval(sql, *args)

    async def _generate_cache_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self._backend_fetch_val(
            backend,
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.cache_registry_table}
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
            candidate = await self._generate_cache_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    async def _source_star(
        self, table: str, schema: str, exclude=(), prefix: str = "",
        extra=(),
    ) -> str:
        # ponytail: SELECT * stays byte-identical unless a previous run left
        # one of the about-to-be-created columns on the source table — then an
        # explicit list minus those names avoids duplicate-column failures.
        try:
            cols = list(await self.db.get_column_types(table, schema) or {})
        except Exception:
            cols = []
        bare_star = "*" if not prefix else f"{prefix}.*"
        if not any(c in cols for c in exclude):
            return bare_star
        keep = [c for c in cols if c not in set(exclude)]
        qualifier = f"{prefix}." if prefix else ""
        parts = [f"{qualifier}{self.db.quote_identifier(c)}" for c in keep]
        parts += [f"{qualifier}{self.db.quote_identifier(c)}" for c in extra]
        return ", ".join(parts) if parts else bare_star

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

    async def _rolling_datetime_stat(
        self, table, schema, column, order_by=None, window=3, stat="mean",
        backend=None, data_id=None, new_table=None, min_periods=None,
        center: bool = False, closed: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            pred = self._frame_predicate(spec)
            stat_key = stat.lower()
            new_col = f"{safe_col}_rolling_{stat_key}_w{win_tag}"
            ord_expr = (
                self._epoch_expr(self.db.quote_identifier(orders[0]))
                if spec["kind"] == "time" else None
            )
            base = self._window_base_cte(
                table, schema, safe_col, orders, ord_expr=ord_expr,
            )
            is_date_only = await self._is_date_only_column(
                table, schema, safe_col,
            )
            from_epoch = self._from_epoch_expr(is_date_only)
            if stat_key == "mean":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._avg_fn() + f"({epoch_expr})"
                value_sql = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE {pred}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._median_epoch_sql(epoch_expr)
                value_sql = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE {pred}
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "mode":
                if isinstance(self.db, ClickHouseAdapter):
                    value_sql = f"""
                        SELECT argMin(v, (-cnt, v))
                        FROM (
                            SELECT r.v AS v, COUNT(*) AS cnt
                            FROM __base r
                            WHERE {pred}
                              AND r.v IS NOT NULL
                            GROUP BY r.v
                        )
                    """
                else:
                    value_sql = f"""
                        SELECT r.v
                        FROM __base r
                        WHERE {pred}
                          AND r.v IS NOT NULL
                        GROUP BY r.v
                        ORDER BY COUNT(*) DESC, r.v
                        LIMIT 1
                    """
            else:
                return self._error_response(
                    f"Unsupported datetime rolling stat '{stat}'"
                )
            count_subq = f"""
                SELECT COUNT(r.v) FROM __base r
                WHERE {pred}
            """
            expr = self._min_periods_gate(
                f"({value_sql})", f"({count_subq})", min_p,
            )
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            source_type = await self._get_column_sql_type(
                table, schema, safe_col,
            ) or self._ewm_float_type()
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                source_type, backend, data_id, order_by,
                f"Rolling datetime {stat_key} on '{column}' "
                f"(window={window}, min_periods={min_p})",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(
                f"rolling datetime error: {str(e)}\n{traceback.format_exc()}"
            )

    def _resolve_min_periods(self, min_periods, window) -> int:
        """Resolve pandas' rolling min_periods.

        Integer windows default to the window size; time windows default to 1.
        """
        kind, value, _tag = self._parse_window(window)
        default = int(value) if kind == "rows" else 1
        if min_periods is None:
            return default
        if isinstance(min_periods, bool) or not isinstance(min_periods, int):
            raise ValueError("min_periods must be an integer")
        if min_periods < 0:
            raise ValueError("min_periods must be >= 0")
        if kind == "rows" and min_periods > default:
            raise ValueError(
                f"min_periods {min_periods} must be <= window {default}"
            )
        return min_periods

    @staticmethod
    def _min_periods_gate(agg_sql: str, count_sql: str, min_periods: int) -> str:
        """Mask an aggregate until the frame holds min_periods values."""
        if min_periods >= 1:
            return (
                f"CASE WHEN {count_sql} >= {min_periods} "
                f"THEN {agg_sql} ELSE NULL END"
            )
        return agg_sql

    _ROLLING_CLOSED = ("right", "left", "both", "neither")

    def _rolling_offsets(self, window, center=False, closed=None):
        """Signed row offsets (start, end) of the rolling frame.

        Mirrors pandas: ``closed`` picks which edges are included, and
        ``center`` shifts the whole frame forward by (window - 1) // 2.
        """
        w = int(window)
        key = "right" if closed is None else closed
        if key not in self._ROLLING_CLOSED:
            raise ValueError(
                f"closed must be one of {self._ROLLING_CLOSED} or None"
            )
        lo, hi = {
            "right": (-(w - 1), 0),
            "left": (-w, -1),
            "both": (-w, 0),
            "neither": (-(w - 1), -1),
        }[key]
        if lo > hi:
            # Only reachable for window=1 + closed="neither": SQL has no
            # empty frame, and pandas returns an all-null column anyway.
            raise ValueError(
                "closed='neither' requires window >= 2 (empty frame)"
            )
        if center:
            shift = (w - 1) // 2
            lo += shift
            hi += shift
        return lo, hi

    @staticmethod
    def _frame_bound(offset: int) -> str:
        if offset == 0:
            return "CURRENT ROW"
        return f"{-offset} PRECEDING" if offset < 0 else f"{offset} FOLLOWING"

    def _window_spec(self, window, center=False, closed=None) -> dict:
        """Normalise a window into row offsets or time edges."""
        kind, value, tag = self._parse_window(window)
        if kind == "rows":
            lo, hi = self._rolling_offsets(value, center, closed)
            return {"kind": "rows", "lo": lo, "hi": hi, "tag": value}
        key = "right" if closed is None else closed
        if key not in self._ROLLING_CLOSED:
            raise ValueError(
                f"closed must be one of {self._ROLLING_CLOSED} or None"
            )
        return {
            "kind": "time", "secs": value, "closed": key, "tag": tag,
            "center": bool(center),
        }

    def _frame_predicate(self, spec, alias="r", base="b") -> str:
        """Correlated frame predicate for a window spec."""
        if spec["kind"] == "rows":
            return (
                f"{alias}.__rn BETWEEN {base}.__rn + ({spec['lo']}) "
                f"AND {base}.__rn + ({spec['hi']})"
            )
        key = spec["closed"]
        left = ">" if key in ("right", "neither") else ">="
        right = "<=" if key in ("right", "both") else "<"
        if spec.get("center"):
            half = spec["secs"] / 2.0
            return (
                f"{alias}.__ord {left} {base}.__ord - {half:g} "
                f"AND {alias}.__ord {right} {base}.__ord + {half:g}"
                f"{self._center_anchor(alias, base)}"
            )
        return (
            f"{alias}.__rn <= {base}.__rn "
            f"AND {alias}.__ord {left} {base}.__ord - {spec['secs']:g} "
            f"AND {alias}.__ord {right} {base}.__ord"
        )

    def _center_anchor(self, alias="r", base="b") -> str:
        """Extra correlation anchor for centered time frames."""
        return ""

    def _ordered_value_sql(self, where, mode, value_expr="r.v") -> str:
        """Value at the earliest (first) or latest (last) row in the frame."""
        order = "__rn" if mode == "first" else "__rn DESC"
        return (
            f"(SELECT (array_agg({value_expr} ORDER BY {order}))[1] "
            f"FROM __base r WHERE {where})"
        )

    def _rolling_frame(self, window, center=False, closed=None) -> str:
        lo, hi = self._rolling_offsets(window, center, closed)
        return f"ROWS BETWEEN {self._frame_bound(lo)} AND {self._frame_bound(hi)}"

    def _rolling_rn(self, window, center=False, closed=None):
        """Correlated-subquery bounds for the ``b.__rn`` row number."""
        lo, hi = self._rolling_offsets(window, center, closed)
        return f"b.__rn + ({lo})", f"b.__rn + ({hi})"

    def _distinct_count_sql(self, expr: str) -> str:
        return f"COUNT(DISTINCT {expr})"

    def _window_base_cte(
        self, table, schema, safe_col, orders, name="__base", ord_expr=None,
    ) -> str:
        q = self.db.quote_identifier
        order_sql = ", ".join(q(c) for c in orders)
        qualified = self._qualified_table(table, schema)
        extra = f", {ord_expr} AS __ord" if ord_expr else ""
        return f"""{name} AS (
                SELECT {q("_mf_idx")}, {q(safe_col)} AS v,
                       ROW_NUMBER() OVER (ORDER BY {order_sql}) AS __rn{extra}
                FROM {qualified}
            )"""

    async def _finish_window_special(
        self, table, schema, column, new_col, feature_sql, col_type,
        backend, data_id, order_by, message, **extra,
    ):
        collision = await assert_owned_columns(
            self, table, schema, [new_col], "window_map", backend, data_id,
        )
        if collision:
            return self._error_response(collision)
        await self._merge_features(
            table, schema, feature_sql, [new_col], [col_type],
            backend=backend, data_id=data_id,
        )
        preview_cols = [column, new_col]
        if order_by:
            preview_cols += [
                c for c in self._order_keys(order_by) if c != "_mf_idx"
            ]
        preview_cols = list(dict.fromkeys(preview_cols))
        res = await self._preview_data(table, schema, preview_cols)
        return self._success_response(
            message, res, new_table=table, new_column=new_col,
            source_updated=True, **extra,
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
    min_periods: Optional[int] = None,
    center: bool = False,
    closed: Optional[str] = None,
    ddof: int = 1,
    win_type: Optional[str] = None,
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

            kind, win_value, tag = self._parse_window(window)

            if win_type:
                return await self._rolling_weighted(
                    table, schema, column, order_by, window, raw_aggs,
                    self._resolve_min_periods(min_periods, window),
                    self._resolve_ddof(ddof), win_type, backend, data_id,
                    new_table, center=center, closed=closed,
                )

            if kind == "time":
                return await self._rolling_time_agg(
                    table, schema, column, order_by, window, raw_aggs,
                    self._resolve_min_periods(min_periods, window),
                    self._resolve_ddof(ddof), backend, data_id, new_table,
                    closed=closed, center=center,
                )

            # --- 1. Narrow ordering: explicit keys, else the upload index ---
            min_p = self._resolve_min_periods(min_periods, window)
            ddof = self._resolve_ddof(ddof)
            q_col = self.db.quote_identifier(safe_col)
            safe_orders = self._order_keys(order_by)
            order_sql = ", ".join(
                self.db.quote_identifier(c) for c in safe_orders
            )
            frame = self._rolling_frame(window, center, closed)
            qualified = self._qualified_table(table, schema)
            source_col_type = await self._get_column_sql_type(table, schema, safe_col)
            count_over = (
                f"{self._count_expr(q_col)} OVER "
                f"(ORDER BY {order_sql} {frame})"
            )
            rows_over = f"COUNT(1) OVER (ORDER BY {order_sql} {frame})"

            # --- 2. Function mapping (all aggs in one window pass) ---
            agg_map = {
                "sum": "SUM", "avg": "AVG", "mean": "AVG",
                "min": "MIN", "max": "MAX", "count": "COUNT",
                "first_value": "FIRST_VALUE", "last_value": "LAST_VALUE",
                "first": "FIRST_VALUE", "last": "LAST_VALUE",
            }
            new_cols: List[str] = []
            exprs: List[str] = []
            col_types: List[str] = []
            used_col_names = set()

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in agg_map:
                    sql_agg = agg_map[key]
                    alias_key = key
                elif key in ("std", "stddev", "stddev_pop", "stddev_samp"):
                    sql_agg = self._std_samp_agg()
                    alias_key = "std"
                elif key in ("var", "variance", "var_samp"):
                    sql_agg = self._var_samp_agg()
                    alias_key = "var"
                else:
                    return self._error_response(f"Unsupported aggregation '{raw}' for _rolling_agg")

                base_col_name = f"{safe_col}_rolling_{alias_key}_w{tag}"
                col_name = base_col_name
                suffix = 2
                while col_name in used_col_names:
                    col_name = f"{base_col_name}_{suffix}"
                    suffix += 1
                used_col_names.add(col_name)
                new_cols.append(col_name)
                agg_call = (
                    self._count_expr(q_col)
                    if alias_key == "count"
                    else f"{sql_agg}({q_col})"
                )
                if alias_key == "std":
                    value_sql = self._std_over(q_col, order_sql, frame, "", ddof)
                elif alias_key == "var":
                    value_sql = self._var_over(q_col, order_sql, frame, "", ddof)
                else:
                    value_sql = f"{agg_call} OVER (ORDER BY {order_sql} {frame})"
                # pandas gates count on the rows in the frame, not on the
                # non-null observations; other aggs are already NULL when the
                # frame is empty, so they only need the gate above one.
                gate_count = rows_over if alias_key == "count" else count_over
                gate_at = min_p if (alias_key == "count" or min_p > 1) else 0
                value_sql = self._min_periods_gate(
                    value_sql, gate_count, gate_at,
                )
                if alias_key == "sum" and min_p == 0:
                    value_sql = f"COALESCE({value_sql}, 0)"
                exprs.append(
                    f"{value_sql} AS {self.db.quote_identifier(col_name)}"
                )
                col_types.append(
                    self._window_result_sql_type(source_col_type, alias_key, sql_agg)
                )

            collision = await assert_owned_columns(
                self, table, schema, new_cols, "window_map", backend, data_id,
            )
            if collision:
                return self._error_response(collision)

            # --- 3. Compute + map back once by _mf_idx ---
            select_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                + ", ".join(exprs)
                + f" FROM {qualified}"
            )
            await self._merge_features(
                table, schema, select_sql, new_cols, col_types,
                backend=backend, data_id=data_id,
            )

            # --- 4. Preview (ordered like the cache replay) ---
            preview_cols = [*new_cols, column]
            if order_by:
                preview_cols += [k for k in safe_orders if k != "_mf_idx"]
            res = await self._preview_data(table, schema, preview_cols)

            response = self._success_response(
                f"Rolling {', '.join(raw_aggs)} on '{column}' (window={window})",
                res,
                new_table=table,
                new_columns=new_cols,
                window=window,
                min_periods=min_p,
                source_updated=True,
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
        min_periods: Optional[int] = None,
        center: bool = False,
        closed: Optional[str] = None,
        label: Optional[str] = None,
        interpolation: str = "linear",
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            self._resolve_interpolation(interpolation)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            pred = self._frame_predicate(spec)
            frame = (
                self._rolling_frame(window, center, closed)
                if spec["kind"] == "rows" else None
            )
            new_col = f"{safe_col}_rolling_{label or f'q{q}'}_w{win_tag}"

            if isinstance(self.db, DuckDBAdapter) and spec["kind"] == "rows":
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                agg_over = (
                    f"{self._quantile_cont_sql(q_col, q)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                expr = self._min_periods_gate(agg_over, count_over, min_p)
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                ord_expr = (
                    self._epoch_expr(self.db.quote_identifier(orders[0]))
                    if spec["kind"] == "time" else None
                )
                base = self._window_base_cte(
                    table, schema, safe_col, orders, ord_expr=ord_expr,
                )
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE {pred}
                """
                quantile_subq = f"""
                    SELECT {self._quantile_cont_sql("r.v", q)}
                    FROM __base r
                    WHERE {pred}
                      AND r.v IS NOT NULL
                """
                expr = self._min_periods_gate(
                    f"({quantile_subq})", f"({count_subq})", min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                "Rolling quantile",
                window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def rolling_median(self, *args, **kwargs):
        kwargs.setdefault("q", 0.5)
        kwargs.setdefault("label", "median")
        return await self.rolling_quantile(*args, **kwargs)

    async def rolling_sem(
        self,
        table,
        schema,
        column,
        order_by: Union[str, List[str]] = None,
        window=3, backend=None, data_id=None,
        new_table: Optional[str] = None,
        min_periods: Optional[int] = None,
        center: bool = False,
        closed: Optional[str] = None,
        ddof: int = 1,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            ddof = self._resolve_ddof(ddof)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_rolling_sem_w{win_tag}"
            if spec["kind"] == "time":
                ord_expr = self._epoch_expr(
                    self.db.quote_identifier(orders[0])
                )
                base = self._window_base_cte(
                    table, schema, safe_col, orders, ord_expr=ord_expr,
                )
                pred = self._frame_predicate(spec)
                n_sql = f"(SELECT COUNT(r.v) FROM __base r WHERE {pred})"
                std_sql = (
                    f"(SELECT {self._std_samp_agg()}(r.v) FROM __base r "
                    f"WHERE {pred})"
                )
                denom = (
                    f"CASE WHEN {n_sql} > {ddof} "
                    f"THEN SQRT({n_sql} - {ddof}) ELSE NULL END"
                )
                expr = self._min_periods_gate(
                    f"({std_sql} / {denom})", n_sql, min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
                return await self._finish_window_special(
                    table, schema, column, new_col, feature_sql,
                    self._ewm_float_type(), backend, data_id, order_by,
                    "Rolling SEM",
                    window=window, min_periods=min_p,
                )
            frame = self._rolling_frame(window, center, closed)
            count_over = (
                f"{self._count_expr(q_col)} OVER "
                f"(ORDER BY {order_sql} {frame})"
            )
            sem_sql = self._sem_over(q_col, order_sql, frame, "", ddof)
            expr = self._min_periods_gate(sem_sql, count_over, min_p)
            feature_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                f"{expr} AS {self.db.quote_identifier(new_col)} "
                f"FROM {self._qualified_table(table, schema)}"
            )
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                "Rolling SEM",
                window=window, min_periods=min_p,
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
        min_periods: Optional[int] = None,
        center: bool = False,
        closed: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            pred = self._frame_predicate(spec)
            frame = (
                self._rolling_frame(window, center, closed)
                if spec["kind"] == "rows" else None
            )
            new_col = f"{safe_col}_rolling_nunique_w{win_tag}"

            if isinstance(self.db, DuckDBAdapter) and spec["kind"] == "rows":
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                distinct_over = (
                    f"COUNT(DISTINCT {q_col}) OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                expr = self._min_periods_gate(
                    distinct_over, count_over, min_p,
                )
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                ord_expr = (
                    self._epoch_expr(self.db.quote_identifier(orders[0]))
                    if spec["kind"] == "time" else None
                )
                base = self._window_base_cte(
                    table, schema, safe_col, orders, ord_expr=ord_expr,
                )
                count_subq = f"""
                    SELECT COUNT(r.v) FROM __base r
                    WHERE {pred}
                """
                distinct_subq = f"""
                    SELECT {self._distinct_count_sql("r.v")}
                    FROM __base r
                    WHERE {pred}
                """
                expr = self._min_periods_gate(
                    f"({distinct_subq})", f"({count_subq})", min_p,
                )
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_int_type(), backend, data_id, order_by,
                "Rolling nunique",
                window=window, min_periods=min_p,
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
        min_periods: Optional[int] = None,
        center: bool = False,
        closed: Optional[str] = None,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            pred = self._frame_predicate(spec)
            new_col = f"{safe_col}_rolling_rank_w{win_tag}"
            ord_expr = (
                self._epoch_expr(self.db.quote_identifier(orders[0]))
                if spec["kind"] == "time" else None
            )
            base = self._window_base_cte(
                table, schema, safe_col, orders, ord_expr=ord_expr,
            )
            frame_where = pred
            count_sql = (
                f"(SELECT COUNT(r.v) FROM __base r WHERE {frame_where})"
            )
            rank_expr = self._rank_sql(
                frame_where, count_sql, method, ascending, pct,
            )
            expr = self._min_periods_gate(rank_expr, count_sql, min_p)
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._rank_type(method, pct), backend, data_id, order_by,
                "Rolling rank",
                window=window, min_periods=min_p,
                rank_method=method, ascending=ascending, pct=pct,
            )
        except Exception as e:
            return self._error_response(str(e))

    # --------------------------------------------------
    # SKEW / KURT
    # --------------------------------------------------
    def _moment_sql(self, stat, where, min_periods, count_sql):
        """Correlated skew/kurt expression over a frame predicate."""
        stat = stat.lower()
        a4 = ", AVG(r.v * r.v * r.v * r.v) AS __a4" if stat == "kurt" else ""
        level1 = f"""
            SELECT COUNT(*) AS __n, MIN(r.v) AS __mn, MAX(r.v) AS __mx,
                   AVG(r.v) AS __a, AVG(r.v * r.v) AS __a2,
                   AVG(r.v * r.v * r.v) AS __a3{a4}
            FROM __base r
            WHERE {where} AND r.v IS NOT NULL
        """
        pick_a4 = ", __a4" if stat == "kurt" else ""
        level2 = f"""
            SELECT __n, __mn, __mx, __a, __a2, __a3{pick_a4},
                   __a2 - __a * __a AS __b,
                   __a3 - __a * __a * __a
                     - 3 * __a * (__a2 - __a * __a) AS __c
            FROM ({level1}) __l1
        """
        if stat == "skew":
            outer = f"""
                SELECT CASE
                    WHEN __n < 3 THEN NULL
                    WHEN __mx = __mn THEN 0
                    WHEN __b <= 1e-14 THEN NULL
                    ELSE SQRT(__n * (__n - 1)) * __c
                         / ((__n - 2) * __b * SQRT(__b))
                END AS __val
                FROM ({level2}) __l2
            """
        else:
            level3 = f"""
                SELECT __n, __mn, __mx, __a, __b, __c,
                       __a4 - __a * __a * __a * __a
                         - 6 * __b * __a * __a - 4 * __c * __a AS __d
                FROM ({level2}) __l2
            """
            outer = f"""
                SELECT CASE
                    WHEN __n < 4 THEN NULL
                    WHEN __mx = __mn THEN -3
                    WHEN __b <= 1e-14 THEN NULL
                    ELSE ((__n * __n - 1) * __d / (__b * __b)
                          - 3 * (__n - 1) * (__n - 1))
                         / ((__n - 2) * (__n - 3))
                END AS __val
                FROM ({level3}) __l3
            """
        return self._min_periods_gate(f"({outer})", count_sql, min_periods)

    async def _rolling_moment(
        self, table, schema, column, order_by, window, stat, backend=None,
        data_id=None, new_table=None, min_periods=None, center=False,
        closed=None, win_type=None, group_cols=None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            spec = self._window_spec(window, center, closed)
            win_tag = spec["tag"]
            min_p = self._resolve_min_periods(min_periods, window)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [
                SQLIdentifierSanitizer.sanitize(c) for c in (group_cols or [])
            ]
            orders = self._order_keys(order_by)
            ord_expr = (
                self._epoch_expr(self.db.quote_identifier(orders[0]))
                if spec["kind"] == "time" else None
            )
            q = self.db.quote_identifier
            suffix = f"_by_{'_'.join(safe_groups)}" if safe_groups else ""
            new_col = f"{safe_col}_rolling_{stat.lower()}_w{win_tag}{suffix}"
            if safe_groups:
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                    ord_expr=ord_expr,
                )
                group_eq = self._group_eq(safe_groups)
            else:
                base = self._window_base_cte(
                    table, schema, safe_col, orders, ord_expr=ord_expr,
                )
                group_eq = ""
            where = f"{self._frame_predicate(spec)}{group_eq}"
            count_sql = f"(SELECT COUNT(r.v) FROM __base r WHERE {where})"
            expr = self._moment_sql(stat, where, min_p, count_sql)
            feature_sql = f"""
                WITH {base}
                SELECT b.{q("_mf_idx")},
                    {expr} AS {q(new_col)}
                FROM __base b
            """
            message = (
                f"Rolling {stat.lower()} on '{column}'"
                + (f" grouped by {safe_groups}" if safe_groups else "")
            )
            if safe_groups:
                return await self._finish_grouped_special(
                    table, schema, column, new_col, feature_sql,
                    self._ewm_float_type(), backend, data_id,
                    safe_groups, order_by, message,
                    window=window, min_periods=min_p,
                )
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                message, window=window, min_periods=min_p,
            )
        except Exception as e:
            return self._error_response(f"rolling {stat} error: {str(e)}")

    async def _expanding_moment(
        self, table, schema, column, order_by, stat, backend=None,
        data_id=None, new_table=None, min_periods=1, win_type=None,
        group_cols=None,
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_groups = [
                SQLIdentifierSanitizer.sanitize(c) for c in (group_cols or [])
            ]
            orders = self._order_keys(order_by)
            q = self.db.quote_identifier
            suffix = f"_by_{'_'.join(safe_groups)}" if safe_groups else ""
            new_col = f"{safe_col}_expanding_{stat.lower()}{suffix}"
            if safe_groups:
                base = self._group_base_cte(
                    table, schema, safe_col, safe_groups, orders,
                )
                group_eq = self._group_eq(safe_groups)
            else:
                base = self._window_base_cte(table, schema, safe_col, orders)
                group_eq = ""
            where = f"r.__rn <= b.__rn{group_eq}"
            count_sql = f"(SELECT COUNT(r.v) FROM __base r WHERE {where})"
            expr = self._moment_sql(stat, where, int(min_periods or 0), count_sql)
            feature_sql = f"""
                WITH {base}
                SELECT b.{q("_mf_idx")},
                    {expr} AS {q(new_col)}
                FROM __base b
            """
            message = (
                f"Expanding {stat.lower()} on '{column}'"
                + (f" grouped by {safe_groups}" if safe_groups else "")
            )
            if safe_groups:
                return await self._finish_grouped_special(
                    table, schema, column, new_col, feature_sql,
                    self._ewm_float_type(), backend, data_id,
                    safe_groups, order_by, message, min_periods=min_periods,
                )
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                message, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(f"expanding {stat} error: {str(e)}")

    async def rolling_skew(self, *args, **kwargs):
        return await self._rolling_moment(*args, stat="skew", **kwargs)

    async def rolling_kurt(self, *args, **kwargs):
        return await self._rolling_moment(*args, stat="kurt", **kwargs)

    async def expanding_skew(self, *args, **kwargs):
        return await self._expanding_moment(*args, stat="skew", **kwargs)

    async def expanding_kurt(self, *args, **kwargs):
        return await self._expanding_moment(*args, stat="kurt", **kwargs)

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
        ddof: int = 1,
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

            # --- 1. Narrow ordering: explicit keys, else the upload index ---
            ddof = self._resolve_ddof(ddof)
            safe_orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in safe_orders)
            qualified = self._qualified_table(table, schema)
            window_frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            q_col = self.db.quote_identifier(safe_col)
            source_col_type = await self._get_column_sql_type(table, schema, safe_col)

            # --- 2. Aggregation mapping (all aggs in one pass) ---
            agg_map = {
                "sum": "SUM", "avg": "AVG", "mean": "AVG",
                "min": "MIN", "max": "MAX", "count": "COUNT",
                "first": "FIRST_VALUE", "last": "LAST_VALUE",
                "first_value": "FIRST_VALUE", "last_value": "LAST_VALUE",
            }
            new_cols: List[str] = []
            exprs: List[str] = []
            col_types: List[str] = []
            used_col_names = set()

            for raw in raw_aggs:
                key = raw.strip().lower()
                if key in agg_map:
                    sql_agg = agg_map[key]
                    alias_key = key
                elif key in ("std", "stddev", "stddev_samp", "stddevsamp"):
                    sql_agg = self._std_agg()
                    alias_key = "std"
                elif key in ("var", "variance", "var_samp", "varsamp"):
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

                count_over = (
                    f"{self._count_expr(q_col)} "
                    f"OVER (ORDER BY {order_sql} {window_frame})"
                )
                agg_call = (
                    self._count_expr(q_col)
                    if alias_key == "count"
                    else f"{sql_agg}({q_col})"
                )
                if alias_key == "std":
                    agg_over = self._std_over(
                        q_col, order_sql, window_frame, "", ddof,
                    )
                elif alias_key == "var":
                    agg_over = self._var_over(
                        q_col, order_sql, window_frame, "", ddof,
                    )
                else:
                    agg_over = (
                        f"{agg_call} OVER (ORDER BY {order_sql} {window_frame})"
                    )
                if min_periods > 1:
                    agg_with_null = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {agg_over} ELSE NULL END"
                    )
                else:
                    agg_with_null = agg_over
                exprs.append(
                    f"{agg_with_null} AS {self.db.quote_identifier(col_name)}"
                )
                col_types.append(
                    self._window_result_sql_type(source_col_type, alias_key, sql_agg)
                )

            collision = await assert_owned_columns(
                self, table, schema, new_cols, "window_map", backend, data_id,
            )
            if collision:
                return self._error_response(collision)

            # --- 3. Compute + map back once by _mf_idx ---
            select_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                + ", ".join(exprs)
                + f" FROM {qualified}"
            )
            await self._merge_features(
                table, schema, select_sql, new_cols, col_types,
                backend=backend, data_id=data_id,
            )

            # --- 4. Preview (ordered like the cache replay) ---
            preview_cols = [*new_cols, column]
            if order_by:
                preview_cols += [k for k in safe_orders if k != "_mf_idx"]
            res = await self._preview_data(table, schema, preview_cols)

            response = self._success_response(
                f"Expanding {', '.join(raw_aggs)} on '{column}' (min_periods={min_periods})",
                res,
                new_table=table,
                new_columns=new_cols,
                min_periods=min_periods,
                source_updated=True,
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
        label: Optional[str] = None,
        interpolation: str = "linear",
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            self._resolve_interpolation(interpolation)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_expanding_{label or f'q{q}'}"
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

            if isinstance(self.db, DuckDBAdapter):
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                agg_over = (
                    f"{self._quantile_cont_sql(q_col, q)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {agg_over} ELSE NULL END"
                    )
                else:
                    expr = agg_over
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                base = self._window_base_cte(table, schema, safe_col, orders)
                count_subq = """
                    SELECT COUNT(r.v) FROM __base r
                    WHERE r.__rn <= b.__rn
                """
                quantile_subq = f"""
                    SELECT {self._quantile_cont_sql("r.v", q)}
                    FROM __base r
                    WHERE r.__rn <= b.__rn
                      AND r.v IS NOT NULL
                """
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({quantile_subq}) ELSE NULL END"
                    )
                else:
                    expr = f"({quantile_subq})"
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                "Expanding quantile",
                q=q, min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def expanding_median(self, *args, **kwargs):
        kwargs.setdefault("q", 0.5)
        kwargs.setdefault("label", "median")
        return await self.expanding_quantile(*args, **kwargs)

    async def expanding_sem(
        self, table, schema, column,
        order_by=None, min_periods=1, ddof=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            ddof = self._resolve_ddof(ddof)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_expanding_sem"
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            count_over = (
                f"{self._count_expr(q_col)} OVER "
                f"(ORDER BY {order_sql} {frame})"
            )
            sem_base = self._sem_over(q_col, order_sql, frame, "", ddof)
            if min_periods > 1:
                expr = (
                    f"CASE WHEN {count_over} >= {min_periods} "
                    f"THEN {sem_base} ELSE NULL END"
                )
            else:
                expr = sem_base
            feature_sql = (
                f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                f"{expr} AS {self.db.quote_identifier(new_col)} "
                f"FROM {self._qualified_table(table, schema)}"
            )
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_float_type(), backend, data_id, order_by,
                "Expanding SEM", min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def expanding_rank(
        self, table, schema, column,
        order_by=None, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            new_col = f"{safe_col}_expanding_rank"
            base = self._window_base_cte(table, schema, safe_col, orders)
            frame_where = "r.__rn <= b.__rn"
            count_sql = (
                f"(SELECT COUNT(r.v) FROM __base r WHERE {frame_where})"
            )
            rank_expr = self._rank_sql(
                frame_where, count_sql, method, ascending, pct,
            )
            expr = self._min_periods_gate(
                rank_expr, count_sql, int(min_periods or 0),
            )
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {expr} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._rank_type(method, pct), backend, data_id, order_by,
                "Expanding rank", min_periods=min_periods,
                rank_method=method, ascending=ascending, pct=pct,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def expanding_nunique(
        self, table, schema, column,
        order_by=None, min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            q_col = self.db.quote_identifier(safe_col)
            new_col = f"{safe_col}_expanding_nunique"
            frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

            if isinstance(self.db, DuckDBAdapter):
                count_over = (
                    f"{self._count_expr(q_col)} OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                distinct_over = (
                    f"COUNT(DISTINCT {q_col}) OVER "
                    f"(ORDER BY {order_sql} {frame})"
                )
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN {count_over} >= {min_periods} "
                        f"THEN {distinct_over} ELSE NULL END"
                    )
                else:
                    expr = distinct_over
                feature_sql = (
                    f'SELECT {self.db.quote_identifier("_mf_idx")}, '
                    f"{expr} AS {self.db.quote_identifier(new_col)} "
                    f"FROM {self._qualified_table(table, schema)}"
                )
            else:
                base = self._window_base_cte(table, schema, safe_col, orders)
                distinct_subq = f"""
                    SELECT {self._distinct_count_sql("r.v")}
                    FROM __base r
                    WHERE r.__rn <= b.__rn
                """
                count_subq = """
                    SELECT COUNT(r.v) FROM __base r
                    WHERE r.__rn <= b.__rn
                """
                if min_periods > 1:
                    expr = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({distinct_subq}) ELSE NULL END"
                    )
                else:
                    expr = f"({distinct_subq})"
                feature_sql = f"""
                    WITH {base}
                    SELECT b.{self.db.quote_identifier("_mf_idx")},
                        {expr} AS {self.db.quote_identifier(new_col)}
                    FROM __base b
                """
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                self._ewm_int_type(), backend, data_id, order_by,
                "Expanding nunique", min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def _expanding_datetime_stat(
        self, table, schema, column,
        order_by=None, stat="mean", min_periods=1,
        backend=None, data_id=None,
        new_table: Optional[str] = None,
        win_type: Optional[str] = None
    ):
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            self._reject_win_type(win_type)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            orders = self._order_keys(order_by)
            stat_key = stat.lower()
            new_col = f"{safe_col}_expanding_{stat_key}"
            base = self._window_base_cte(table, schema, safe_col, orders)
            is_date_only = await self._is_date_only_column(
                table, schema, safe_col,
            )
            from_epoch = self._from_epoch_expr(is_date_only)
            if stat_key == "mean":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._avg_fn() + f"({epoch_expr})"
                value_subq = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "median":
                epoch_expr = self._datetime_epoch_expr("r.v")
                inner = self._median_epoch_sql(epoch_expr)
                value_subq = f"""
                    SELECT {from_epoch}
                    FROM (
                        SELECT {inner} AS value_epoch
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          AND r.v IS NOT NULL
                    ) __agg
                """
            elif stat_key == "mode":
                if isinstance(self.db, ClickHouseAdapter):
                    mode_subq = """
                        SELECT argMin(v, (-cnt, v))
                        FROM (
                            SELECT r.v AS v, COUNT(*) AS cnt
                            FROM __base r
                            WHERE r.__rn <= b.__rn
                              AND r.v IS NOT NULL
                            GROUP BY r.v
                        )
                    """
                else:
                    mode_subq = """
                        SELECT r.v
                        FROM __base r
                        WHERE r.__rn <= b.__rn
                          AND r.v IS NOT NULL
                        GROUP BY r.v
                        ORDER BY COUNT(*) DESC, r.v
                        LIMIT 1
                    """
            else:
                return self._error_response(
                    f"Unsupported datetime expanding stat '{stat}'"
                )
            count_subq = """
                SELECT COUNT(r.v) FROM __base r
                WHERE r.__rn <= b.__rn
            """
            if stat_key == "mode":
                if min_periods > 1:
                    value_sql = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({mode_subq}) ELSE NULL END"
                    )
                else:
                    value_sql = f"({mode_subq})"
            else:
                if min_periods > 1:
                    value_sql = (
                        f"CASE WHEN ({count_subq}) >= {min_periods} "
                        f"THEN ({value_subq}) ELSE NULL END"
                    )
                else:
                    value_sql = f"({value_subq})"
            feature_sql = f"""
                WITH {base}
                SELECT b.{self.db.quote_identifier("_mf_idx")},
                    {value_sql} AS {self.db.quote_identifier(new_col)}
                FROM __base b
            """
            source_type = await self._get_column_sql_type(
                table, schema, safe_col,
            ) or self._ewm_float_type()
            return await self._finish_window_special(
                table, schema, column, new_col, feature_sql,
                source_type, backend, data_id, order_by,
                f"Expanding datetime {stat_key} on '{column}' "
                f"(min_periods={min_periods})",
                min_periods=min_periods,
            )
        except Exception as e:
            return self._error_response(
                f"expanding datetime error: {str(e)}\n{traceback.format_exc()}"
            )

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

    # ------------------------------------------------------------------
    # EWM blockwise engine
    # ------------------------------------------------------------------
    @staticmethod
    def _ewm_value(value) -> str:
        return f"CAST({float(value)!r} AS FLOAT8)"

    def _ewm_cast_float(self, expr: str) -> str:
        return f"CAST({expr} AS FLOAT8)"

    def _ewm_float_type(self) -> str:
        return "DOUBLE PRECISION"

    def _ewm_int_type(self) -> str:
        return "BIGINT"

    def _ewm_uses_transaction(self) -> bool:
        return True

    def _is_missing_sql(self, expr: str) -> str:
        return f"({expr} IS NULL OR isnan({self._ewm_cast_float(expr)}))"

    def _ewm_block_size(self, alpha: float) -> int:
        if self.EWM_BLOCK_SIZE:
            return max(1, int(self.EWM_BLOCK_SIZE))
        q = 1.0 - float(alpha)
        if q <= 0.0:
            return 1
        return min(512, max(16, int(690 / (-2 * math.log(q)))))

    async def _ewm_unique_name(self, schema, base, suffix):
        name = SQLIdentifierSanitizer.sanitize(f"{base}__ewm_{suffix}")
        idx = 1
        while await self.db.table_exists(name, schema):
            name = SQLIdentifierSanitizer.sanitize(
                f"{base}__ewm_{suffix}_{idx}"
            )
            idx += 1
        return name

    def _ewm_group_select(self, groups, prefix: str = "") -> str:
        q = self.db.quote_identifier
        return "".join(f"{prefix}{q(c)}, " for c in groups)

    def _ewm_group_cols(self, groups, prefix: str = "") -> str:
        q = self.db.quote_identifier
        return ", ".join(f"{prefix}{q(c)}" for c in groups)

    def _ewm_partition(
        self, groups, prefix: str = "", block_prefix: str = "",
    ) -> str:
        q = self.db.quote_identifier
        cols = [f"{prefix}{q(c)}" for c in groups]
        if block_prefix:
            cols.append(f"{block_prefix}block")
        if not cols:
            return ""
        return f"PARTITION BY {', '.join(cols)} "

    def _ewm_group_join(self, left: str, right: str, groups) -> str:
        q = self.db.quote_identifier
        # ponytail: group keys are assumed non-null (matches existing grouped
        # joins). Switch to IS NOT DISTINCT FROM if null keys matter.
        return "".join(
            f" AND {left}.{q(c)} = {right}.{q(c)}" for c in groups
        )

    def _ewm_ordered_sql(
        self, table, schema, column, order_by, filter_missing, block_size,
        partition_cols=(),
    ) -> str:
        groups = list(partition_cols)
        q_mf = self.db.quote_identifier("_mf_idx")
        q_col = self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(column))
        missing = self._is_missing_sql(q_col)
        partition_sql = self._ewm_partition(groups)
        if order_by:
            orders = self._order_keys(order_by)
            order_sql = ", ".join(self.db.quote_identifier(c) for c in orders)
            rn_expr = (
                f"CAST(ROW_NUMBER() OVER ({partition_sql}ORDER BY {order_sql}) "
                f"AS {self._ewm_int_type()})"
            )
        else:
            rn_expr = (
                f"CAST(ROW_NUMBER() OVER "
                f"({partition_sql}ORDER BY {self.db.quote_identifier('_mf_idx')}) "
                f"AS {self._ewm_int_type()})"
            )
        group_select = self._ewm_group_select(groups)
        where = f"WHERE NOT ({missing})" if filter_missing else ""
        return f"""
            WITH __src AS (
                SELECT {q_mf} AS _mf_idx,
                       {group_select}CASE WHEN {missing} THEN NULL
                            ELSE {self._ewm_cast_float(q_col)} END AS x_clean,
                       {rn_expr} AS rn
                FROM {self._qualified_table(table, schema)}
                {where}
            ),
            __blk AS (
                SELECT _mf_idx, {group_select}x_clean, rn,
                       CAST(FLOOR(
                           {self._ewm_cast_float("rn - 1")} / {int(block_size)}
                       ) AS {self._ewm_int_type()}) AS block
                FROM __src
            )
            SELECT _mf_idx, {group_select}x_clean, rn, block,
                   (rn - 1) - block * {int(block_size)} AS pos
            FROM __blk
        """

    def _ewm_blocks_sql(
        self, ordered: str, q: float, adjust: bool, partition_cols=(),
    ) -> str:
        groups = list(partition_cols)
        group_select = self._ewm_group_select(groups)
        group_cols = self._ewm_group_cols(groups)
        group_by = f"{group_cols}, block" if groups else "block"
        qv = self._ewm_value(q)
        if adjust:
            return f"""
                SELECT {group_select}block, MAX(pos) + 1 AS blen,
                       COUNT(x_clean) AS valid_count,
                       SUM(CASE WHEN x_clean IS NOT NULL
                                THEN POWER({qv}, -pos) ELSE 0 END) AS sum_z1,
                       SUM(x_clean * POWER({qv}, -pos)) AS sum_xz,
                       SUM(x_clean * x_clean * POWER({qv}, -pos)) AS sum_x2z,
                       SUM(CASE WHEN x_clean IS NOT NULL
                                THEN POWER({qv}, -2 * pos) ELSE 0 END) AS sum_z2
                FROM {ordered}
                GROUP BY {group_by}
            """
        vp_partition = f"PARTITION BY {group_cols + ', ' if groups else ''}block "
        return f"""
            WITH vp AS (
                SELECT *,
                       COUNT(x_clean) OVER (
                           {vp_partition}ORDER BY pos
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS vpos
                FROM {ordered}
            ),
            agg AS (
                SELECT {group_select}block, MAX(pos) + 1 AS blen,
                       COUNT(x_clean) AS valid_count,
                       MIN(CASE WHEN x_clean IS NOT NULL THEN pos END) AS first_valid_pos,
                       SUM(CASE WHEN x_clean IS NOT NULL
                                THEN POWER({qv}, -vpos) ELSE 0 END) AS sum_z1_all,
                       SUM(x_clean * POWER({qv}, -vpos)) AS sum_xz_all,
                       SUM(x_clean * x_clean * POWER({qv}, -vpos)) AS sum_x2z_all,
                       SUM(CASE WHEN vpos > 1
                                THEN x_clean * POWER({qv}, -(vpos - 1)) END) AS sum_xz_after,
                       SUM(CASE WHEN vpos > 1
                                THEN x_clean * x_clean * POWER({qv}, -(vpos - 1)) END) AS sum_x2z_after
                FROM vp
                GROUP BY {group_by}
            )
            SELECT a.*, o.x_clean AS x_first
            FROM agg a
            LEFT JOIN {ordered} o
              ON o.block = a.block{self._ewm_group_join('o', 'a', groups)}
              AND o.pos = a.first_valid_pos
        """

    def _ewm_state_sql(
        self, blocks: str, adjust: bool, alpha, q, partition_cols=(),
    ) -> str:
        groups = list(partition_cols)
        fl = self._ewm_cast_float
        qv = self._ewm_value(q)
        a = self._ewm_value(alpha)
        a2 = self._ewm_value(float(alpha) * float(alpha))
        q2 = self._ewm_value(float(q) * float(q))
        zero_f = fl("0")
        zero_i = f"CAST(0 AS {self._ewm_int_type()})"
        group_select = self._ewm_group_select(groups)
        group_select_b = self._ewm_group_select(groups, prefix="b.")
        group_cols = self._ewm_group_cols(groups)
        group_join = self._ewm_group_join("b", "st", groups)
        anchor_group = f"{group_select}" if groups else ""
        anchor_from = (
            f" FROM (SELECT DISTINCT {group_cols} FROM {blocks}) g"
            if groups else ""
        )
        if adjust:
            cols = (
                f"{group_cols}, block, s_num, s_den, s_w2, s_x2w, valid_seen"
                if groups else "block, s_num, s_den, s_w2, s_x2w, valid_seen"
            )
            anchor = (
                f"SELECT {anchor_group}{zero_i}, {zero_f}, {zero_f}, "
                f"{zero_f}, {zero_f}, {zero_i}{anchor_from}"
            )
            recursive = f"""
                SELECT {group_select_b}b.block + 1,
                       POWER({qv}, b.blen) * st.s_num
                       + POWER({qv}, b.blen - 1) * COALESCE(b.sum_xz, 0),
                       POWER({qv}, b.blen) * st.s_den
                       + POWER({qv}, b.blen - 1) * COALESCE(b.sum_z1, 0),
                       POWER({qv}, 2 * b.blen) * st.s_w2
                       + POWER({qv}, 2 * (b.blen - 1)) * COALESCE(b.sum_z2, 0),
                       POWER({qv}, b.blen) * st.s_x2w
                       + POWER({qv}, b.blen - 1) * COALESCE(b.sum_x2z, 0),
                       st.valid_seen + b.valid_count
                FROM st JOIN {blocks} b
                  ON b.block = st.block{group_join}
            """
        else:
            cols = (
                f"{group_cols}, block, state, s_w2, s_x2w, valid_seen, has_state"
                if groups else
                "block, state, s_w2, s_x2w, valid_seen, has_state"
            )
            anchor = (
                f"SELECT {anchor_group}{zero_i}, {zero_f}, {zero_f}, "
                f"{zero_f}, {zero_i}, {zero_i}{anchor_from}"
            )
            recursive = f"""
                SELECT {group_select_b}b.block + 1,
                       CASE
                           WHEN b.valid_count = 0 THEN st.state
                           WHEN st.has_state = 1 THEN
                               POWER({qv}, b.valid_count) * st.state
                               + {a} * POWER({qv}, b.valid_count)
                               * COALESCE(b.sum_xz_all, 0)
                           ELSE
                               POWER({qv}, b.valid_count - 1) * b.x_first
                               + {a} * POWER({qv}, b.valid_count - 1)
                               * COALESCE(b.sum_xz_after, 0)
                       END,
                       CASE
                           WHEN b.valid_count = 0 THEN st.s_w2
                           WHEN st.has_state = 1 THEN
                               POWER({qv}, 2 * b.valid_count) * st.s_w2
                               + {a2} * (1 - POWER({qv}, 2 * b.valid_count))
                               / (1 - {q2})
                           ELSE
                               POWER({qv}, 2 * (b.valid_count - 1))
                               + {a2} * (1 - POWER({qv}, 2 * (b.valid_count - 1)))
                               / (1 - {q2})
                       END,
                       CASE
                           WHEN b.valid_count = 0 THEN st.s_x2w
                           WHEN st.has_state = 1 THEN
                               POWER({qv}, b.valid_count) * st.s_x2w
                               + {a} * POWER({qv}, b.valid_count)
                               * COALESCE(b.sum_x2z_all, 0)
                           ELSE
                               POWER({qv}, b.valid_count - 1)
                               * b.x_first * b.x_first
                               + {a} * POWER({qv}, b.valid_count - 1)
                               * COALESCE(b.sum_x2z_after, 0)
                       END,
                       st.valid_seen + b.valid_count,
                       CASE WHEN b.valid_count > 0 THEN 1 ELSE st.has_state END
                FROM st JOIN {blocks} b
                  ON b.block = st.block{group_join}
            """
        return (
            f"WITH RECURSIVE st({cols}) AS ("
            f"{anchor} UNION ALL {recursive}) "
            f"SELECT * FROM st"
        )

    def _ewm_create_state(self, qualified: str, select_sql: str) -> str:
        return f"CREATE TABLE {qualified} AS {select_sql}"

    def _ewm_agg_select(
        self, aggs, new_cols, min_periods, adjust, alpha,
    ) -> str:
        mp = int(min_periods)
        out = []
        q = self.db.quote_identifier
        for agg, col in zip(aggs, new_cols):
            if alpha >= 1:
                gate = f"CASE WHEN x_clean IS NOT NULL AND valid_1 >= {mp} THEN x_clean END"
                expr = gate if agg in ("mean", "sum") else (
                    f"CAST(NULL AS {self._ewm_float_type()})"
                )
            elif adjust:
                if agg == "mean":
                    expr = (
                        f"CASE WHEN x_clean IS NOT NULL AND valid_t >= {mp} "
                        f"AND s_den_t > 0 THEN s_num_t / s_den_t END"
                    )
                elif agg == "sum":
                    expr = (
                        f"CASE WHEN x_clean IS NOT NULL AND valid_t >= {mp} "
                        f"AND s_den_t > 0 THEN s_num_t END"
                    )
                else:
                    var_expr = (
                        f"CASE WHEN x_clean IS NOT NULL AND valid_t >= GREATEST({mp}, 2) "
                        f"AND s_den_t > 0 AND (s_den_t - s_w2_t / s_den_t) > 0 "
                        f"THEN GREATEST((s_x2w_t / s_den_t "
                        f"- (s_num_t / s_den_t) * (s_num_t / s_den_t)) "
                        f"* (s_den_t / (s_den_t - s_w2_t / s_den_t)), 0) END"
                    )
                    expr = var_expr if agg == "var" else f"SQRT({var_expr})"
            else:
                if agg in ("mean", "sum"):
                    expr = (
                        f"CASE WHEN x_clean IS NOT NULL AND valid_t >= {mp} "
                        f"THEN state_t END"
                    )
                else:
                    var_expr = (
                        f"CASE WHEN x_clean IS NOT NULL AND valid_t >= GREATEST({mp}, 2) "
                        f"AND (1 - w2_t) > 0 "
                        f"THEN GREATEST((x2w_t - state_t * state_t) "
                        f"/ (1 - w2_t), 0) END"
                    )
                    expr = var_expr if agg == "var" else f"SQRT({var_expr})"
            out.append(f"{expr} AS {q(col)}")
        return ", ".join(out)

    def _ewm_feature_sql(
        self, ordered, state, blocks, aggs, new_cols,
        min_periods, adjust, alpha, q, partition_cols=(),
    ) -> str:
        groups = list(partition_cols)
        qv = self._ewm_value(q)
        frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        part = self._ewm_partition(groups, prefix="o.", block_prefix="o.")
        group_cols = self._ewm_group_cols(groups)
        vp_partition = f"PARTITION BY {group_cols + ', ' if groups else ''}block "
        state_join = "o.block = st.block" + self._ewm_group_join("o", "st", groups)
        blocks_join = "o.block = b.block" + self._ewm_group_join("o", "b", groups)
        if alpha >= 1:
            valid_partition = self._ewm_partition(groups)
            valid_1 = (
                f"COUNT(x_clean) OVER ({valid_partition}ORDER BY rn {frame})"
            )
            select = self._ewm_agg_select(aggs, new_cols, min_periods, adjust, alpha)
            return (
                f"SELECT _mf_idx, {select} FROM ("
                f"SELECT *, {valid_1} AS valid_1 FROM {ordered}) __a"
            )
        if adjust:
            calc = f"""
                SELECT o._mf_idx, o.x_clean, o.pos,
                       st.valid_seen + COUNT(o.x_clean) OVER (
                           {part}ORDER BY o.pos {frame}
                       ) AS valid_t,
                       POWER({qv}, o.pos + 1) * st.s_num
                       + POWER({qv}, o.pos) * SUM(
                           o.x_clean * POWER({qv}, -o.pos)
                       ) OVER ({part}ORDER BY o.pos {frame}) AS s_num_t,
                       POWER({qv}, o.pos + 1) * st.s_den
                       + POWER({qv}, o.pos) * SUM(
                           CASE WHEN o.x_clean IS NOT NULL
                                THEN POWER({qv}, -o.pos) ELSE 0 END
                       ) OVER ({part}ORDER BY o.pos {frame}) AS s_den_t,
                       POWER({qv}, 2 * (o.pos + 1)) * st.s_w2
                       + POWER({qv}, 2 * o.pos) * SUM(
                           CASE WHEN o.x_clean IS NOT NULL
                                THEN POWER({qv}, -2 * o.pos) ELSE 0 END
                       ) OVER ({part}ORDER BY o.pos {frame}) AS s_w2_t,
                       POWER({qv}, o.pos + 1) * st.s_x2w
                       + POWER({qv}, o.pos) * SUM(
                           o.x_clean * o.x_clean * POWER({qv}, -o.pos)
                       ) OVER ({part}ORDER BY o.pos {frame}) AS s_x2w_t
                FROM {ordered} o
                JOIN {state} st ON {state_join}
            """
            select = self._ewm_agg_select(aggs, new_cols, min_periods, adjust, alpha)
            return f"WITH calc AS ({calc}) SELECT _mf_idx, {select} FROM calc"
        calc = f"""
            WITH vp AS (
                SELECT *,
                       COUNT(x_clean) OVER (
                           {vp_partition}ORDER BY pos {frame}
                       ) AS v
                FROM {ordered}
            ),
            local AS (
                SELECT o.*, st.state, st.s_w2, st.s_x2w,
                       st.valid_seen AS valid_in, st.has_state, b.x_first,
                       SUM(o.x_clean * POWER({qv}, -o.v)) OVER (
                           {part}ORDER BY o.pos {frame}
                       ) AS local_x_all,
                       SUM(o.x_clean * o.x_clean * POWER({qv}, -o.v)) OVER (
                           {part}ORDER BY o.pos {frame}
                       ) AS local_x2_all,
                       SUM(CASE WHEN o.v > 1
                                THEN o.x_clean * POWER({qv}, -(o.v - 1)) END) OVER (
                           {part}ORDER BY o.pos {frame}
                       ) AS local_x_after,
                       SUM(CASE WHEN o.v > 1
                                THEN o.x_clean * o.x_clean * POWER({qv}, -(o.v - 1)) END) OVER (
                           {part}ORDER BY o.pos {frame}
                       ) AS local_x2_after
                FROM vp o
                JOIN {state} st ON {state_join}
                JOIN {blocks} b ON {blocks_join}
            ),
            calc AS (
                SELECT *,
                       CASE
                           WHEN x_clean IS NULL THEN NULL
                           WHEN has_state = 1 THEN
                               POWER({qv}, v) * state
                               + {self._ewm_value(alpha)} * POWER({qv}, v)
                               * COALESCE(local_x_all, 0)
                           WHEN v = 1 THEN x_clean
                           WHEN v > 1 THEN
                               POWER({qv}, v - 1) * x_first
                               + {self._ewm_value(alpha)} * POWER({qv}, v - 1)
                               * COALESCE(local_x_after, 0)
                       END AS state_t,
                       CASE
                           WHEN has_state = 1 THEN
                               POWER({qv}, 2 * v) * s_w2
                               + {self._ewm_value(alpha * alpha)}
                               * (1 - POWER({qv}, 2 * v)) / (1 - {self._ewm_value(q * q)})
                           WHEN v >= 1 THEN
                               POWER({qv}, 2 * (v - 1))
                               + {self._ewm_value(alpha * alpha)}
                               * (1 - POWER({qv}, 2 * (v - 1))) / (1 - {self._ewm_value(q * q)})
                       END AS w2_t,
                       CASE
                           WHEN x_clean IS NULL THEN NULL
                           WHEN has_state = 1 THEN
                               POWER({qv}, v) * s_x2w
                               + {self._ewm_value(alpha)} * POWER({qv}, v)
                               * COALESCE(local_x2_all, 0)
                           WHEN v = 1 THEN x_clean * x_clean
                           WHEN v > 1 THEN
                               POWER({qv}, v - 1) * x_first * x_first
                               + {self._ewm_value(alpha)} * POWER({qv}, v - 1)
                               * COALESCE(local_x2_after, 0)
                       END AS x2w_t,
                       CASE
                           WHEN has_state = 1 THEN valid_in + v
                           WHEN v > 0 THEN v
                           ELSE 0
                       END AS valid_t
                FROM local
            )
        """
        select = self._ewm_agg_select(aggs, new_cols, min_periods, adjust, alpha)
        return f"{calc} SELECT _mf_idx, {select} FROM calc"
    def _ewm_all_rows_sql(
        self, table, schema, feature_sql: str, columns,
    ) -> str:
        """Left-join a filtered feature result back onto every source row."""
        q = self.db.quote_identifier
        mf = q("_mf_idx")
        cols = ", ".join(f"f.{q(c)}" for c in columns)
        return (
            f"SELECT s.{mf}, {cols} "
            f"FROM {self._qualified_table(table, schema)} s "
            f"LEFT JOIN ({feature_sql}) f ON s.{mf} = f.{mf}"
        )

    async def _ewm_run(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols=(),
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
        op_type: str = "window_map",
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            safe_col = SQLIdentifierSanitizer.sanitize(column)
            groups = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
            if isinstance(agg, str):
                aggs = [agg]
            else:
                aggs = list(agg)
            for a in aggs:
                if a not in ("mean", "sum", "std", "var"):
                    return self._error_response(f"Unsupported ewm aggregation '{a}'")

            a = float(self._alpha_from_params(com, span, halflife, alpha))
            q = 1.0 - a
            suffix = f"_by_{'_'.join(groups)}" if groups else ""
            new_cols = [f"{safe_col}_ewm_{f}{suffix}" for f in aggs]
            block_size = self._ewm_block_size(a)
            filter_missing = bool(ignore_na) and (bool(adjust) or a >= 1.0)

            base = await self._resolve_output_table_name(
                table, schema, backend=backend, data_id=data_id,
            )
            ordered = await self._ewm_unique_name(schema, base, "ordered")
            blocks = await self._ewm_unique_name(schema, base, "blocks")
            state_t = await self._ewm_unique_name(schema, base, "state")
            q_ordered = self._qualified_table(ordered, schema)
            q_blocks = self._qualified_table(blocks, schema)
            q_state = self._qualified_table(state_t, schema)

            tx_factory = getattr(getattr(self.db, "_pool", None), "transaction", None)
            tx = (
                tx_factory()
                if self._ewm_uses_transaction() and tx_factory is not None
                else nullcontext()
            )
            try:
                async with tx:
                    await self._exec(
                        f"CREATE TABLE {q_ordered} AS "
                        f"{self._ewm_ordered_sql(table, schema, column, order_by, filter_missing, block_size, groups)}"
                    )

                    if a < 1.0:
                        await self._exec(
                            f"CREATE TABLE {q_blocks} AS "
                            f"{self._ewm_blocks_sql(q_ordered, q, bool(adjust), groups)}"
                        )
                        await self._exec(
                            self._ewm_create_state(
                                q_state,
                                self._ewm_state_sql(
                                    q_blocks, bool(adjust), a, q, groups,
                                ),
                            )
                        )

                    feature_sql = self._ewm_feature_sql(
                        q_ordered, q_state, q_blocks, aggs, new_cols,
                        min_periods, bool(adjust), a, q, groups,
                    )
                    if filter_missing:
                        feature_sql = self._ewm_all_rows_sql(
                            table, schema, feature_sql, new_cols,
                        )
                    collision = await assert_owned_columns(
                        self, table, schema, new_cols, op_type,
                        backend, data_id,
                    )
                    if collision:
                        return self._error_response(collision)

                    await self._merge_features(
                        table, schema, feature_sql, new_cols,
                        [self._ewm_float_type()] * len(new_cols),
                        backend=backend, data_id=data_id,
                    )

                    preview_cols = [*new_cols, column]
                    if order_by:
                        preview_cols += [
                            k for k in self._order_keys(order_by)
                            if k != "_mf_idx"
                        ]
                    preview_cols += groups
                    preview_cols = list(dict.fromkeys(preview_cols))
                    res_df = await self._preview_data(table, schema, preview_cols)
                    message = f"EWM {aggs} on '{column}'"
                    if groups:
                        message += f" grouped by {groups}"
                    response = self._success_response(
                        message,
                        res_df,
                        new_table=table,
                        new_columns=new_cols,
                        source_updated=True,
                    )
                    if groups:
                        response["group_cols"] = groups
                    return response
            finally:
                for name in (state_t, blocks, ordered):
                    try:
                        await self._exec(
                            f"DROP TABLE IF EXISTS {self._qualified_table(name, schema)}"
                        )
                    except Exception:
                        pass
        except Exception as e:
            return self._error_response(
                f"ewm error: {str(e)}\n{traceback.format_exc()}"
            )

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
        return await self._ewm_run(
            table, schema, column, (),
            order_by=order_by, com=com, span=span, halflife=halflife,
            alpha=alpha, adjust=adjust, ignore_na=ignore_na,
            min_periods=min_periods, agg=agg,
            backend=backend, data_id=data_id, op_type="window_map",
        )
