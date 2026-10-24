from typing import Any, Dict, List, Optional
from memframe.core.analytix.stats import DataStatsOps, make_stats_ops
from memframe.core.analytix._response import fail, ok
from memframe.cache import record_call
from memframe.utils.helper import SQLIdentifierSanitizer


def _coerce_num(value):
    # ponytail: user numbers reach SQL by interpolation downstream; coerce
    # here (accepts numeric strings like "3.0") so non-numerics fail cleanly
    if value is None or isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"expected a number, got {value!r}")


def _coerce_int(value):
    if isinstance(value, bool):
        raise ValueError(f"expected an integer, got {value!r}")
    if isinstance(value, int):
        return value
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"expected an integer, got {value!r}")
    if not as_float.is_integer():
        raise ValueError(f"expected an integer, got {value!r}")
    return int(as_float)


# Families a whole-table stat runs on. Eligibility is decided from the DB
# column types before any stat query runs, so an op never sees a column it
# cannot handle. "categorical" is the catch-all (text, bool, uuid, json, …).
_WHOLE_TABLE_FAMILIES: Dict[str, frozenset] = {
    "count": frozenset(("numeric", "categorical", "datetime")),
    "min": frozenset(("numeric", "datetime")),
    "max": frozenset(("numeric", "datetime")),
    "mode": frozenset(("categorical", "numeric")),
    "unique": frozenset(("categorical", "numeric")),
    "nunique": frozenset(("numeric", "categorical", "datetime")),
    "value_counts": frozenset(("numeric", "categorical", "datetime")),
    "mean": frozenset(("numeric", "datetime")),
    "median": frozenset(("numeric", "datetime")),
    "sum": frozenset(("numeric",)),
    "std": frozenset(("numeric",)),
    "var": frozenset(("numeric",)),
    "sem": frozenset(("numeric",)),
    "mad": frozenset(("numeric",)),
    "iqr": frozenset(("numeric",)),
    "range": frozenset(("numeric",)),
    "skew": frozenset(("numeric",)),
    "kurtosis": frozenset(("numeric",)),
    "entropy": frozenset(("numeric",)),
    "quantile": frozenset(("numeric",)),
    "autocorr": frozenset(("numeric",)),
    "coefficient_of_variation": frozenset(("numeric",)),
    "outliers_iqr": frozenset(("numeric",)),
    "outliers_zscore": frozenset(("numeric",)),
    "proportions": frozenset(("categorical",)),
}

# SQL type prefixes per family. Anything else (text, bool, uuid, json, …) is
# categorical, so numeric-only stats never attempt an operation on it.
_DATETIME_TYPE_PREFIXES = ("timestamp", "datetime", "date", "time")
_NUMERIC_TYPE_PREFIXES = (
    "int", "uint", "bigint", "smallint", "tinyint", "mediumint", "hugeint",
    "float", "double", "real", "decimal", "numeric", "money",
    "serial", "bigserial", "smallserial", "number",
)


class StatsOrchestrator:
    """Orchestrator for statistical operations. Supports method‑call recording and replay."""

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._stats_ops = None


    async def _ensure_ops(self) -> DataStatsOps:
        if self._stats_ops is None:
            await self._ops_parent._ensure_adapter()
            self._stats_ops = make_stats_ops(self._ops_parent._adapter)
        return self._stats_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    @staticmethod
    def _unwrap_sql_type(sql_type: Any) -> str:
        """Strip ClickHouse wrappers such as Nullable(...) / LowCardinality(...)."""
        text = str(sql_type or "").strip()
        while text.endswith(")"):
            lowered = text.lower()
            for prefix in ("nullable(", "lowcardinality("):
                if lowered.startswith(prefix):
                    text = text[len(prefix):-1].strip()
                    break
            else:
                break
        return text

    @classmethod
    def _classify_sql_type(cls, sql_type: Any) -> Optional[str]:
        """Return the stats family for a DB type."""
        base = cls._unwrap_sql_type(sql_type).split("(", 1)[0].strip().lower()
        if not base:
            return None
        if base == "interval" or base in ("bool", "boolean"):
            return "categorical"
        if base.startswith(_DATETIME_TYPE_PREFIXES):
            return "datetime"
        if base.startswith(_NUMERIC_TYPE_PREFIXES):
            return "numeric"
        return "categorical"

    async def _detect_stats_dtype(self, ops: DataStatsOps, table: str, schema: str, column: str) -> str:
        """
        Detect column family for stats routing.
        Returns one of: 'numeric', 'categorical', 'datetime'.
        """
        SQLIdentifierSanitizer.sanitize(column)  # fail closed on empty identifiers
        col_types = await ops.db.get_column_types(table, schema)
        return self._classify_sql_type(col_types.get(column)) or "categorical"
    
    
    
    # ── pandas-like unified APIs with dtype auto-routing ──────────────
    async def _whole_table(
        self,
        ops: DataStatsOps,
        table: str,
        schema: str,
        op_name: str,
        call_single,
    ) -> Dict[str, Any]:
        """Fan a single-column stat across its compatible columns.

        Families are resolved from the DB column types before any stat runs,
        so a column that would fail is never attempted. `call_single(col,
        family)` must behave exactly like the single-column call.
        Inapplicable columns are skipped (pandas `numeric_only` spirit); zero
        compatible columns is a clear failure, not an empty dict.
        """
        families = _WHOLE_TABLE_FAMILIES[op_name]
        col_types = await ops.db.get_column_types(table, schema)
        columns = list(col_types.keys())
        detected = await self._detect_column_families(
            ops, table, schema, columns, col_types=col_types
        )
        targets = [c for c in columns if detected.get(c) in families]
        if not targets:
            return fail(f"{op_name} found no compatible columns", columns, [])
        combined: Dict[str, Any] = {}
        for col in targets:
            resp = await call_single(col, detected.get(col))
            if resp.get("is_error"):
                return resp
            combined[col] = resp.get("result")
        return ok(
            f"{op_name} for all {len(targets)} compatible columns",
            targets,
            result=combined,
        )

    async def count(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "count",
                lambda col, family: self._count_one(ops, table, schema, col, family),
            )
        return await self._count_one(ops, table, schema, column)

    async def _count_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_count(table, schema, column)
        if detected_dtype == "categorical":
            return await ops.categorical_count(table, schema, column)
        return await ops.numeric_count(table, schema, column)

    async def min(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "min",
                lambda col, family: self._min_one(ops, table, schema, col, family),
            )
        return await self._min_one(ops, table, schema, column)

    async def _min_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_min(table, schema, column)
        return await ops.numeric_min(table, schema, column)

    async def max(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "max",
                lambda col, family: self._max_one(ops, table, schema, col, family),
            )
        return await self._max_one(ops, table, schema, column)

    async def _max_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_max(table, schema, column)
        return await ops.numeric_max(table, schema, column)

    async def mode(self, column: Optional[str] = None, top_n: int = 1) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        try:
            top_n = _coerce_int(top_n)
        except ValueError as exc:
            return fail(f"top_n must be an integer: {exc}")

        if column is None:
            return await self._whole_table(
                ops, table, schema, "mode",
                lambda col, family: self._mode_one(ops, table, schema, col, top_n, family),
            )
        return await self._mode_one(ops, table, schema, column, top_n)

    async def _mode_one(self, ops, table, schema, column, top_n, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_mode(table, schema, column, top_n)
        return await ops.numeric_mode(table, schema, column, top_n)

    async def unique(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "unique",
                lambda col, family: self._unique_one(ops, table, schema, col, family),
            )
        return await self._unique_one(ops, table, schema, column)

    async def _unique_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_unique(table, schema, column)
        return await ops.numeric_unique(table, schema, column)

    async def nunique(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "nunique",
                lambda col, family: self._nunique_one(ops, table, schema, col, family),
            )
        return await self._nunique_one(ops, table, schema, column)

    async def _nunique_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_nunique(table, schema, column)
        if detected_dtype == "categorical":
            return await ops.categorical_nunique(table, schema, column)
        return await ops.numeric_nunique(table, schema, column)

    async def value_counts(self, column: Optional[str] = None, top_n: int = 10) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        try:
            top_n = _coerce_int(top_n)
        except ValueError as exc:
            return fail(f"top_n must be an integer: {exc}")

        if column is not None:
            detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

            if detected_dtype == "categorical":
                return await ops.categorical_value_counts(table, schema, column, top_n)
            return await ops.numeric_value_counts(table, schema, column, top_n)

        # ponytail: no column → per-column counts for the whole table, reusing
        # the single-column routing above; first column error short-circuits.
        return await self._whole_table(
            ops, table, schema, "value_counts",
            lambda col, family: self._value_counts_one(ops, table, schema, col, top_n, family),
        )

    async def _value_counts_one(self, ops, table, schema, column, top_n, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_value_counts(table, schema, column, top_n)
        return await ops.numeric_value_counts(table, schema, column, top_n)

    async def mean(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "mean",
                lambda col, family: self._mean_one(ops, table, schema, col, family),
            )
        return await self._mean_one(ops, table, schema, column)

    async def _mean_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return fail(f"mean needs a numeric or datetime column; '{column}' is categorical", [column])
        if detected_dtype == "datetime":
            return await ops.datetime_mean(table, schema, column)
        return await ops.numeric_mean(table, schema, column)

    async def median(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "median",
                lambda col, family: self._median_one(ops, table, schema, col, family),
            )
        return await self._median_one(ops, table, schema, column)

    async def _median_one(self, ops, table, schema, column, family=None) -> Dict[str, Any]:
        detected_dtype = family or await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return fail(f"median needs a numeric or datetime column; '{column}' is categorical", [column])
        if detected_dtype == "datetime":
            return await ops.datetime_median(table, schema, column)
        return await ops.numeric_median(table, schema, column)

    # ── numeric-focused methods ───────────────────────────────────────
    # ponytail: column=None fans out over numeric columns only (pandas
    # numeric_only spirit); each branch below is one line via _numeric_whole.
    async def _numeric_whole(self, ops, table, schema, op_name, call_single) -> Dict[str, Any]:
        async def one(col, _family):
            return await call_single(col)

        return await self._whole_table(ops, table, schema, op_name, one)

    async def sum(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "sum",
                lambda col: ops.numeric_sum(table, schema, col))
        return await ops.numeric_sum(table, schema, column)

    async def std(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "std",
                lambda col: ops.numeric_std(table, schema, col))
        return await ops.numeric_std(table, schema, column)

    async def var(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "var",
                lambda col: ops.numeric_var(table, schema, col))
        return await ops.numeric_var(table, schema, column)

    async def sem(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "sem",
                lambda col: ops.numeric_sem(table, schema, col))
        return await ops.numeric_sem(table, schema, column)

    async def mad(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "mad",
                lambda col: ops.numeric_mad(table, schema, col))
        return await ops.numeric_mad(table, schema, column)

    async def iqr(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "iqr",
                lambda col: ops.numeric_iqr(table, schema, col))
        return await ops.numeric_iqr(table, schema, column)

    async def range(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "range",
                lambda col: ops.numeric_range(table, schema, col))
        return await ops.numeric_range(table, schema, column)

    async def skew(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "skew",
                lambda col: ops.numeric_skew(table, schema, col))
        return await ops.numeric_skew(table, schema, column)

    async def kurtosis(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "kurtosis",
                lambda col: ops.numeric_kurtosis(table, schema, col))
        return await ops.numeric_kurtosis(table, schema, column)

    async def entropy(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "entropy",
                lambda col: ops.numeric_entropy(table, schema, col))
        return await ops.numeric_entropy(table, schema, column)

    async def quantile(self, column: Optional[str] = None, q: List[float] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if q is not None:
            try:
                q = [_coerce_num(x) for x in q]
            except (ValueError, TypeError) as exc:
                return fail(f"quantiles must be numbers: {exc}")
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "quantile",
                lambda col: ops.numeric_quantile(table, schema, col, quantiles=q))
        return await ops.numeric_quantile(table, schema, column, quantiles=q)

    async def autocorr(self, column: Optional[str] = None, lag: int = 1) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        try:
            lag = _coerce_int(lag)
        except ValueError as exc:
            return fail(f"lag must be an integer: {exc}")
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "autocorr",
                lambda col: ops.numeric_autocorr(table, schema, col, lag))
        return await ops.numeric_autocorr(table, schema, column, lag)

    async def coefficient_of_variation(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "coefficient_of_variation",
                lambda col: ops.numeric_coefficient_of_variation(table, schema, col))
        return await ops.numeric_coefficient_of_variation(table, schema, column)

    async def outliers_iqr(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "outliers_iqr",
                lambda col: ops.numeric_outliers_iqr(table, schema, col))
        return await ops.numeric_outliers_iqr(table, schema, column)

    @record_call(deep_cache=True)
    async def outliers_zscore(self, column: Optional[str] = None, threshold: float = 3.0) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        persist = {"backend": self._memframe._backend, "data_id": self._data_id}
        if column is None:
            return await self._numeric_whole(
                ops, table, schema, "outliers_zscore",
                lambda col: ops.numeric_outliers_zscore(table, schema, col, threshold, **persist))
        return await ops.numeric_outliers_zscore(
            table, schema, column, threshold,
            backend=self._memframe._backend, data_id=self._data_id,
        )

    # ── Methods that **return DataFrames** → table creation + call recording ──
    async def _detect_column_families(self, ops, table, schema, candidate_columns, col_types=None) -> Dict[str, str]:
        """Classify every candidate column from the DB types in one query.

        Returns {column: 'numeric'|'categorical'|'datetime'}.
        """
        if not candidate_columns:
            return {}
        if col_types is None:
            col_types = await ops.db.get_column_types(table, schema)
        return {
            col: self._classify_sql_type(col_types.get(col)) or "categorical"
            for col in candidate_columns
        }

    async def _detect_numeric_columns(self, ops, table, schema, candidate_columns):
        """One 5000-row sample query for all candidate columns (ponytail: was N
        per-column queries). Dtype inferred per column from that single sample."""
        families = await self._detect_column_families(ops, table, schema, candidate_columns)
        return [col for col, family in families.items() if family == "numeric"]

    @record_call(deep_cache=True)
    async def corr(self, columns: List[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id

        candidate_columns = columns
        if columns is None:
            col_types = await ops.db.get_column_types(table, schema)
            candidate_columns = list(col_types.keys())

        numeric_cols = await self._detect_numeric_columns(ops, table, schema, candidate_columns or [])

        return await ops.numeric_multi_column_correlation(
            table, schema, numeric_cols,
            backend=backend, data_id=data_id
        )

    @record_call(deep_cache=True)
    async def cov(self, columns: List[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id

        candidate_columns = columns
        if columns is None:
            col_types = await ops.db.get_column_types(table, schema)
            candidate_columns = list(col_types.keys())

        numeric_cols = await self._detect_numeric_columns(ops, table, schema, candidate_columns or [])

        return await ops.numeric_multi_column_covariance(
            table, schema, numeric_cols,
            backend=backend, data_id=data_id
        )

    # ── categorical-specific methods ───────────────────────────────────
    async def proportions(self, column: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if column is None:
            return await self._whole_table(
                ops, table, schema, "proportions",
                lambda col, _family: ops.categorical_proportions(table, schema, col))
        return await ops.categorical_proportions(table, schema, column)


    # ── datetime-specific methods ──────────────────────────────────────
    @record_call(deep_cache=True)
    async def datetime_diff(self, column: str, target_col: str = None, new_table: str = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_diff(
            table, schema, column,
            backend=self._memframe._backend, data_id=self._data_id,
            new_table=new_table, target_col=target_col,
        )

    async def time_delta_stats(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_delta_stats(table, schema, column)

    async def event_rate(self, column: str, unit: str = "day") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_event_rate(table, schema, column, unit)

    async def time_unit_counts(self, column: str, unit: str = "day") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_time_unit_counts(table, schema, column, unit)

    async def weekday_weekend_counts(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_weekday_weekend_counts(table, schema, column)

    async def holiday_counts(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_holiday_counts(table, schema, column)


# Alias for convenience
StatsAccessor = StatsOrchestrator