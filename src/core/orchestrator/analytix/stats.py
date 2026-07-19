from typing import Any, Dict, List
import numpy as np
import pandas as pd
from core.ingestion.datatype_detector import DatatypeDetector
from core.analytix.stats import DataStatsOps
from utils.method_call_logger import record_call


class StatsOrchestrator:
    """Orchestrator for statistical operations. Supports method‑call recording and replay."""

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._stats_ops = None
        self._dtype_detector = DatatypeDetector()


    async def _ensure_ops(self) -> DataStatsOps:
        if self._stats_ops is None:
            await self._ops_parent._ensure_adapter()
            self._stats_ops = DataStatsOps(self._ops_parent._adapter)
        return self._stats_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    async def _detect_stats_dtype(self, ops: DataStatsOps, table: str, schema: str, column: str) -> str:
        """
        Detect column family for stats routing.
        Returns one of: 'numeric', 'categorical', 'datetime'.
        """
        sample_df = await ops._fetch_data(table, schema, columns=[column],limit = 10)
        if sample_df.empty:
            return "categorical"
        
        series = sample_df[column]
        series = series.replace('', np.nan)
        import pyarrow as pa
        chunked = pa.chunked_array([pa.array(series)])
        
        inferred = self._dtype_detector._infer_column(chunked_array=chunked)
        # print("Inferred : ", inferred)
        detected = str(inferred.get("type", "text")).lower()

        if detected in ("integer", "float"):
            return "numeric"
        if detected == "datetime":
            return "datetime"
        return "categorical"
    
    
    
    # ── pandas-like unified APIs with dtype auto-routing ──────────────
    async def count(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_count(table, schema, column)
        if detected_dtype == "categorical":
            return await ops.categorical_count(table, schema, column)
        return await ops.numeric_count(table, schema, column)

    async def min(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_min(table, schema, column)
        return await ops.numeric_min(table, schema, column)

    async def max(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_max(table, schema, column)
        return await ops.numeric_max(table, schema, column)

    async def mode(self, column: str, top_n: int = 1) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_mode(table, schema, column, top_n)
        return await ops.numeric_mode(table, schema, column, top_n)

    async def unique(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_unique(table, schema, column)
        return await ops.numeric_unique(table, schema, column)

    async def nunique(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_nunique(table, schema, column)
        if detected_dtype == "categorical":
            return await ops.categorical_nunique(table, schema, column)
        return await ops.numeric_nunique(table, schema, column)

    async def value_counts(self, column: str, top_n: int = 10) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "categorical":
            return await ops.categorical_value_counts(table, schema, column, top_n)
        return await ops.numeric_value_counts(table, schema, column, top_n)

    async def mean(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_mean(table, schema, column)
        return await ops.numeric_mean(table, schema, column)

    async def median(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        detected_dtype = await self._detect_stats_dtype(ops, table, schema, column)

        if detected_dtype == "datetime":
            return await ops.datetime_median(table, schema, column)
        return await ops.numeric_median(table, schema, column)

    # ── numeric-focused methods ───────────────────────────────────────
    async def sum(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_sum(table, schema, column)

    async def std(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_std(table, schema, column)

    async def var(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_var(table, schema, column)

    async def sem(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_sem(table, schema, column)

    async def mad(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_mad(table, schema, column)

    async def iqr(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_iqr(table, schema, column)

    async def range(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_range(table, schema, column)

    async def skew(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_skew(table, schema, column)

    async def kurtosis(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_kurtosis(table, schema, column)

    async def entropy(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_entropy(table, schema, column)

    async def quantile(self, column: str, q: List[float] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_quantile(table, schema, column, quantiles=q)

    async def autocorr(self, column: str, lag: int = 1) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_autocorr(table, schema, column, lag)

    async def coefficient_of_variation(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_coefficient_of_variation(table, schema, column)

    async def outliers_iqr(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_outliers_iqr(table, schema, column)

    async def outliers_zscore(self, column: str, threshold: float = 3.0) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_outliers_zscore(table, schema, column, threshold)

    # ── Methods that **return DataFrames** → table creation + call recording ──
    @record_call
    async def corr(self, columns: List[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id

        candidate_columns = columns
        if columns is None:
            col_types = await ops.db.get_column_types(table, schema)
            candidate_columns = list(col_types.keys())

        numeric_cols = []
        for col in candidate_columns or []:
            detected_dtype = await self._detect_stats_dtype(ops, table, schema, col)
            if detected_dtype == "numeric":
                numeric_cols.append(col)

        # if not numeric_cols or len(numeric_cols) < 2:
        #     return ops._error_response("Need at least 2 numeric columns for correlation")

        return await ops.numeric_multi_column_correlation(
            table, schema, numeric_cols,
            backend=backend, data_id=data_id
        )

    @record_call
    async def cov(self, columns: List[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id

        candidate_columns = columns
        if columns is None:
            col_types = await ops.db.get_column_types(table, schema)
            candidate_columns = list(col_types.keys())

        numeric_cols = []
        for col in candidate_columns or []:
            detected_dtype = await self._detect_stats_dtype(ops, table, schema, col)
            if detected_dtype == "numeric":
                numeric_cols.append(col)

        # if not numeric_cols or len(numeric_cols) < 2:
            # return ops._error_response("Need at least 2 numeric columns for covariance")

        return await ops.numeric_multi_column_covariance(
            table, schema, numeric_cols,
            backend=backend, data_id=data_id
        )

    # ── categorical-specific methods ───────────────────────────────────
    async def proportions(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_proportions(table, schema, column)

    async def chi_square(self, column1: str, column2: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_chi_square(table, schema, column1, column2)

    async def cramers_v(self, column1: str, column2: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_cramers_v(table, schema, column1, column2)

    async def theil_u(self, column1: str, column2: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_theil_u(table, schema, column1, column2)

    async def mutual_information(self, column1: str, column2: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_mutual_information(table, schema, column1, column2)


    # ── datetime-specific methods ──────────────────────────────────────
    async def datetime_diff(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_diff(table, schema, column)

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