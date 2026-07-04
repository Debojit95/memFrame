from typing import Any, Dict, List, Optional, Union

import numpy as np

from src.core.ingestion.datatype_detector import DatatypeDetector
from src.core.analytix.cleaning import DataCleaningOps
from src.utils.method_call_logger import record_call


class CleaningOrchestrator:
    """
    User‑facing cleaning methods.
    Accessed via `ops.clean`.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe   # MemFrame
        self._data_id = memframe_ops_instance._data_id
        self._cleaning_ops = None
        self._dtype_detector = DatatypeDetector()


    async def _ensure_ops(self) -> DataCleaningOps:
        if self._cleaning_ops is None:
            await self._ops_parent._ensure_adapter()
            self._cleaning_ops = DataCleaningOps(self._ops_parent._adapter)
        return self._cleaning_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    def _persistence_context(self) -> Dict[str, Any]:
        backend = self._ops_parent.memframe._backend
        data_id = self._ops_parent._data_id or self._ops_parent.memframe._active_id
        return {"backend": backend, "data_id": data_id}

    async def _detect_fillna_dtype(self, ops: DataCleaningOps, table: str, schema: str, column: str) -> str:
        sample_df = await ops._fetch_data(table, schema, columns=[column],limit=10)
        if sample_df.empty:
            return "categorical"
        
        series = sample_df[column]
        series = series.replace('', np.nan)
        import pyarrow as pa
        chunked = pa.chunked_array([pa.array(series)])
        
        inferred = self._dtype_detector._infer_column(chunked_array=chunked)
        print("Inferred : ", inferred)
        detected = str(inferred.get("type", "text")).lower()

        if detected in ("integer", "float"):
            return "numeric"
        if detected == "datetime":
            return "datetime"
        return "categorical"

    # ------------------------------------------------------------------
    # Public API (pandas‑like names)
    # ------------------------------------------------------------------
    @record_call
    async def fillna(self, column: str,value: Optional[Any] = None, method: str = "mean", mapping: Optional[Dict[Any, Any]] = None, dtype: Optional[str] = None,) -> Dict[str, Any]:
        """
        Fill null values in a column using the specified method.

        Args:
            column: Column name.
            value: Replacement value for method='constant'.
            method: 'mean', 'median', 'mode', 'constant', 'min', 'max',
                    'std', 'var', 'now' (datetime only).
            mapping: Dict for method='map' (categorical).
            dtype: Optional hint ('numeric', 'categorical', 'datetime').
                   If provided, it overrides auto-detection.

        Returns:
            Standard response dict with a sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        persist = self._persistence_context()

        method_lower = method.lower()
        detected_dtype = await self._detect_fillna_dtype(ops, table, schema, column)
        print(f"detected_dtype--------- : {detected_dtype}")
        dtype_hint = (dtype or "").strip().lower()
        if dtype_hint in ("numeric", "categorical", "datetime"):
            detected_dtype = dtype_hint

        if method_lower == "map":
            return await ops.categorical_fillna(table, schema, column, mode="MAP", mapping=mapping, **persist)

        # Datetime‑specific mode
        if method_lower in ("now",):
            if detected_dtype != "datetime":
                return {
                    "is_error": True,
                    "error_message": f"Method '{method}' is only valid for datetime columns (detected: {detected_dtype})",
                    "involved_cols": [column],
                    "generated_cols": [],
                }
            return await ops.datetime_fillna(
                table, schema, column, mode=method_lower.upper(), value=value, **persist
            )

        # Route using detected type
        if detected_dtype == "numeric":
            return await ops.numeric_fillna(
                table, schema, column, mode=method_lower.upper(), value=value, **persist
            )

        if detected_dtype == "datetime":
            return await ops.datetime_fillna(
                table, schema, column, mode=method_lower.upper(), value=value, **persist
            )

        # Categorical/text/boolean branch
        if method_lower in ("mean", "median", "std", "var", "min", "max"):
            return {
                "is_error": True,
                "error_message": f"Method '{method}' requires a numeric column (detected: {detected_dtype})",
                "involved_cols": [column],
                "generated_cols": [],
            }

        return await ops.categorical_fillna(table, schema, column, mode=method_lower.upper(), value=value, mapping=mapping, **persist)

    @record_call
    async def clip(self, column: str,  lower: int|float = None, upper: int|float = None,) -> Dict[str, Any]:
        """
        Trim values at lower and/or upper bounds. Out‑of‑bounds become NULL.
        Equivalent to pandas `clip` but with NULL instead of bound clamping.

        Args:
            column: Column name.
            lower: Minimum allowed value.
            upper: Maximum allowed value.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_enforce_range(
            table, schema, column, lower, upper, **self._persistence_context()
        )

    @record_call
    async def drop_outliers(self, column: str, z_thresh: float = 3.0,) -> Dict[str, Any]:
        """
        Remove outliers using Z‑score method. Outliers become NULL.

        Args:
            column: Numeric column name.
            z_thresh: Z‑score threshold (default 3.0).

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_drop_outliers_zscore(
            table, schema, column, z_thresh, **self._persistence_context()
        )

    @record_call
    async def to_numeric(self, column: str, ) -> Dict[str, Any]:
        """
        Convert a text column to numeric, stripping non‑numeric characters.

        Args:
            column: Column name.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_convert_text(table, schema, column, **self._persistence_context())

    @record_call
    async def map_values(self, column: str, mapping: Dict[Any, Any],) -> Dict[str, Any]:
        """
        Replace categorical values using a mapping dictionary.

        Args:
            column: Column name.
            mapping: Dict of old → new values.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_map_values(
            table, schema, column, mapping, **self._persistence_context()
        )

    @record_call
    async def filter_valid( self, column: str, valid_values: List[str]) -> Dict[str, Any]:
        """
        Set values not in `valid_values` to NULL.

        Args:
            column: Column name.
            valid_values: List of allowed values.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_filter_invalid(
            table, schema, column, valid_values, **self._persistence_context()
        )

    @record_call
    async def compress_rare(self, column: str, min_count: int = 10, other_label: str = "other",) -> Dict[str, Any]:
        """
        Replace rare categories (appearing less than `min_count` times)
        with a single label.

        Args:
            column: Categorical column name.
            min_count: Minimum frequency to keep original value.
            other_label: Label for rare categories.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_compress_rare(
            table, schema, column, min_count, other_label, **self._persistence_context()
        )

    @record_call
    async def fix_dates(self, column: str,) -> Dict[str, Any]:
        """
        Fix common invalid date strings (e.g., '0000-00-00') by setting to NULL.

        Args:
            column: Date column name.

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_fix_invalid(
            table, schema, column, **self._persistence_context()
        )

    @record_call
    async def clip_dates(self, column: str,min_dt: Optional[str] = None, max_dt: Optional[str] = None,) -> Dict[str, Any]:
        """
        Remove dates outside a specified range (set to NULL).

        Args:
            column: Date column name.
            min_dt: Minimum allowed date (inclusive, format 'YYYY-MM-DD').
            max_dt: Maximum allowed date (inclusive).

        Returns:
            Standard response dict with sample DataFrame.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_remove_out_of_range(
            table, schema, column, min_dt, max_dt, **self._persistence_context()
        )

    @record_call
    async def groupby_fillna(self, column: str, group_cols: List[str], value: Optional[Any] = None, method: str = "mean", dtype: Optional[str] = None,) -> Dict[str, Any]:
        """
        Groupby-based fillna.

        Args:
            column: Column to fill.
            group_cols: Columns to group by.
            value: For constant (if ever extended).
            method: 'mean', 'median', 'mode', 'min', 'max',
                    'std', 'var', 'ffill', 'bfill', 'now'.
            mapping: For categorical map.
            dtype: Optional override ('numeric', 'categorical', 'datetime').

        Returns:
            Standard response dict.
        """
        if not group_cols:
            return {
                "is_error": True,
                "error_message": "group_cols must be provided for groupby_fillna",
                "involved_cols": [column],
                "generated_cols": [],
            }

        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        persist = self._persistence_context()

        method_lower = method.lower()

        # ----------------------------------------
        # DETECT DTYPE (REUSE YOUR METHOD)
        # ----------------------------------------
        detected_dtype = await self._detect_fillna_dtype(
            ops, table, schema, column
        )

        dtype_hint = (dtype or "").strip().lower()
        if dtype_hint in ("numeric", "categorical", "datetime"):
            detected_dtype = dtype_hint

        
        # ----------------------------------------
        # DATETIME ONLY METHOD
        # ----------------------------------------
        if method_lower in ("now",):
            if detected_dtype != "datetime":
                return {
                    "is_error": True,
                    "error_message": f"Method '{method}' only valid for datetime columns (detected: {detected_dtype})",
                    "involved_cols": [column],
                    "generated_cols": [],
                }

            return await ops.datetime_fillna_groupby(
                table,
                schema,
                column,
                group_cols=group_cols,
                mode=method_lower.upper(),
                **persist,
            )

        # ----------------------------------------
        # NUMERIC
        # ----------------------------------------
        if detected_dtype == "numeric":
            return await ops.numeric_fillna_groupby(
                table,
                schema,
                column,
                group_cols=group_cols,
                mode=method_lower.upper(),
                value=value,
                **persist,
            )

        # ----------------------------------------
        # DATETIME
        # ----------------------------------------
        if detected_dtype == "datetime":
            return await ops.datetime_fillna_groupby(
                table,
                schema,
                column,
                group_cols=group_cols,
                mode=method_lower.upper(),
                **persist,
            )

        # ----------------------------------------
        # INVALID NUMERIC METHODS ON CATEGORICAL
        # ----------------------------------------
        if method_lower in ("mean", "median", "std", "var", "min", "max"):
            return {
                "is_error": True,
                "error_message": f"Method '{method}' requires numeric column (detected: {detected_dtype})",
                "involved_cols": [column],
                "generated_cols": [],
            }

        # ----------------------------------------
        # CATEGORICAL
        # ----------------------------------------
        return await ops.categorical_fillna_groupby(
            table,
            schema,
            column,
            group_cols=group_cols,
            mode=method_lower.upper(),
            **persist,
        )
         
    @record_call       
    async def dropna(self, axis: int = 0, how: str = "any", thresh: Optional[int] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        return await ops.dataframe_dropna(
            table,
            schema,
            axis=axis,
            how=how,
            thresh=thresh,
            **self._persistence_context(),
        )
    
    @record_call
    async def drop(self, axis: int = 0, index: Optional[List[int]] = None, columns: Optional[List[str]] = None,) -> Dict[str, Any]:
        """
        Drop rows or columns from the active dataset.

        Parameters
        ----------
        axis : {0, 1}
            0 → drop rows (requires index)
            1 → drop columns (requires columns)

        index : list[int], optional
            Row indices to drop (used when axis=0)

        columns : list[str], optional
            Column names to drop (used when axis=1)

        Returns
        -------
        Dict[str, Any]
            Standard response with resulting DataFrame
        """
        ops = await self._ensure_ops()

        table, schema = await self._get_context()

        return await ops.dataframe_drop(
            table=table,
            schema=schema,
            axis=axis,
            index=index,
            columns=columns,
            **self._persistence_context(),
        )
    
    @record_call
    async def isna(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_isna(table, schema, **self._persistence_context())
    
    @record_call    
    async def notna(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_notna(table, schema, **self._persistence_context())

    @record_call
    async def drop_duplicates(self,  subset: Optional[List[str]] = None,  keep: Union[str, bool] = "first",) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        
        table, schema = await self._get_context()

        return await ops.dataframe_drop_duplicates(
            table=table,
            schema=schema,
            subset=subset,
            keep=keep,
            **self._persistence_context(),
        )
        
        
        
    # ── data quality ─────────────────────────────────
    @record_call
    async def data_quality_missing_values(self, columns: List[str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.data_quality_missing_values(table, schema, columns)

    @record_call
    async def data_quality_completeness_score(self, columns: List[str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.data_quality_completeness_score(table, schema, columns)

    # ── comprehensive ────────────────────────────────
    @record_call
    async def comprehensive_numeric_summary(self, columns: List[str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.comprehensive_numeric_summary(table, schema, columns)

    @record_call
    async def statistical_profile_report(self, columns: List[str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.statistical_profile_report(table, schema, columns)
    
    
