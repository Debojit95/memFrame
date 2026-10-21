"""
Group-by fillna orchestrator (moved out of groupby_stats).
"""

from typing import Any, Dict, List, Optional, Union

import numpy as np

from memframe.core.analytix.groupby_cleaning import make_groupby_cleaning_ops
from memframe.core.ingestion.datatype_detector import DatatypeDetector
from memframe.cache import record_call


class GroupByFillnaOrchestrator:
    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._cleaning_ops = None
        self._adapter = None
        self._dtype_detector = DatatypeDetector()

    async def _ensure_adapter(self):
        await self._ops_parent._ensure_adapter()
        self._adapter = self._ops_parent._adapter

    async def _get_active_context(self):
        return await self._ops_parent._get_active_context()

    async def _ensure_ops(self):
        if self._cleaning_ops is None:
            await self._ensure_adapter()
            self._cleaning_ops = make_groupby_cleaning_ops(self._adapter)
        return self._cleaning_ops

    def _persistence_context(self) -> Dict[str, Any]:
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        return {"backend": backend, "data_id": data_id}

    async def _detect_fillna_dtype(self, ops, table, schema, column):
        sql_type = (await ops._get_column_type(table, schema, column)).lower()
        if any(k in sql_type for k in ("timestamp", "datetime", "date", "time")):
            return "datetime"
        if any(k in sql_type for k in ("int", "float", "double", "decimal", "numeric", "real")):
            return "numeric"
        sample_df = await ops._fetch_data(table, schema, columns=[column], limit=10)
        if sample_df.empty:
            return "categorical"
        series = sample_df[column].replace("", np.nan)
        import pyarrow as pa
        chunked = pa.chunked_array([pa.array(series)])
        inferred = self._dtype_detector._infer_column(chunked_array=chunked)
        detected = str(inferred.get("type", "text")).lower()
        if detected in ("integer", "float"):
            return "numeric"
        if detected == "datetime":
            return "datetime"
        return "categorical"

    @record_call(deep_cache=True)
    async def groupby_fillna(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        if not group_cols:
            from memframe.core.analytix._response import fail
            return fail("group_cols must be provided for groupby_fillna", [column], [])

        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        persist = self._persistence_context()

        method_lower = method.lower()
        detected_dtype = await self._detect_fillna_dtype(ops, table, schema, column)
        dtype_hint = (dtype or "").strip().lower()
        if dtype_hint in ("numeric", "categorical", "datetime"):
            detected_dtype = dtype_hint

        if method_lower in ("now",):
            if detected_dtype != "datetime":
                from memframe.core.analytix._response import fail as _fail
                return _fail(f"Method '{method}' only valid for datetime columns (detected: {detected_dtype})", [column], [])
            return await ops.datetime_fillna_groupby(table, schema, column, group_cols=group_cols, mode=method_lower.upper(), **persist)

        if detected_dtype == "numeric":
            return await ops.numeric_fillna_groupby(table, schema, column, group_cols=group_cols, mode=method_lower.upper(), value=value, **persist)

        if detected_dtype == "datetime":
            return await ops.datetime_fillna_groupby(table, schema, column, group_cols=group_cols, mode=method_lower.upper(), **persist)

        if method_lower in ("mean", "median", "std", "var", "min", "max"):
            from memframe.core.analytix._response import fail as _fail
            return _fail(f"Method '{method}' requires numeric column (detected: {detected_dtype})", [column], [])

        return await ops.categorical_fillna_groupby(table, schema, column, group_cols=group_cols, mode=method_lower.upper(), **persist)

    def groupby(self, *columns: str) -> "GroupByFillna":
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByFillna(self, list(columns))


class GroupByFillna:
    def __init__(self, parent: "GroupByFillnaOrchestrator", group_cols: List[str]):
        self._parent = parent
        self._memframe = parent._memframe
        self._data_id = parent._data_id
        self.group_cols = group_cols

    @record_call(deep_cache=True)
    async def fillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        parent = self._parent
        caller = getattr(parent, "agroupby_fillna", None) or parent.groupby_fillna
        return await caller(
            column=column,
            group_cols=self.group_cols,
            value=value,
            method=method,
            dtype=dtype,
        )
