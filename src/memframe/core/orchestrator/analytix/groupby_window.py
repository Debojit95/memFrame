"""
Group‑by window orchestrator.
Now supports the same unified interface as WindowOrchestrator:
    ops.groupby_window.rolling("col", window=3, func=["mean","std"], group_cols="A")
    ops.groupby_window.expanding("col", min_periods=2, func="sum", group_cols=["A","B"])
    ops.groupby_window.ewm("col", span=5, func=["mean","var"], group_cols="A")

All operations are recorded (via @record_call).
The builder pattern (groupby().rolling().mean()) is also retained.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
from memframe.core.analytix.groupby_window import GroupbyWindowOps
from memframe.core.ingestion.datatype_detector import DatatypeDetector
from memframe.cache import record_call


class GroupByWindow:
    """Created by `groupby()`, holds group columns and provides builder methods."""

    def __init__(self, parent: "GroupByWindowOrchestrator",memframe_ops_instance, group_cols: List[str]):
        self._parent = parent
        self._memframe_ops_instance = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self.group_cols = group_cols

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)
    
    
    async def _get_context_and_ops(self):
        await self._parent._ensure_adapter()
        if self._parent._core_ops is None:
            self._parent._core_ops = GroupbyWindowOps(self._parent._adapter)
        table, schema = await self._parent._get_active_context()
        backend = self._parent._memframe._backend
        data_id = self._parent._data_id or self._parent._memframe._active_id
        return self._parent._core_ops, table, schema, backend, data_id

    @record_call
    def rolling(self, window: int, order_by: Union[str, List[str]] = None) -> "GroupByRolling":
        return GroupByRolling(self, window, order_by)
    
    @record_call
    def expanding(self, min_periods: int = 1, order_by: Union[str, List[str]] = None) -> "GroupByExpanding":
        return GroupByExpanding(self, min_periods, order_by)
    
    @record_call
    def ewm(self, order_by=None, com=None, span=None, halflife=None, alpha=None,
            adjust=True, ignore_na=False, min_periods=0) -> "GroupByEWM":
        return GroupByEWM(self,order_by, com, span, halflife, alpha, adjust, ignore_na, min_periods)


class GroupByRolling:
    
    def __init__(self, group_window: GroupByWindow, window: int, order_by):
        self._gw = group_window
        self.window = window
        self.order_by = order_by


    async def _run(self, method_suffix: str, column: str, **extra_kwargs):
        
        return await self._gw._parent.rolling(
            column=column,
            window=self.window,
            func=method_suffix,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            **extra_kwargs,
        )

    async def mean(self, column: str) -> Dict[str, Any]:
        return await self._run("mean", column)

    async def sum(self, column: str) -> Dict[str, Any]:
        return await self._run("sum", column)

    async def min(self, column: str) -> Dict[str, Any]:
        return await self._run("min", column)

    async def max(self, column: str) -> Dict[str, Any]:
        return await self._run("max", column)

    async def std(self, column: str) -> Dict[str, Any]:
        return await self._run("std", column)

    async def var(self, column: str) -> Dict[str, Any]:
        return await self._run("var", column)

    async def count(self, column: str) -> Dict[str, Any]:
        return await self._run("count", column)

    async def nunique(self, column: str) -> Dict[str, Any]:
        return await self._run("nunique", column)

    async def first(self, column: str) -> Dict[str, Any]:
        return await self._run("first", column)

    async def last(self, column: str) -> Dict[str, Any]:
        return await self._run("last", column)

    async def quantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        return await self._run("quantile", column, q=q)

    async def rank(self, column: str) -> Dict[str, Any]:
        return await self._run("rank", column)

    async def sem(self, column: str) -> Dict[str, Any]:
        return await self._run("sem", column)

    async def agg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Apply multiple rolling aggregations at once, returning a single DataFrame."""
        return await self._gw._parent.rolling(
            column=column,
            window=self.window,
            func=funcs,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            **kwargs,
        )


class GroupByExpanding:
    
    def __init__(self, group_window: GroupByWindow, min_periods: int, order_by):       
        self._gw = group_window
        self.min_periods = min_periods
        self.order_by = order_by

    
    async def _run(self, method_suffix: str, column: str, **extra_kwargs):
        return await self._gw._parent.expanding(
            column=column,
            func=method_suffix,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            min_periods=self.min_periods,
            **extra_kwargs,
        )

    async def sum(self, column: str) -> Dict[str, Any]:
        return await self._run("sum", column)

    async def mean(self, column: str) -> Dict[str, Any]:
        return await self._run("mean", column)

    async def min(self, column: str) -> Dict[str, Any]:
        return await self._run("min", column)

    async def max(self, column: str) -> Dict[str, Any]:
        return await self._run("max", column)

    async def std(self, column: str) -> Dict[str, Any]:
        return await self._run("std", column)

    async def var(self, column: str) -> Dict[str, Any]:
        return await self._run("var", column)

    async def count(self, column: str) -> Dict[str, Any]:
        return await self._run("count", column)

    async def nunique(self, column: str) -> Dict[str, Any]:
        return await self._run("nunique", column)

    async def first(self, column: str) -> Dict[str, Any]:
        return await self._run("first", column)

    async def last(self, column: str) -> Dict[str, Any]:
        return await self._run("last", column)

    async def quantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        return await self._run("quantile", column, q=q)

    async def rank(self, column: str) -> Dict[str, Any]:
        return await self._run("rank", column)

    async def sem(self, column: str) -> Dict[str, Any]:
        return await self._run("sem", column)

    async def agg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Apply multiple expanding aggregations at once, returning a single DataFrame."""
        return await self._gw._parent.expanding(
            column=column,
            func=funcs,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            min_periods=self.min_periods,
            **kwargs,
        )


class GroupByEWM:
    def __init__(self,group_window: GroupByWindow,order_by, com, span, halflife, alpha, adjust, ignore_na, min_periods):
        self._gw = group_window
        self.order_by = order_by
        self.com = com
        self.span = span
        self.halflife = halflife
        self.alpha = alpha
        self.adjust = adjust
        self.ignore_na = ignore_na
        self.min_periods = min_periods

    
    async def _run(self, column: str, agg: Union[str, List[str]]):
        return await self._gw._parent.ewm(
            column=column,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            com=self.com,
            span=self.span,
            halflife=self.halflife,
            alpha=self.alpha,
            adjust=self.adjust,
            ignore_na=self.ignore_na,
            min_periods=self.min_periods,
            func=agg,
        )

    async def mean(self, column: str) -> Dict[str, Any]:
        return await self._run(column, "mean")

    async def sum(self, column: str) -> Dict[str, Any]:
        return await self._run(column, "sum")

    async def std(self, column: str) -> Dict[str, Any]:
        return await self._run(column, "std")

    async def var(self, column: str) -> Dict[str, Any]:
        return await self._run(column, "var")

    async def agg(self, column: str, funcs: List[str]) -> Dict[str, Any]:
        """Apply multiple EWM aggregations at once, returning a single DataFrame."""
        return await self._run(column, funcs)


class GroupByWindowOrchestrator:
    """
    Unified group‑by window orchestrator.
    Provides both a builder pattern and direct dispatch methods.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._core_ops: Optional[GroupbyWindowOps] = None
        self._adapter = None
        self._dtype_detector = DatatypeDetector()

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)
    
    
    async def _ensure_adapter(self):
        await self._ops_parent._ensure_adapter()
        self._adapter = self._ops_parent._adapter

    async def _get_active_context(self):
        return await self._ops_parent._get_active_context()

    async def _ensure_ops(self) -> GroupbyWindowOps:
        if self._core_ops is None:
            await self._ensure_adapter()
            self._core_ops = GroupbyWindowOps(self._adapter)
        return self._core_ops

    async def _detect_dtype(self, ops, table, schema, column):
        sample_df = await ops._fetch_data(table, schema, columns=[column], limit=10)
        if sample_df.empty:
            return "categorical"
        series = sample_df[column].replace('', np.nan) if column in sample_df.columns else sample_df.iloc[:, 0]
        import pyarrow as pa
        chunked = pa.chunked_array([pa.array(series)])
        inferred = self._dtype_detector._infer_column(chunked)
        detected = str(inferred.get("type", "text")).lower()
        if detected in ("integer", "float"):
            return "numeric"
        if detected == "datetime":
            return "datetime"
        return "categorical"

    def _normalize_funcs(self, func: Union[str, List[str]]) -> Tuple[List[str], List[Any]]:
        if isinstance(func, str):
            raw_funcs = [func]
        elif isinstance(func, (list, tuple)):
            raw_funcs = list(func)
        else:
            return [], [func]
        normalized = []
        invalid = []
        seen = set()
        alias_map = {"nunqiue": "nunique"}
        for item in raw_funcs:
            if not isinstance(item, str) or not item.strip():
                invalid.append(item)
                continue
            key = alias_map.get(item.strip().lower(), item.strip().lower())
            if key not in seen:
                normalized.append(key)
                seen.add(key)
        return normalized, invalid

    def _dtype_method_map(self, dtype: str, window_type: str) -> Dict[str, str]:
        """Return mapping of user func names to groupby core method names."""
        if window_type == "rolling":
            numeric_map = {
                "sum": "rolling_sum_groupby",
                "mean": "rolling_mean_groupby",
                "min": "rolling_min_groupby",
                "max": "rolling_max_groupby",
                "count": "rolling_count_groupby",
                "std": "rolling_std_groupby",
                "var": "rolling_var_groupby",
                "quantile": "rolling_quantile_groupby",
                "sem": "rolling_sem_groupby",
                "rank": "rolling_rank_groupby",
                "nunique": "rolling_nunique_groupby",
                "first": "rolling_first_groupby",
                "last": "rolling_last_groupby",
            }
            datetime_map = {
                "min": "rolling_min_datetime_groupby",
                "max": "rolling_max_datetime_groupby",
                "mean": "rolling_mean_datetime_groupby",
                "median": "rolling_median_datetime_groupby",
                "mode": "rolling_mode_datetime_groupby",
                "count": "rolling_count_groupby",
                "nunique": "rolling_nunique_groupby",
                "rank": "rolling_rank_groupby",
                "first": "rolling_first_groupby",
                "last": "rolling_last_groupby",
            }
            categorical_map = {
                "min": "rolling_min_groupby",
                "max": "rolling_max_groupby",
                "count": "rolling_count_groupby",
                "nunique": "rolling_nunique_groupby",
                "rank": "rolling_rank_groupby",
                "first": "rolling_first_groupby",
                "last": "rolling_last_groupby",
            }
        elif window_type == "expanding":
            numeric_map = {
                "sum": "expanding_sum_groupby",
                "mean": "expanding_mean_groupby",
                "min": "expanding_min_groupby",
                "max": "expanding_max_groupby",
                "count": "expanding_count_groupby",
                "std": "expanding_std_groupby",
                "var": "expanding_var_groupby",
                "quantile": "expanding_quantile_groupby",
                "sem": "expanding_sem_groupby",
                "rank": "expanding_rank_groupby",
                "nunique": "expanding_nunique_groupby",
                "first": "expanding_first_groupby",
                "last": "expanding_last_groupby",
            }
            datetime_map = {
                "min": "expanding_min_datetime_groupby",
                "max": "expanding_max_datetime_groupby",
                "mean": "expanding_mean_datetime_groupby",
                "median": "expanding_median_datetime_groupby",
                "mode": "expanding_mode_datetime_groupby",
                "count": "expanding_count_groupby",
                "nunique": "expanding_nunique_groupby",
                "rank": "expanding_rank_groupby",
                "first": "expanding_first_groupby",
                "last": "expanding_last_groupby",
            }
            categorical_map = {
                "count": "expanding_count_groupby",
                "nunique": "expanding_nunique_groupby",
                "rank": "expanding_rank_groupby",
                "first": "expanding_first_groupby",
                "last": "expanding_last_groupby",
            }
        else:
            raise ValueError(f"Unknown window_type: {window_type}")

        dtype_key = dtype if dtype in {"numeric", "datetime", "categorical"} else "categorical"
        if dtype_key == "numeric":
            return numeric_map
        if dtype_key == "datetime":
            return datetime_map
        return categorical_map

    # --------------------------------------------------
    #  UNIFIED ROLLING
    # --------------------------------------------------
    @record_call
    async def rolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(group_cols, str):
            group_cols = [group_cols]

        dtype = await self._detect_dtype(ops, table, schema, column)
        requested_funcs, invalid_funcs = self._normalize_funcs(func)
        if not requested_funcs:
            return {"is_error": True, "error_message": "At least one valid rolling function is required."}

        dtype_map = self._dtype_method_map(dtype, "rolling")
        all_known = set().union(
            self._dtype_method_map("numeric", "rolling").keys(),
            self._dtype_method_map("datetime", "rolling").keys(),
            self._dtype_method_map("categorical", "rolling").keys(),
        )
        unsupported_unknown = []
        unsupported_dtype = []
        executable = []

        for func_key in requested_funcs:
            if func_key not in all_known:
                unsupported_unknown.append(func_key)
                continue
            method_name = dtype_map.get(func_key)
            if not method_name:
                unsupported_dtype.append(func_key)
                continue
            executable.append((func_key, method_name))

        if not executable:
            return {
                "is_error": True,
                "message": "",
                "error_message": f"No requested rolling function is supported for dtype '{dtype}'.",
                "dtype": dtype,
                "requested_funcs": requested_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        current_table = table
        successful_funcs = []
        failed_funcs = {}
        generated_cols = []
        last_success_result = None

        for func_key, method_name in executable:
            method = getattr(ops, method_name)
            call_kwargs = dict(
                table=current_table,
                schema=schema,
                column=column,
                group_cols=group_cols,
                order_by=order_by,
                window=window,
                backend=backend,
                data_id=data_id,
                map_feature=map_feature,
            )
            if func_key == "quantile":
                call_kwargs["q"] = q

            result = await method(**call_kwargs)
            if result.get("is_error"):
                failed_funcs[func_key] = result.get("error_message", "Unknown error")
                continue

            last_success_result = result
            successful_funcs.append(func_key)

            new_table = result.get("new_table")
            if new_table:
                current_table = new_table

            if isinstance(result.get("new_columns"), list):
                generated_cols.extend(result["new_columns"])
            elif result.get("new_column"):
                generated_cols.append(result["new_column"])

        if not successful_funcs:
            first_failure = next(iter(failed_funcs.values()), "")
            detail = f" First failure: {first_failure}" if first_failure else ""
            return {
                "is_error": True,
                "message": "",
                "error_message": f"No rolling function could be executed successfully.{detail}",
                "dtype": dtype,
                "requested_funcs": requested_funcs,
                "failed_funcs": failed_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        if len(requested_funcs) == 1 and not failed_funcs and not unsupported_unknown and not unsupported_dtype and not invalid_funcs:
            return last_success_result

        generated_cols = list(dict.fromkeys(generated_cols))
        preview_cols = [column] + generated_cols
        if order_by:
            if isinstance(order_by, (list, tuple)):
                preview_cols.extend(order_by)
            else:
                preview_cols.append(order_by)
        preview_cols.extend(group_cols)
        preview_cols = list(dict.fromkeys(preview_cols))

        final_result = None
        try:
            final_result = await ops._fetch_data(current_table, schema, preview_cols)
        except Exception:
            final_result = last_success_result.get("result") if last_success_result else None

        skipped = {
            **{k: "Unsupported rolling function." for k in unsupported_unknown},
            **{k: f"Not supported for dtype '{dtype}'." for k in unsupported_dtype},
            **{str(k): "Invalid function entry." for k in invalid_funcs},
        }
        is_partial = bool(skipped or failed_funcs)
        message = "Partial rolling functions applied." if is_partial else "All requested rolling functions applied."

        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "dtype": dtype,
            "result": final_result,
            "new_table": current_table,
            "new_columns": generated_cols,
            "successful_funcs": successful_funcs,
            "skipped_funcs": skipped,
            "failed_funcs": failed_funcs,
            "is_partial": is_partial,
        }

    # --------------------------------------------------
    #  UNIFIED EXPANDING
    # --------------------------------------------------
    @record_call
    async def expanding(
        self,
        column: str,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
        min_periods: int = 1,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(group_cols, str):
            group_cols = [group_cols]

        dtype = await self._detect_dtype(ops, table, schema, column)
        requested_funcs, invalid_funcs = self._normalize_funcs(func)
        if not requested_funcs:
            return {"is_error": True, "error_message": "At least one valid expanding function is required."}

        dtype_map = self._dtype_method_map(dtype, "expanding")
        all_known = set().union(
            self._dtype_method_map("numeric", "expanding").keys(),
            self._dtype_method_map("datetime", "expanding").keys(),
            self._dtype_method_map("categorical", "expanding").keys(),
        )
        unsupported_unknown = []
        unsupported_dtype = []
        executable = []

        for func_key in requested_funcs:
            if func_key not in all_known:
                unsupported_unknown.append(func_key)
                continue
            method_name = dtype_map.get(func_key)
            if not method_name:
                unsupported_dtype.append(func_key)
                continue
            executable.append((func_key, method_name))

        if not executable:
            return {
                "is_error": True,
                "error_message": f"No requested expanding function is supported for dtype '{dtype}'.",
                "dtype": dtype,
                "requested_funcs": requested_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        current_table = table
        successful_funcs = []
        failed_funcs = {}
        generated_cols = []
        last_success_result = None

        for func_key, method_name in executable:
            method = getattr(ops, method_name)
            call_kwargs = dict(
                table=current_table,
                schema=schema,
                column=column,
                group_cols=group_cols,
                order_by=order_by,
                min_periods=min_periods,
                backend=backend,
                data_id=data_id,
                map_feature=map_feature,
            )
            if func_key == "quantile":
                call_kwargs["q"] = q

            result = await method(**call_kwargs)
            if result.get("is_error"):
                failed_funcs[func_key] = result.get("error_message", "Unknown error")
                continue

            last_success_result = result
            successful_funcs.append(func_key)

            new_table = result.get("new_table")
            if new_table:
                current_table = new_table

            if isinstance(result.get("new_columns"), list):
                generated_cols.extend(result["new_columns"])
            elif result.get("new_column"):
                generated_cols.append(result["new_column"])

        if not successful_funcs:
            first_failure = next(iter(failed_funcs.values()), "")
            detail = f" First failure: {first_failure}" if first_failure else ""
            return {
                "is_error": True,
                "error_message": f"No expanding function could be executed successfully.{detail}",
                "failed_funcs": failed_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        if len(requested_funcs) == 1 and not failed_funcs and not unsupported_unknown and not unsupported_dtype and not invalid_funcs:
            return last_success_result

        generated_cols = list(dict.fromkeys(generated_cols))
        preview_cols = [column] + generated_cols
        if order_by:
            if isinstance(order_by, (list, tuple)):
                preview_cols.extend(order_by)
            else:
                preview_cols.append(order_by)
        preview_cols.extend(group_cols)
        preview_cols = list(dict.fromkeys(preview_cols))

        final_result = None
        try:
            final_result = await ops._fetch_data(current_table, schema, preview_cols)
        except Exception:
            final_result = last_success_result.get("result") if last_success_result else None

        skipped = {
            **{k: "Unsupported expanding function." for k in unsupported_unknown},
            **{k: f"Not supported for dtype '{dtype}'." for k in unsupported_dtype},
            **{str(k): "Invalid." for k in invalid_funcs},
        }
        is_partial = bool(skipped or failed_funcs)
        message = "Partial expanding functions applied." if is_partial else "All requested expanding functions applied."

        return {
            "is_error": False,
            "message": message,
            "dtype": dtype,
            "result": final_result,
            "new_table": current_table,
            "new_columns": generated_cols,
            "successful_funcs": successful_funcs,
            "skipped_funcs": skipped,
            "failed_funcs": failed_funcs,
            "is_partial": is_partial,
            "min_periods": min_periods,
        }

    # --------------------------------------------------
    #  UNIFIED EWM
    # --------------------------------------------------
    @record_call
    async def ewm(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
        func: Union[str, List[str]] = "mean",
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(group_cols, str):
            group_cols = [group_cols]

        dtype = await self._detect_dtype(ops, table, schema, column)
        if dtype != "numeric":
            return {
                "is_error": True,
                "error_message": f"EWM is only supported for numeric columns (detected {dtype}).",
            }

        if isinstance(func, str):
            funcs = [func]
        else:
            funcs = func
        for f in funcs:
            if f not in ("mean", "sum", "std", "var"):
                return {
                    "is_error": True,
                    "error_message": f"Unsupported ewm function '{f}'. Choose from mean, sum, std, var.",
                }

        result = await ops.ewm_groupby(
            table, schema, column,
            group_cols=group_cols,
            order_by=order_by,
            com=com, span=span, halflife=halflife, alpha=alpha,
            adjust=adjust, ignore_na=ignore_na, min_periods=min_periods,
            agg=funcs,
            backend=backend, data_id=data_id,
        )
        return result

    # --------------------------------------------------
    #  BUILDER PATTERN (kept for convenience)
    # --------------------------------------------------
    def groupby(self, *columns: str) -> GroupByWindow:
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByWindow(self, self._ops_parent,list(columns))
