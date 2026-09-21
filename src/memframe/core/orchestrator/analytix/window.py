from typing import Any, Dict, List, Tuple, Union

import numpy as np
from memframe.core.analytix.window import WindowOps, make_window_ops
from memframe.core.ingestion.datatype_detector import DatatypeDetector
from memframe.cache import record_call


class WindowOrchestrator:
    """
    Supports rolling, expanding, and ewm operations with automatic
    dtype detection, multi‑function chaining, and single‑table output.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._window_ops = None
        self._dtype_detector = DatatypeDetector()

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)

    async def _ensure_ops(self) -> WindowOps:
        if self._window_ops is None:
            await self._ops_parent._ensure_adapter()
            self._window_ops = make_window_ops(self._ops_parent._adapter)
        return self._window_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

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
        alias_map = {"nunqiue": "nunique"}   # typo tolerance

        for item in raw_funcs:
            if not isinstance(item, str) or not item.strip():
                invalid.append(item)
                continue
            key = alias_map.get(item.strip().lower(), item.strip().lower())
            if key not in seen:
                normalized.append(key)
                seen.add(key)

        return normalized, invalid

    def _dtype_method_map(self, dtype: str) -> Dict[str, str]:
        numeric_map = {
            "sum": "rolling_sum",
            "mean": "rolling_mean",
            "min": "rolling_min",
            "max": "rolling_max",
            "count": "rolling_count",
            "std": "rolling_std",
            "var": "rolling_var",
            "quantile": "rolling_quantile",
            "sem": "rolling_sem",
            "rank": "rolling_rank",
            "nunique": "rolling_nunique",
            "first": "rolling_first",
            "last": "rolling_last",
        }

        datetime_map = {
            "min": "rolling_min_datetime",
            "max": "rolling_max_datetime",
            "mean": "rolling_mean_datetime",
            "median": "rolling_median_datetime",
            "mode": "rolling_mode_datetime",
            "count": "rolling_count",
            "nunique": "rolling_nunique",
            "rank": "rolling_rank",
            "first": "rolling_first",
            "last": "rolling_last",
        }

        categorical_map = {
            "min": "rolling_min",
            "max": "rolling_max",
            "count": "rolling_count",
            "nunique": "rolling_nunique",
            "rank": "rolling_rank",
            "first": "rolling_first",
            "last": "rolling_last",
        }

        dtype_key = dtype if dtype in {"numeric", "datetime", "categorical"} else "categorical"
        if dtype_key == "numeric":
            return numeric_map
        if dtype_key == "datetime":
            return datetime_map
        return categorical_map

    # --------------------------------------------------
    # ROLLING API (FIXED CHAINING)
    # --------------------------------------------------
    @record_call
    async def rolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._ops_parent.memframe._backend
        data_id = self._ops_parent._data_id or self._ops_parent.memframe._active_id

        dtype = await self._detect_dtype(ops, table, schema, column)
        requested_funcs, invalid_funcs = self._normalize_funcs(func)
        if not requested_funcs:
            return {
                "is_error": True,
                "error_message": "At least one valid rolling function is required.",
            }

        dtype_map = self._dtype_method_map(dtype)
        all_known_funcs = set().union(
            self._dtype_method_map("numeric").keys(),
            self._dtype_method_map("datetime").keys(),
            self._dtype_method_map("categorical").keys(),
        )

        unsupported_unknown = []
        unsupported_dtype = []
        executable = []

        for func_key in requested_funcs:
            if func_key not in all_known_funcs:
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
                "error_message": (
                    f"No requested rolling function is supported for dtype '{dtype}'."
                ),
                "dtype": dtype,
                "requested_funcs": requested_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        # ---- CHAINING: each successful call updates the working table ----
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
                order_by=order_by,
                window=window,
                backend=backend,
                data_id=data_id,
            )
            if func_key == "quantile":
                call_kwargs["q"] = q

            result = await method(**call_kwargs)

            if result.get("is_error"):
                failed_funcs[func_key] = result.get("error_message", "Unknown error")
                continue

            last_success_result = result
            successful_funcs.append(func_key)

            # Collect all newly created columns
            if isinstance(result.get("new_columns"), list):
                generated_cols.extend(result["new_columns"])
            elif result.get("new_column"):
                generated_cols.append(result["new_column"])

            # **Critical fix**: move to the table that was just generated
            new_table = result.get("new_table")
            if new_table:
                current_table = new_table

        # If nothing succeeded, return error
        if not successful_funcs:
            return {
                "is_error": True,
                "message": "",
                "error_message": "No rolling function could be executed successfully.",
                "dtype": dtype,
                "requested_funcs": requested_funcs,
                "failed_funcs": failed_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        # Single successful function → pass through its original result unchanged
        if len(requested_funcs) == 1 and not failed_funcs and not unsupported_unknown and not unsupported_dtype and not invalid_funcs:
            return last_success_result

        # Multiple functions → fetch from the final chained table
        generated_cols = list(dict.fromkeys(generated_cols))  # order preserving dedup
        preview_cols = [column] + generated_cols
        if order_by:
            if isinstance(order_by, (list, tuple)):
                preview_cols.extend(order_by)
            else:
                preview_cols.append(order_by)
        preview_cols = list(dict.fromkeys(preview_cols))

        final_result = None
        try:
            final_result = await ops._fetch_data(current_table, schema, preview_cols)
        except Exception:
            final_result = last_success_result.get("result") if last_success_result else None

        skipped_funcs = {
            **{k: "Unsupported rolling function." for k in unsupported_unknown},
            **{k: f"Not supported for detected dtype '{dtype}'." for k in unsupported_dtype},
            **{str(k): "Invalid function entry." for k in invalid_funcs},
        }
        is_partial = bool(skipped_funcs or failed_funcs)
        message = (
            "Partial rolling functions applied." if is_partial
            else "All requested rolling functions applied."
        )

        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "dtype": dtype,
            "result": final_result,
            "new_table": current_table,
            "new_columns": generated_cols,
            "successful_funcs": successful_funcs,
            "skipped_funcs": skipped_funcs,
            "failed_funcs": failed_funcs,
            "is_partial": is_partial,
        }

    # --------------------------------------------------
    # EXPANDING API (FIXED CHAINING)
    # --------------------------------------------------
    @record_call
    async def expanding(
        self,
        column: str,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
        min_periods: int = 1,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._ops_parent.memframe._backend
        data_id = self._ops_parent._data_id or self._ops_parent.memframe._active_id

        dtype = await self._detect_dtype(ops, table, schema, column)
        requested_funcs, invalid_funcs = self._normalize_funcs(func)
        if not requested_funcs:
            return {
                "is_error": True,
                "error_message": "At least one valid expanding function is required.",
            }

        # method mapping per dtype
        numeric_map = {
            "sum": "expanding_sum",
            "mean": "expanding_mean",
            "min": "expanding_min",
            "max": "expanding_max",
            "count": "expanding_count",
            "std": "expanding_std",
            "var": "expanding_var",
            "quantile": "expanding_quantile",
            "sem": "expanding_sem",
            "rank": "expanding_rank",
            "nunique": "expanding_nunique",
            "first": "expanding_first",
            "last": "expanding_last",
        }
        datetime_map = {
            "min": "expanding_min_datetime",
            "max": "expanding_max_datetime",
            "mean": "expanding_mean_datetime",
            "median": "expanding_median_datetime",
            "mode": "expanding_mode_datetime",
            "count": "expanding_count",
            "nunique": "expanding_nunique",
            "rank": "expanding_rank",
            "first": "expanding_first",
            "last": "expanding_last",
        }
        categorical_map = {
            "count": "expanding_count",
            "nunique": "expanding_nunique",
            "rank": "expanding_rank",
            "first": "expanding_first",
            "last": "expanding_last",
        }

        dtype_map = {
            "numeric": numeric_map,
            "datetime": datetime_map,
            "categorical": categorical_map,
        }.get(dtype, categorical_map)

        all_known = set(numeric_map.keys()) | set(datetime_map.keys()) | set(categorical_map.keys())
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

        # ---- CHAINING ----
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
                order_by=order_by,
                backend=backend,
                data_id=data_id,
                min_periods=min_periods,
            )
            if func_key == "quantile":
                call_kwargs["q"] = q

            result = await method(**call_kwargs)

            if result.get("is_error"):
                failed_funcs[func_key] = result.get("error_message", "Unknown error")
                continue

            last_success_result = result
            successful_funcs.append(func_key)

            if isinstance(result.get("new_columns"), list):
                generated_cols.extend(result["new_columns"])
            elif result.get("new_column"):
                generated_cols.append(result["new_column"])

            new_table = result.get("new_table")
            if new_table:
                current_table = new_table

        if not successful_funcs:
            return {
                "is_error": True,
                "error_message": "No expanding function could be executed successfully.",
                "failed_funcs": failed_funcs,
                "unsupported_unknown": unsupported_unknown,
                "unsupported_for_dtype": unsupported_dtype,
                "invalid_funcs": invalid_funcs,
            }

        # Single successful function → pass through
        if len(requested_funcs) == 1 and not failed_funcs and not unsupported_unknown and not unsupported_dtype and not invalid_funcs:
            return last_success_result

        generated_cols = list(dict.fromkeys(generated_cols))
        preview_cols = [column] + generated_cols
        if order_by:
            if isinstance(order_by, (list, tuple)):
                preview_cols.extend(order_by)
            else:
                preview_cols.append(order_by)
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
    # EWM API (already returns a single table; unchanged)
    # --------------------------------------------------
    @record_call
    async def ewm(
        self,
        column: str,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
        func: Union[str, List[str]] = "mean",
        order_by: Union[str, List[str]] = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._ops_parent.memframe._backend
        data_id = self._ops_parent._data_id or self._ops_parent.memframe._active_id

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

        result = await ops.ewm(
            table, schema, column,
            order_by=order_by,
            com=com, span=span, halflife=halflife, alpha=alpha,
            adjust=adjust, ignore_na=ignore_na, min_periods=min_periods,
            agg=funcs,
            backend=backend, data_id=data_id,
        )
        return result