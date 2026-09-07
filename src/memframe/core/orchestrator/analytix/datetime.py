from typing import Any, Dict, List, Union

import pandas as pd

from memframe.core.analytix.datetime import DatetimeOps
from memframe.core.analytix._response import fail
from memframe.cache import record_call


def _coerce_datetime_literal(value: Any, name: str) -> Union[str, Dict[str, Any]]:
    # ponytail: fail fast on unparseable bounds so no raw string reaches SQL.
    try:
        parsed = pd.to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return fail(f"{name} must be a datetime-like value, got {value!r}")
    if pd.isna(parsed):
        return fail(f"{name} must be a datetime-like value, got {value!r}")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _coerce_int_list(values: Any, name: str) -> Union[List[int], Dict[str, Any]]:
    items = values if isinstance(values, (list, tuple)) else [values]
    if not items:
        return fail(f"{name} must be a non-empty int or list of ints")
    try:
        return [int(v) for v in items]
    except (TypeError, ValueError):
        return fail(f"{name} must be ints, got {values!r}")


class DateTimeOrchestrator:
    """
    User-facing datetime methods with a pandas-inspired API.
    Accessed via `ops.dt`.
    """

    def __init__(self, memframe_ops_instance):
        """
        Args:
            totem_ops_instance: An instance of TotemOps (or TableOpsOrchestrator)
        """
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe   
        self._data_id = memframe_ops_instance._data_id
        self._datetime_ops = None
    
    
    async def _ensure_ops(self) -> DatetimeOps:
        if self._datetime_ops is None:
            await self._ops_parent._ensure_adapter()
            self._datetime_ops = DatetimeOps(self._ops_parent._adapter)
        return self._datetime_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    @record_call
    async def _extract(self, column: str, field: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.extract(table, schema, column, field)
    
    @record_call
    async def year(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "year")

    @record_call
    async def month(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "month")

    @record_call
    async def day(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "day")

    @record_call
    async def hour(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "hour")

    @record_call
    async def minute(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "minute")

    @record_call
    async def second(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "second")

    @record_call
    async def dayofweek(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "dayofweek")

    @record_call
    async def dayofyear(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "dayofyear")

    @record_call
    async def week(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "week")

    @record_call
    async def quarter(self, column: str) -> Dict[str, Any]:
        return await self._extract(column, "quarter")

    @record_call
    async def floor(self, column: str, unit: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.floor(table, schema, column, unit)


    @record_call
    async def ceil(self, column: str, unit: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.ceil(table, schema, column, unit)


    @record_call
    async def round(self, column: str, unit: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.round(table, schema, column, unit)
    
    @record_call
    async def tz_localize(self, column: str, tz: str | None, ambiguous: str = "raise", nonexistent: str = "raise",) -> Dict[str, Any]:

        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.tz_localize(table, schema,  column, tz, ambiguous, nonexistent,)
        
        
    @record_call
    async def tz_convert(self, column: str, tz: str | None,) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        return await ops.tz_convert(table,  schema, column, tz)
        
    @record_call
    async def is_month_start(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_month_start(table, schema, column)


    @record_call
    async def is_month_end(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_month_end(table, schema, column)


    @record_call
    async def is_year_start(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_year_start(table, schema, column)


    @record_call
    async def is_year_end(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_year_end(table, schema, column)
    
    @record_call
    async def is_quarter_start(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_quarter_start(table, schema, column)
    
    @record_call
    async def is_quarter_end(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_quarter_end(table, schema, column)
    
    @record_call
    async def days_in_month(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.days_in_month(table, schema, column)
    
    @record_call
    async def is_weekend(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_weekend(table, schema, column)


    @record_call
    async def is_weekday(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_weekday(table, schema, column)


    @record_call
    async def is_business_day(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.is_business_day(table, schema, column)


    @record_call
    async def week_of_month(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.week_of_month(table, schema, column)
    
    
    @record_call
    async def timestamp(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.timestamp(table, schema, column)


    @record_call
    async def from_timestamp(self, column: str = None,  value: float = None, tz: str = None,):
        
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.fromtimestamp( table,  schema, column, value,    tz )
    
    
    
    @record_call
    async def strftime(self, column: str, fmt: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.strftime(table, schema, column, fmt)


    @record_call
    async def strptime(self, column: str, fmt: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.strptime(table, schema, column, fmt)
    
    
    @record_call
    async def add(self, column: str, interval: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.add_timedelta(table, schema, column, interval)


    @record_call
    async def sub(self, column: str, interval: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.sub_timedelta(table, schema, column, interval)


    @record_call
    async def replace(self, column: str, **kwargs):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.replace(table, schema, column, **kwargs)


    @record_call
    async def normalize(self, column: str):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.normalize(table, schema, column)

    # ── Wave 1: names, durations, parsing, filtering ──────────────
    @record_call
    async def day_name(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.day_name(table, schema, column)

    @record_call
    async def month_name(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.month_name(table, schema, column)

    @record_call
    async def diff(self, col1: str, col2: str, unit: str = "day",
                   target_col: str = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.diff(table, schema, col1, col2, unit,
                              target_col=target_col)

    @record_call
    async def to_datetime(self, column: str, format: str = None,
                          errors: str = "raise", unit: str = None,
                          tz: str = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.to_datetime(table, schema, column, format,
                                     errors, unit, tz)

    @record_call
    async def between(self, column: str, start: str, end: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        start_norm = _coerce_datetime_literal(start, "start")
        if isinstance(start_norm, dict):
            return start_norm
        end_norm = _coerce_datetime_literal(end, "end")
        if isinstance(end_norm, dict):
            return end_norm
        return await ops.between(table, schema, column, start_norm, end_norm)

    @record_call
    async def before(self, column: str, value: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        norm = _coerce_datetime_literal(value, "value")
        if isinstance(norm, dict):
            return norm
        return await ops.before(table, schema, column, norm)

    @record_call
    async def after(self, column: str, value: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        norm = _coerce_datetime_literal(value, "value")
        if isinstance(norm, dict):
            return norm
        return await ops.after(table, schema, column, norm)

    @record_call
    async def select_year(self, column: str, values: Any) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        coerced = _coerce_int_list(values, "values")
        if isinstance(coerced, dict):
            return coerced
        return await ops.select_year(table, schema, column, coerced)

    @record_call
    async def select_month(self, column: str, values: Any) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        coerced = _coerce_int_list(values, "values")
        if isinstance(coerced, dict):
            return coerced
        for month in coerced:
            if month < 1 or month > 12:
                return fail(f"months must be 1..12, got {values!r}")
        return await ops.select_month(table, schema, column, coerced)

    # ── Wave 2: resampling + frequency conversion ─────────────
    # ponytail: deep_cache=True (moved with the op) — aggregated/grid tables
    # are persisted for replay, unlike the audit-only datetime one-shots above.
    @record_call(deep_cache=True)
    async def resample(self, column: str, freq: str, agg: Any = "count",
                       value_columns: Any = None, group_by: Any = None,
                       label: str = "left", closed: str = "left") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.resample(table, schema, column, freq, agg,
                                  value_columns, group_by, label, closed)

    @record_call(deep_cache=True)
    async def asfreq(self, column: str, freq: str,
                     method: str = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.asfreq(table, schema, column, freq, method)



DateTimeAccessor = DateTimeOrchestrator
