from typing import Any, Dict

from memframe.core.analytix.datetime import DatetimeOps
from memframe.cache import record_call


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
    
    
    
DateTimeAccessor = DateTimeOrchestrator
