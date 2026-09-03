from typing import Any, Dict

from memframe.core.orchestrator.analytix.datetime import DateTimeOrchestrator
from memframe.utils.async_sync import async_to_sync


class DateTimeWrapper(DateTimeOrchestrator):
    """Wrapper around `DateTimeOrchestrator` with async/sync methods."""

    def __init__(self, *args, **kwargs):
        """Initialize the datetime wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    # ------------------------------------------------------------------
    # extract helper
    # ------------------------------------------------------------------
    async def aextract(self, column: str, field: str) -> Dict[str, Any]:
        """Asynchronously extract a datetime field from a column."""
        return await super()._extract(column, field)

    @async_to_sync
    async def extract(self, column: str, field: str) -> Dict[str, Any]:
        """Synchronously extract a datetime field from a column."""
        return await self.aextract(column, field)

    # ------------------------------------------------------------------
    # date component extraction
    # ------------------------------------------------------------------
    async def ayear(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract year values from a datetime column."""
        return await super().year(column)

    @async_to_sync
    async def year(self, column: str) -> Dict[str, Any]:
        """Synchronously extract year values from a datetime column."""
        return await self.ayear(column)

    async def amonth(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract month values from a datetime column."""
        return await super().month(column)

    @async_to_sync
    async def month(self, column: str) -> Dict[str, Any]:
        """Synchronously extract month values from a datetime column."""
        return await self.amonth(column)

    async def aday(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract day-of-month values."""
        return await super().day(column)

    @async_to_sync
    async def day(self, column: str) -> Dict[str, Any]:
        """Synchronously extract day-of-month values."""
        return await self.aday(column)

    async def ahour(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract hour values from datetime entries."""
        return await super().hour(column)

    @async_to_sync
    async def hour(self, column: str) -> Dict[str, Any]:
        """Synchronously extract hour values from datetime entries."""
        return await self.ahour(column)

    async def aminute(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract minute values from datetime entries."""
        return await super().minute(column)

    @async_to_sync
    async def minute(self, column: str) -> Dict[str, Any]:
        """Synchronously extract minute values from datetime entries."""
        return await self.aminute(column)

    async def asecond(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract second values from datetime entries."""
        return await super().second(column)

    @async_to_sync
    async def second(self, column: str) -> Dict[str, Any]:
        """Synchronously extract second values from datetime entries."""
        return await self.asecond(column)

    async def adayofweek(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract day-of-week values."""
        return await super().dayofweek(column)

    @async_to_sync
    async def dayofweek(self, column: str) -> Dict[str, Any]:
        """Synchronously extract day-of-week values."""
        return await self.adayofweek(column)

    async def adayofyear(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract day-of-year values."""
        return await super().dayofyear(column)

    @async_to_sync
    async def dayofyear(self, column: str) -> Dict[str, Any]:
        """Synchronously extract day-of-year values."""
        return await self.adayofyear(column)

    async def aweek(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract week values from a datetime column."""
        return await super().week(column)

    @async_to_sync
    async def week(self, column: str) -> Dict[str, Any]:
        """Synchronously extract week values from a datetime column."""
        return await self.aweek(column)

    async def aquarter(self, column: str) -> Dict[str, Any]:
        """Asynchronously extract quarter values from a datetime column."""
        return await super().quarter(column)

    @async_to_sync
    async def quarter(self, column: str) -> Dict[str, Any]:
        """Synchronously extract quarter values from a datetime column."""
        return await self.aquarter(column)

    # ------------------------------------------------------------------
    # rounding operations
    # ------------------------------------------------------------------
    async def afloor(self, column: str, unit: str) -> Dict[str, Any]:
        """Asynchronously floor datetime values to a given unit."""
        return await super().floor(column, unit)

    @async_to_sync
    async def floor(self, column: str, unit: str) -> Dict[str, Any]:
        """Synchronously floor datetime values to a given unit."""
        return await self.afloor(column, unit)

    async def aceil(self, column: str, unit: str) -> Dict[str, Any]:
        """Asynchronously ceil datetime values to a given unit."""
        return await super().ceil(column, unit)

    @async_to_sync
    async def ceil(self, column: str, unit: str) -> Dict[str, Any]:
        """Synchronously ceil datetime values to a given unit."""
        return await self.aceil(column, unit)

    async def around(self, column: str, unit: str) -> Dict[str, Any]:
        """Asynchronously round datetime values to a given unit."""
        return await super().round(column, unit)

    @async_to_sync
    async def round(self, column: str, unit: str) -> Dict[str, Any]:
        """Synchronously round datetime values to a given unit."""
        return await self.around(column, unit)

    # ------------------------------------------------------------------
    # timezone
    # ------------------------------------------------------------------
    async def atz_localize(
        self,
        column: str,
        tz: str | None,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
    ) -> Dict[str, Any]:
        """Asynchronously localize naive datetimes to a timezone."""
        return await super().tz_localize(
            column,
            tz,
            ambiguous,
            nonexistent,
        )

    @async_to_sync
    async def tz_localize(
        self,
        column: str,
        tz: str | None,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
    ) -> Dict[str, Any]:
        """Synchronously localize naive datetimes to a timezone."""
        return await self.atz_localize(
            column,
            tz,
            ambiguous,
            nonexistent,
        )

    async def atz_convert(
        self,
        column: str,
        tz: str | None,
    ) -> Dict[str, Any]:
        """Asynchronously convert timezone-aware datetimes to another zone."""
        return await super().tz_convert(column, tz)

    @async_to_sync
    async def tz_convert(
        self,
        column: str,
        tz: str | None,
    ) -> Dict[str, Any]:
        """Synchronously convert timezone-aware datetimes to another zone."""
        return await self.atz_convert(column, tz)

    # ------------------------------------------------------------------
    # boolean datetime checks
    # ------------------------------------------------------------------
    async def ais_month_start(self, column: str):
        """Asynchronously flag whether each date is month start."""
        return await super().is_month_start(column)

    @async_to_sync
    async def is_month_start(self, column: str):
        """Synchronously flag whether each date is month start."""
        return await self.ais_month_start(column)

    async def ais_month_end(self, column: str):
        """Asynchronously flag whether each date is month end."""
        return await super().is_month_end(column)

    @async_to_sync
    async def is_month_end(self, column: str):
        """Synchronously flag whether each date is month end."""
        return await self.ais_month_end(column)

    async def ais_year_start(self, column: str):
        """Asynchronously flag whether each date is year start."""
        return await super().is_year_start(column)

    @async_to_sync
    async def is_year_start(self, column: str):
        """Synchronously flag whether each date is year start."""
        return await self.ais_year_start(column)

    async def ais_year_end(self, column: str):
        """Asynchronously flag whether each date is year end."""
        return await super().is_year_end(column)

    @async_to_sync
    async def is_year_end(self, column: str):
        """Synchronously flag whether each date is year end."""
        return await self.ais_year_end(column)

    async def ais_quarter_start(self, column: str):
        """Asynchronously flag whether each date is quarter start."""
        return await super().is_quarter_start(column)

    @async_to_sync
    async def is_quarter_start(self, column: str):
        """Synchronously flag whether each date is quarter start."""
        return await self.ais_quarter_start(column)

    async def ais_quarter_end(self, column: str):
        """Asynchronously flag whether each date is quarter end."""
        return await super().is_quarter_end(column)

    @async_to_sync
    async def is_quarter_end(self, column: str):
        """Synchronously flag whether each date is quarter end."""
        return await self.ais_quarter_end(column)

    async def adays_in_month(self, column: str):
        """Asynchronously compute number of days in each month."""
        return await super().days_in_month(column)

    @async_to_sync
    async def days_in_month(self, column: str):
        """Synchronously compute number of days in each month."""
        return await self.adays_in_month(column)

    async def ais_weekend(self, column: str):
        """Asynchronously flag whether each date is a weekend."""
        return await super().is_weekend(column)

    @async_to_sync
    async def is_weekend(self, column: str):
        """Synchronously flag whether each date is a weekend."""
        return await self.ais_weekend(column)

    async def ais_weekday(self, column: str):
        """Asynchronously flag whether each date is a weekday."""
        return await super().is_weekday(column)

    @async_to_sync
    async def is_weekday(self, column: str):
        """Synchronously flag whether each date is a weekday."""
        return await self.ais_weekday(column)

    async def ais_business_day(self, column: str):
        """Asynchronously flag whether each date is a business day."""
        return await super().is_business_day(column)

    @async_to_sync
    async def is_business_day(self, column: str):
        """Synchronously flag whether each date is a business day."""
        return await self.ais_business_day(column)

    async def aweek_of_month(self, column: str):
        """Asynchronously compute week-of-month values."""
        return await super().week_of_month(column)

    @async_to_sync
    async def week_of_month(self, column: str):
        """Synchronously compute week-of-month values."""
        return await self.aweek_of_month(column)

    # ------------------------------------------------------------------
    # timestamp conversion
    # ------------------------------------------------------------------
    async def atimestamp(self, column: str):
        """Asynchronously convert datetime values to Unix timestamps."""
        return await super().timestamp(column)

    @async_to_sync
    async def timestamp(self, column: str):
        """Synchronously convert datetime values to Unix timestamps."""
        return await self.atimestamp(column)

    async def afrom_timestamp(
        self,
        column: str = None,
        value: float = None,
        tz: str = None,
    ):
        """Asynchronously convert Unix timestamps to datetime values."""
        return await super().from_timestamp(column, value, tz)

    @async_to_sync
    async def from_timestamp(
        self,
        column: str = None,
        value: float = None,
        tz: str = None,
    ):
        """Synchronously convert Unix timestamps to datetime values."""
        return await self.afrom_timestamp(column, value, tz)

    # ------------------------------------------------------------------
    # formatting / parsing
    # ------------------------------------------------------------------
    async def astrftime(self, column: str, fmt: str):
        """Asynchronously format datetime values to strings."""
        return await super().strftime(column, fmt)

    @async_to_sync
    async def strftime(self, column: str, fmt: str):
        """Synchronously format datetime values to strings."""
        return await self.astrftime(column, fmt)

    async def astrptime(self, column: str, fmt: str):
        """Asynchronously parse datetime strings using a format."""
        return await super().strptime(column, fmt)

    @async_to_sync
    async def strptime(self, column: str, fmt: str):
        """Synchronously parse datetime strings using a format."""
        return await self.astrptime(column, fmt)

    # ------------------------------------------------------------------
    # timedelta
    # ------------------------------------------------------------------
    async def aadd(self, column: str, interval: str):
        """Asynchronously add a time interval to datetime values."""
        return await super().add(column, interval)

    @async_to_sync
    async def add(self, column: str, interval: str):
        """Synchronously add a time interval to datetime values."""
        return await self.aadd(column, interval)

    async def asub(self, column: str, interval: str):
        """Asynchronously subtract a time interval from datetime values."""
        return await super().sub(column, interval)

    @async_to_sync
    async def sub(self, column: str, interval: str):
        """Synchronously subtract a time interval from datetime values."""
        return await self.asub(column, interval)

    # ------------------------------------------------------------------
    # replacement / normalization
    # ------------------------------------------------------------------
    async def areplace(self, column: str, **kwargs):
        """Asynchronously replace components of datetime values."""
        return await super().replace(column, **kwargs)

    @async_to_sync
    async def replace(self, column: str, **kwargs):
        """Synchronously replace components of datetime values."""
        return await self.areplace(column, **kwargs)

    async def anormalize(self, column: str):
        """Asynchronously normalize datetimes to midnight."""
        return await super().normalize(column)

    @async_to_sync
    async def normalize(self, column: str):
        """Synchronously normalize datetimes to midnight."""
        return await self.anormalize(column)
