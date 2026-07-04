# stats_wrapper.py

from typing import Any, Dict, List

from src.core.orchestrator.analytix.stats import StatsOrchestrator
from src.utils.async_sync import async_to_sync


class StatsWrapper(StatsOrchestrator):
    """
    Public sync/async wrapper over StatsOrchestrator.

    Supports:
        - sync  : .mean(...)
        - async : .amean(...)
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the stats wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # Unified APIs
    # ------------------------------------------------------------------

    async def acount(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute count of non-null values."""
        return await super().count(column)

    @async_to_sync
    async def count(self, column: str) -> Dict[str, Any]:
        """Synchronously compute count of non-null values."""
        return await self.acount(column)

    async def amin(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute minimum value."""
        return await super().min(column)

    @async_to_sync
    async def min(self, column: str) -> Dict[str, Any]:
        """Synchronously compute minimum value."""
        return await self.amin(column)

    async def amax(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute maximum value."""
        return await super().max(column)

    @async_to_sync
    async def max(self, column: str) -> Dict[str, Any]:
        """Synchronously compute maximum value."""
        return await self.amax(column)

    async def amode(self, column: str, top_n: int = 1) -> Dict[str, Any]:
        """Asynchronously compute mode values."""
        return await super().mode(column, top_n)

    @async_to_sync
    async def mode(self, column: str, top_n: int = 1) -> Dict[str, Any]:
        """Synchronously compute mode values."""
        return await self.amode(column, top_n)

    async def aunique(self, column: str) -> Dict[str, Any]:
        """Asynchronously return unique values."""
        return await super().unique(column)

    @async_to_sync
    async def unique(self, column: str) -> Dict[str, Any]:
        """Synchronously return unique values."""
        return await self.aunique(column)

    async def anunique(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute number of unique values."""
        return await super().nunique(column)

    @async_to_sync
    async def nunique(self, column: str) -> Dict[str, Any]:
        """Synchronously compute number of unique values."""
        return await self.anunique(column)

    async def avalue_counts(
        self,
        column: str,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """Asynchronously compute value counts for a column."""
        return await super().value_counts(column, top_n)

    @async_to_sync
    async def value_counts(
        self,
        column: str,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """Synchronously compute value counts for a column."""
        return await self.avalue_counts(column, top_n)

    async def amean(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute mean value."""
        return await super().mean(column)

    @async_to_sync
    async def mean(self, column: str) -> Dict[str, Any]:
        """Synchronously compute mean value."""
        return await self.amean(column)

    async def amedian(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute median value."""
        return await super().median(column)

    @async_to_sync
    async def median(self, column: str) -> Dict[str, Any]:
        """Synchronously compute median value."""
        return await self.amedian(column)

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------

    async def asum(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute sum."""
        return await super().sum(column)

    @async_to_sync
    async def sum(self, column: str) -> Dict[str, Any]:
        """Synchronously compute sum."""
        return await self.asum(column)

    async def astd(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute standard deviation."""
        return await super().std(column)

    @async_to_sync
    async def std(self, column: str) -> Dict[str, Any]:
        """Synchronously compute standard deviation."""
        return await self.astd(column)

    async def avar(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute variance."""
        return await super().var(column)

    @async_to_sync
    async def var(self, column: str) -> Dict[str, Any]:
        """Synchronously compute variance."""
        return await self.avar(column)

    async def asem(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute standard error of mean."""
        return await super().sem(column)

    @async_to_sync
    async def sem(self, column: str) -> Dict[str, Any]:
        """Synchronously compute standard error of mean."""
        return await self.asem(column)

    async def amad(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute mean absolute deviation."""
        return await super().mad(column)

    @async_to_sync
    async def mad(self, column: str) -> Dict[str, Any]:
        """Synchronously compute mean absolute deviation."""
        return await self.amad(column)

    async def aiqr(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute interquartile range."""
        return await super().iqr(column)

    @async_to_sync
    async def iqr(self, column: str) -> Dict[str, Any]:
        """Synchronously compute interquartile range."""
        return await self.aiqr(column)

    async def arange(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute numeric range (max-min)."""
        return await super().range(column)

    @async_to_sync
    async def range(self, column: str) -> Dict[str, Any]:
        """Synchronously compute numeric range (max-min)."""
        return await self.arange(column)

    async def askew(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute skewness."""
        return await super().skew(column)

    @async_to_sync
    async def skew(self, column: str) -> Dict[str, Any]:
        """Synchronously compute skewness."""
        return await self.askew(column)

    async def akurtosis(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute kurtosis."""
        return await super().kurtosis(column)

    @async_to_sync
    async def kurtosis(self, column: str) -> Dict[str, Any]:
        """Synchronously compute kurtosis."""
        return await self.akurtosis(column)

    async def aentropy(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute entropy."""
        return await super().entropy(column)

    @async_to_sync
    async def entropy(self, column: str) -> Dict[str, Any]:
        """Synchronously compute entropy."""
        return await self.aentropy(column)

    async def aquantile(
        self,
        column: str,
        q: List[float] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute quantiles."""
        return await super().quantile(column, q)

    @async_to_sync
    async def quantile(
        self,
        column: str,
        q: List[float] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute quantiles."""
        return await self.aquantile(column, q)

    async def aautocorr(
        self,
        column: str,
        lag: int = 1,
    ) -> Dict[str, Any]:
        """Asynchronously compute autocorrelation at a lag."""
        return await super().autocorr(column, lag)

    @async_to_sync
    async def autocorr(
        self,
        column: str,
        lag: int = 1,
    ) -> Dict[str, Any]:
        """Synchronously compute autocorrelation at a lag."""
        return await self.aautocorr(column, lag)

    async def acoefficient_of_variation(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Asynchronously compute coefficient of variation."""
        return await super().coefficient_of_variation(column)

    @async_to_sync
    async def coefficient_of_variation(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Synchronously compute coefficient of variation."""
        return await self.acoefficient_of_variation(column)

    async def aoutliers_iqr(self, column: str) -> Dict[str, Any]:
        """Asynchronously detect outliers using IQR rule."""
        return await super().outliers_iqr(column)

    @async_to_sync
    async def outliers_iqr(self, column: str) -> Dict[str, Any]:
        """Synchronously detect outliers using IQR rule."""
        return await self.aoutliers_iqr(column)

    async def aoutliers_zscore(
        self,
        column: str,
        threshold: float = 3.0,
    ) -> Dict[str, Any]:
        """Asynchronously detect outliers using z-score threshold."""
        return await super().outliers_zscore(column, threshold)

    @async_to_sync
    async def outliers_zscore(
        self,
        column: str,
        threshold: float = 3.0,
    ) -> Dict[str, Any]:
        """Synchronously detect outliers using z-score threshold."""
        return await self.aoutliers_zscore(column, threshold)

    # ------------------------------------------------------------------
    # DataFrame-returning methods
    # ------------------------------------------------------------------

    async def acorr(
        self,
        columns: List[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute correlation matrix."""
        return await super().corr(columns)

    @async_to_sync
    async def corr(
        self,
        columns: List[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute correlation matrix."""
        return await self.acorr(columns)

    async def acov(
        self,
        columns: List[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute covariance matrix."""
        return await super().cov(columns)

    @async_to_sync
    async def cov(
        self,
        columns: List[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute covariance matrix."""
        return await self.acov(columns)

    # ------------------------------------------------------------------
    # Categorical
    # ------------------------------------------------------------------

    async def aproportions(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute categorical proportions."""
        return await super().proportions(column)

    @async_to_sync
    async def proportions(self, column: str) -> Dict[str, Any]:
        """Synchronously compute categorical proportions."""
        return await self.aproportions(column)

    async def achi_square(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Asynchronously compute chi-square association test."""
        return await super().chi_square(column1, column2)

    @async_to_sync
    async def chi_square(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Synchronously compute chi-square association test."""
        return await self.achi_square(column1, column2)

    async def acramers_v(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Asynchronously compute Cramer's V association score."""
        return await super().cramers_v(column1, column2)

    @async_to_sync
    async def cramers_v(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Synchronously compute Cramer's V association score."""
        return await self.acramers_v(column1, column2)

    async def atheil_u(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Asynchronously compute Theil's U association score."""
        return await super().theil_u(column1, column2)

    @async_to_sync
    async def theil_u(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Synchronously compute Theil's U association score."""
        return await self.atheil_u(column1, column2)

    async def amutual_information(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Asynchronously compute mutual information."""
        return await super().mutual_information(column1, column2)

    @async_to_sync
    async def mutual_information(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Synchronously compute mutual information."""
        return await self.amutual_information(column1, column2)

    
    # ------------------------------------------------------------------
    # Datetime
    # ------------------------------------------------------------------

    async def adatetime_diff(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute consecutive datetime differences."""
        return await super().datetime_diff(column)

    @async_to_sync
    async def datetime_diff(self, column: str) -> Dict[str, Any]:
        """Synchronously compute consecutive datetime differences."""
        return await self.adatetime_diff(column)

    async def atime_delta_stats(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute statistics over datetime deltas."""
        return await super().time_delta_stats(column)

    @async_to_sync
    async def time_delta_stats(self, column: str) -> Dict[str, Any]:
        """Synchronously compute statistics over datetime deltas."""
        return await self.atime_delta_stats(column)

    async def aevent_rate(
        self,
        column: str,
        unit: str = "day",
    ) -> Dict[str, Any]:
        """Asynchronously compute event rate by time unit."""
        return await super().event_rate(column, unit)

    @async_to_sync
    async def event_rate(
        self,
        column: str,
        unit: str = "day",
    ) -> Dict[str, Any]:
        """Synchronously compute event rate by time unit."""
        return await self.aevent_rate(column, unit)

    async def atime_unit_counts(
        self,
        column: str,
        unit: str = "day",
    ) -> Dict[str, Any]:
        """Asynchronously count events per time unit."""
        return await super().time_unit_counts(column, unit)

    @async_to_sync
    async def time_unit_counts(
        self,
        column: str,
        unit: str = "day",
    ) -> Dict[str, Any]:
        """Synchronously count events per time unit."""
        return await self.atime_unit_counts(column, unit)

    async def aweekday_weekend_counts(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Asynchronously count weekday versus weekend events."""
        return await super().weekday_weekend_counts(column)

    @async_to_sync
    async def weekday_weekend_counts(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Synchronously count weekday versus weekend events."""
        return await self.aweekday_weekend_counts(column)

    async def aholiday_counts(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Asynchronously count events on holidays."""
        return await super().holiday_counts(column)

    @async_to_sync
    async def holiday_counts(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Synchronously count events on holidays."""
        return await self.aholiday_counts(column)


StatsAccessor = StatsWrapper
