# stats_wrapper.pyi

from typing import Any, Dict, List


class StatsWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    async def acount(self, column: str) -> Dict[str, Any]: ...
    def count(self, column: str) -> Dict[str, Any]: ...

    async def amin(self, column: str) -> Dict[str, Any]: ...
    def min(self, column: str) -> Dict[str, Any]: ...

    async def amax(self, column: str) -> Dict[str, Any]: ...
    def max(self, column: str) -> Dict[str, Any]: ...

    async def amode(
        self,
        column: str,
        top_n: int = ...,
    ) -> Dict[str, Any]: ...
    def mode(
        self,
        column: str,
        top_n: int = ...,
    ) -> Dict[str, Any]: ...

    async def aunique(self, column: str) -> Dict[str, Any]: ...
    def unique(self, column: str) -> Dict[str, Any]: ...

    async def anunique(self, column: str) -> Dict[str, Any]: ...
    def nunique(self, column: str) -> Dict[str, Any]: ...

    async def avalue_counts(
        self,
        column: str,
        top_n: int = ...,
    ) -> Dict[str, Any]: ...
    def value_counts(
        self,
        column: str,
        top_n: int = ...,
    ) -> Dict[str, Any]: ...

    async def amean(self, column: str) -> Dict[str, Any]: ...
    def mean(self, column: str) -> Dict[str, Any]: ...

    async def amedian(self, column: str) -> Dict[str, Any]: ...
    def median(self, column: str) -> Dict[str, Any]: ...

    async def asum(self, column: str) -> Dict[str, Any]: ...
    def sum(self, column: str) -> Dict[str, Any]: ...

    async def astd(self, column: str) -> Dict[str, Any]: ...
    def std(self, column: str) -> Dict[str, Any]: ...

    async def avar(self, column: str) -> Dict[str, Any]: ...
    def var(self, column: str) -> Dict[str, Any]: ...

    async def asem(self, column: str) -> Dict[str, Any]: ...
    def sem(self, column: str) -> Dict[str, Any]: ...

    async def amad(self, column: str) -> Dict[str, Any]: ...
    def mad(self, column: str) -> Dict[str, Any]: ...

    async def aiqr(self, column: str) -> Dict[str, Any]: ...
    def iqr(self, column: str) -> Dict[str, Any]: ...

    async def arange(self, column: str) -> Dict[str, Any]: ...
    def range(self, column: str) -> Dict[str, Any]: ...

    async def askew(self, column: str) -> Dict[str, Any]: ...
    def skew(self, column: str) -> Dict[str, Any]: ...

    async def akurtosis(self, column: str) -> Dict[str, Any]: ...
    def kurtosis(self, column: str) -> Dict[str, Any]: ...

    async def aentropy(self, column: str) -> Dict[str, Any]: ...
    def entropy(self, column: str) -> Dict[str, Any]: ...

    async def aquantile(
        self,
        column: str,
        q: List[float] = ...,
    ) -> Dict[str, Any]: ...
    def quantile(
        self,
        column: str,
        q: List[float] = ...,
    ) -> Dict[str, Any]: ...

    async def aautocorr(
        self,
        column: str,
        lag: int = ...,
    ) -> Dict[str, Any]: ...
    def autocorr(
        self,
        column: str,
        lag: int = ...,
    ) -> Dict[str, Any]: ...

    async def acoefficient_of_variation(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def coefficient_of_variation(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aoutliers_iqr(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def outliers_iqr(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aoutliers_zscore(
        self,
        column: str,
        threshold: float = ...,
    ) -> Dict[str, Any]: ...
    def outliers_zscore(
        self,
        column: str,
        threshold: float = ...,
    ) -> Dict[str, Any]: ...

    async def acorr(
        self,
        columns: List[str] = ...,
    ) -> Dict[str, Any]: ...
    def corr(
        self,
        columns: List[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acov(
        self,
        columns: List[str] = ...,
    ) -> Dict[str, Any]: ...
    def cov(
        self,
        columns: List[str] = ...,
    ) -> Dict[str, Any]: ...

    async def aproportions(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def proportions(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def achi_square(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...
    def chi_square(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...

    async def acramers_v(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...
    def cramers_v(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...

    async def atheil_u(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...
    def theil_u(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...

    async def amutual_information(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...
    def mutual_information(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...


    async def adatetime_diff(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def datetime_diff(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def atime_delta_stats(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def time_delta_stats(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aevent_rate(
        self,
        column: str,
        unit: str = ...,
    ) -> Dict[str, Any]: ...
    def event_rate(
        self,
        column: str,
        unit: str = ...,
    ) -> Dict[str, Any]: ...

    async def atime_unit_counts(
        self,
        column: str,
        unit: str = ...,
    ) -> Dict[str, Any]: ...
    def time_unit_counts(
        self,
        column: str,
        unit: str = ...,
    ) -> Dict[str, Any]: ...

    async def aweekday_weekend_counts(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def weekday_weekend_counts(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aholiday_counts(
        self,
        column: str,
    ) -> Dict[str, Any]: ...
    def holiday_counts(
        self,
        column: str,
    ) -> Dict[str, Any]: ...


StatsAccessor = StatsWrapper