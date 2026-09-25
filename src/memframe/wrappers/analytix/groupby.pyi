from typing import Any, Dict, List, Optional, Union


class GroupBy:
    """
    Unified GroupBy object that supports statistics (agg, mean, sum, ...),
    cumulative operations (cumsum, cummean, ...), and window operations
    (rolling, expanding, ewm).

    Created by ``ContextManager.groupby(*columns)``.
    """

    _stats: Any
    _cum: Any
    _window: Any
    group_cols: List[str]

    def __init__(self, stats_builder: Any, cum_builder: Any, window_builder: Any) -> None: ...

    async def aagg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def agg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def asum(self, column: str) -> Dict[str, Any]: ...
    def sum(self, column: str) -> Dict[str, Any]: ...

    async def amean(self, column: str) -> Dict[str, Any]: ...
    def mean(self, column: str) -> Dict[str, Any]: ...

    async def amin(self, column: str) -> Dict[str, Any]: ...
    def min(self, column: str) -> Dict[str, Any]: ...

    async def amax(self, column: str) -> Dict[str, Any]: ...
    def max(self, column: str) -> Dict[str, Any]: ...

    async def acount(self, column: str) -> Dict[str, Any]: ...
    def count(self, column: str) -> Dict[str, Any]: ...

    async def amedian(self, column: str) -> Dict[str, Any]: ...
    def median(self, column: str) -> Dict[str, Any]: ...

    async def amode(self, column: str) -> Dict[str, Any]: ...
    def mode(self, column: str) -> Dict[str, Any]: ...

    async def astd(self, column: str) -> Dict[str, Any]: ...
    def std(self, column: str) -> Dict[str, Any]: ...

    async def avar(self, column: str) -> Dict[str, Any]: ...
    def var(self, column: str) -> Dict[str, Any]: ...

    async def asem(self, column: str) -> Dict[str, Any]: ...
    def sem(self, column: str) -> Dict[str, Any]: ...

    async def anunique(self, column: str) -> Dict[str, Any]: ...
    def nunique(self, column: str) -> Dict[str, Any]: ...

    async def arange(self, column: str) -> Dict[str, Any]: ...
    def range(self, column: str) -> Dict[str, Any]: ...

    async def aproduct(self, column: str) -> Dict[str, Any]: ...
    def product(self, column: str) -> Dict[str, Any]: ...

    async def aevent_rate(
        self,
        datetime_col: str,
        unit: str = ...,
        new_table: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def event_rate(
        self,
        datetime_col: str,
        unit: str = ...,
        new_table: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    async def acumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def cumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
        map_feature: bool = ...,
    ) -> Dict[str, Any]: ...

    def __repr__(self) -> str: ...
