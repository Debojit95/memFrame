# interface

from typing import Any, Dict, List, Literal, Optional, Union, overload


OrderBy = Optional[Union[str, List[str]]]
RollingFunc = Literal[
    "sum",
    "mean",
    "min",
    "max",
    "count",
    "std",
    "var",
    "quantile",
    "sem",
    "rank",
    "nunique",
    "first",
    "last",
    "median",
    "mode",
]
ExpandingFunc = Literal[
    "sum",
    "mean",
    "min",
    "max",
    "count",
    "std",
    "var",
    "quantile",
    "sem",
    "rank",
    "nunique",
    "first",
    "last",
    "median",
    "mode",
]
EWMFunc = Literal["mean", "sum", "std", "var"]
RollingFuncArg = Union[RollingFunc, List[RollingFunc]]
ExpandingFuncArg = Union[ExpandingFunc, List[ExpandingFunc]]
EWMFuncArg = Union[EWMFunc, List[EWMFunc]]


class RollingWindowBuilderWrapper:

    async def aapply(
        self,
        func: RollingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    def apply(
        self,
        func: RollingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    async def asum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def sum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def mean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amin(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def min(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amax(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def max(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def acount(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def count(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def astd(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def std(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def avar(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def var(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def aquantile(
        self,
        q: float = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def quantile(
        self,
        q: float = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def asem(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def sem(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def arank(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def rank(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def anunique(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def nunique(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def afirst(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def first(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def alast(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def last(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...


class ExpandingWindowBuilderWrapper:

    async def aapply(
        self,
        func: ExpandingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    def apply(
        self,
        func: ExpandingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    async def asum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def sum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def mean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amin(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def min(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amax(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def max(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def acount(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def count(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def astd(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def std(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def avar(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def var(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def aquantile(
        self,
        q: float = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def quantile(
        self,
        q: float = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def asem(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def sem(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def arank(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def rank(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def anunique(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def nunique(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def afirst(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def first(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def alast(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def last(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...


class EWMWindowBuilderWrapper:

    async def aapply(
        self,
        func: EWMFuncArg = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def apply(
        self,
        func: EWMFuncArg = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def amean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def mean(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def asum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def sum(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def astd(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def std(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    async def avar(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def var(
        self,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...


class WindowBuilderWrapper:

    column: str

    def rolling(
        self,
        window: int,
    ) -> RollingWindowBuilderWrapper: ...

    def expanding(
        self,
        min_periods: int = ...,
    ) -> ExpandingWindowBuilderWrapper: ...

    def ewm(
        self,
        com: Optional[float] = ...,
        span: Optional[float] = ...,
        halflife: Optional[float] = ...,
        alpha: Optional[float] = ...,
        adjust: bool = ...,
        ignore_na: bool = ...,
        min_periods: int = ...,
    ) -> EWMWindowBuilderWrapper: ...


class WindowWrapper:

    def __init__(self, memframe_ops_instance) -> None: ...

    @classmethod
    def replay_create(
        cls,
        memframe,
        data_id: str,
    ) -> "WindowWrapper": ...

    @classmethod
    def from_context(
        cls,
        memframe,
        data_id,
    ) -> "WindowWrapper": ...

    async def arolling(
        self,
        column: str,
        window: int,
        func: RollingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    @overload
    def rolling(
        self,
        column: str,
        window: int,
        func: None = ...,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> RollingWindowBuilderWrapper: ...

    @overload
    def rolling(
        self,
        column: str,
        window: int,
        func: RollingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
    ) -> Dict[str, Any]: ...

    async def aexpanding(
        self,
        column: str,
        func: ExpandingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
        min_periods: int = ...,
    ) -> Dict[str, Any]: ...

    @overload
    def expanding(
        self,
        column: str,
        func: None = ...,
        order_by: OrderBy = ...,
        q: float = ...,
        min_periods: int = ...,
    ) -> ExpandingWindowBuilderWrapper: ...

    @overload
    def expanding(
        self,
        column: str,
        func: ExpandingFuncArg,
        order_by: OrderBy = ...,
        q: float = ...,
        min_periods: int = ...,
    ) -> Dict[str, Any]: ...

    async def aewm(
        self,
        column: str,
        com: Optional[float] = ...,
        span: Optional[float] = ...,
        halflife: Optional[float] = ...,
        alpha: Optional[float] = ...,
        adjust: bool = ...,
        ignore_na: bool = ...,
        min_periods: int = ...,
        func: EWMFuncArg = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def ewm(
        self,
        column: str,
        com: Optional[float] = ...,
        span: Optional[float] = ...,
        halflife: Optional[float] = ...,
        alpha: Optional[float] = ...,
        adjust: bool = ...,
        ignore_na: bool = ...,
        min_periods: int = ...,
        func: EWMFuncArg = ...,
        order_by: OrderBy = ...,
    ) -> Dict[str, Any]: ...

    def on(
        self,
        column: str,
    ) -> WindowBuilderWrapper: ...


WindowAccessor = WindowWrapper
