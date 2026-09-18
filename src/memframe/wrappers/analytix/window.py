# wrapper

from typing import Any, Dict, List, Optional, Union, overload

from memframe.core.orchestrator.analytix.window import WindowOrchestrator
from memframe.utils.async_sync import async_to_sync


class WindowBuilderWrapper:
    """
    Fluent builder wrapper for rolling/expanding/ewm APIs.

    Example:
        ops.window.on("sales").rolling(7).mean(order_by="date")

    Supports:
      - sync  : .mean(...)
      - async : .amean(...)
    """

    def __init__(
        self,
        parent: WindowOrchestrator,
        column: str,
    ):
        """Initialize a fluent window builder for a target column."""
        self._parent = parent
        self.column = column

    # ------------------------------------------------------------------
    # Rolling Builder
    # ------------------------------------------------------------------
    def rolling(
        self,
        window: int,
    ) -> "RollingWindowBuilderWrapper":
        """Create a rolling window builder for the selected column."""
        return RollingWindowBuilderWrapper(
            parent=self._parent,
            column=self.column,
            window=window,
        )

    # ------------------------------------------------------------------
    # Expanding Builder
    # ------------------------------------------------------------------
    def expanding(
        self,
        min_periods: int = 1,
    ) -> "ExpandingWindowBuilderWrapper":
        """Create an expanding window builder for the selected column."""
        return ExpandingWindowBuilderWrapper(
            parent=self._parent,
            column=self.column,
            min_periods=min_periods,
        )

    # ------------------------------------------------------------------
    # EWM Builder
    # ------------------------------------------------------------------
    def ewm(
        self,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
    ) -> "EWMWindowBuilderWrapper":
        """Create an exponentially weighted window builder."""
        return EWMWindowBuilderWrapper(
            parent=self._parent,
            column=self.column,
            com=com,
            span=span,
            halflife=halflife,
            alpha=alpha,
            adjust=adjust,
            ignore_na=ignore_na,
            min_periods=min_periods,
        )


# ======================================================================
# ROLLING
# ======================================================================


class RollingWindowBuilderWrapper:
    """Fluent rolling window builder with async/sync operations."""

    def __init__(
        self,
        parent: WindowOrchestrator,
        column: str,
        window: int,
    ):
        """Initialize rolling builder with column and window size."""
        self._parent = parent
        self.column = column
        self.window = window

    async def aapply(
        self,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Asynchronously execute a generic rolling aggregation."""
        return await self._parent.arolling(
            column=self.column,
            window=self.window,
            func=func,
            order_by=order_by,
            q=q,
        )

    @async_to_sync
    async def apply(
        self,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Synchronously execute a generic rolling aggregation."""
        return await self.aapply(func, order_by, q)

    async def asum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling sum."""
        return await self.aapply("sum", order_by)

    @async_to_sync
    async def sum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling sum."""
        return await self.asum(order_by)

    async def amean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling mean."""
        return await self.aapply("mean", order_by)

    @async_to_sync
    async def mean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling mean."""
        return await self.amean(order_by)

    async def amin(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling minimum."""
        return await self.aapply("min", order_by)

    @async_to_sync
    async def min(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling minimum."""
        return await self.amin(order_by)

    async def amax(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling maximum."""
        return await self.aapply("max", order_by)

    @async_to_sync
    async def max(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling maximum."""
        return await self.amax(order_by)

    async def acount(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling count."""
        return await self.aapply("count", order_by)

    @async_to_sync
    async def count(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling count."""
        return await self.acount(order_by)

    async def astd(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling standard deviation."""
        return await self.aapply("std", order_by)

    @async_to_sync
    async def std(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling standard deviation."""
        return await self.astd(order_by)

    async def avar(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling variance."""
        return await self.aapply("var", order_by)

    @async_to_sync
    async def var(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling variance."""
        return await self.avar(order_by)

    async def aquantile(
        self,
        q: float = 0.5,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling quantile."""
        return await self.aapply("quantile", order_by, q)

    @async_to_sync
    async def quantile(
        self,
        q: float = 0.5,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling quantile."""
        return await self.aquantile(q, order_by)

    async def asem(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling standard error."""
        return await self.aapply("sem", order_by)

    @async_to_sync
    async def sem(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling standard error."""
        return await self.asem(order_by)

    async def arank(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling rank."""
        return await self.aapply("rank", order_by)

    @async_to_sync
    async def rank(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling rank."""
        return await self.arank(order_by)

    async def anunique(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling unique-count."""
        return await self.aapply("nunique", order_by)

    @async_to_sync
    async def nunique(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling unique-count."""
        return await self.anunique(order_by)

    async def afirst(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling first value."""
        return await self.aapply("first", order_by)

    @async_to_sync
    async def first(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling first value."""
        return await self.afirst(order_by)

    async def alast(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute rolling last value."""
        return await self.aapply("last", order_by)

    @async_to_sync
    async def last(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute rolling last value."""
        return await self.alast(order_by)


# ======================================================================
# EXPANDING
# ======================================================================


class ExpandingWindowBuilderWrapper:
    """Fluent expanding window builder with async/sync operations."""

    def __init__(
        self,
        parent: WindowOrchestrator,
        column: str,
        min_periods: int,
    ):
        """Initialize expanding builder with column and min periods."""
        self._parent = parent
        self.column = column
        self.min_periods = min_periods

    async def aapply(
        self,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Asynchronously execute a generic expanding aggregation."""
        return await self._parent.aexpanding(
            column=self.column,
            func=func,
            order_by=order_by,
            q=q,
            min_periods=self.min_periods,
        )

    @async_to_sync
    async def apply(
        self,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Synchronously execute a generic expanding aggregation."""
        return await self.aapply(func, order_by, q)

    async def asum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding sum."""
        return await self.aapply("sum", order_by)

    @async_to_sync
    async def sum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding sum."""
        return await self.asum(order_by)

    async def amean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding mean."""
        return await self.aapply("mean", order_by)

    @async_to_sync
    async def mean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding mean."""
        return await self.amean(order_by)

    async def amin(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding minimum."""
        return await self.aapply("min", order_by)

    @async_to_sync
    async def min(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding minimum."""
        return await self.amin(order_by)

    async def amax(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding maximum."""
        return await self.aapply("max", order_by)

    @async_to_sync
    async def max(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding maximum."""
        return await self.amax(order_by)

    async def acount(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding count."""
        return await self.aapply("count", order_by)

    @async_to_sync
    async def count(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding count."""
        return await self.acount(order_by)

    async def astd(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding standard deviation."""
        return await self.aapply("std", order_by)

    @async_to_sync
    async def std(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding standard deviation."""
        return await self.astd(order_by)

    async def avar(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding variance."""
        return await self.aapply("var", order_by)

    @async_to_sync
    async def var(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding variance."""
        return await self.avar(order_by)

    async def aquantile(
        self,
        q: float = 0.5,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding quantile."""
        return await self.aapply("quantile", order_by, q)

    @async_to_sync
    async def quantile(
        self,
        q: float = 0.5,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding quantile."""
        return await self.aquantile(q, order_by)

    async def asem(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding standard error."""
        return await self.aapply("sem", order_by)

    @async_to_sync
    async def sem(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding standard error."""
        return await self.asem(order_by)

    async def arank(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding rank."""
        return await self.aapply("rank", order_by)

    @async_to_sync
    async def rank(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding rank."""
        return await self.arank(order_by)

    async def anunique(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding unique-count."""
        return await self.aapply("nunique", order_by)

    @async_to_sync
    async def nunique(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding unique-count."""
        return await self.anunique(order_by)

    async def afirst(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding first value."""
        return await self.aapply("first", order_by)

    @async_to_sync
    async def first(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding first value."""
        return await self.afirst(order_by)

    async def alast(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute expanding last value."""
        return await self.aapply("last", order_by)

    @async_to_sync
    async def last(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute expanding last value."""
        return await self.alast(order_by)


# ======================================================================
# EWM
# ======================================================================


class EWMWindowBuilderWrapper:
    """Fluent exponentially weighted window builder."""

    def __init__(
        self,
        parent: WindowOrchestrator,
        column: str,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
    ):
        """Initialize EWM builder with weighting configuration."""
        self._parent = parent
        self.column = column
        self.com = com
        self.span = span
        self.halflife = halflife
        self.alpha = alpha
        self.adjust = adjust
        self.ignore_na = ignore_na
        self.min_periods = min_periods

    async def aapply(
        self,
        func: Union[str, List[str]] = "mean",
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously execute a generic EWM aggregation."""
        return await self._parent.aewm(
            column=self.column,
            com=self.com,
            span=self.span,
            halflife=self.halflife,
            alpha=self.alpha,
            adjust=self.adjust,
            ignore_na=self.ignore_na,
            min_periods=self.min_periods,
            func=func,
            order_by=order_by,
        )

    @async_to_sync
    async def apply(
        self,
        func: Union[str, List[str]] = "mean",
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously execute a generic EWM aggregation."""
        return await self.aapply(func, order_by)

    async def amean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute exponentially weighted mean."""
        return await self.aapply("mean", order_by)

    @async_to_sync
    async def mean(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute exponentially weighted mean."""
        return await self.amean(order_by)

    async def asum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute exponentially weighted sum."""
        return await self.aapply("sum", order_by)

    @async_to_sync
    async def sum(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute exponentially weighted sum."""
        return await self.asum(order_by)

    async def astd(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute exponentially weighted stddev."""
        return await self.aapply("std", order_by)

    @async_to_sync
    async def std(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute exponentially weighted stddev."""
        return await self.astd(order_by)

    async def avar(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Asynchronously compute exponentially weighted variance."""
        return await self.aapply("var", order_by)

    @async_to_sync
    async def var(
        self,
        order_by: Union[str, List[str]] = None,
    ):
        """Synchronously compute exponentially weighted variance."""
        return await self.avar(order_by)


# ======================================================================
# PUBLIC ACCESSOR
# ======================================================================


class WindowWrapper(WindowOrchestrator):
    """
    Public wrapper over WindowOrchestrator.
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the window wrapper."""
        super().__init__(memframe_ops_instance)

    async def arolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Asynchronously run direct rolling window operation."""
        return await super().rolling(
            column=column,
            window=window,
            func=func,
            order_by=order_by,
            q=q,
        )

    @overload
    def rolling(
        self,
        column: str,
        window: int,
        func: None = None,
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
    ) -> RollingWindowBuilderWrapper: ...

    @overload
    def rolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
    ) -> Dict[str, Any]: ...

    def rolling(
        self,
        column: str,
        window: int,
        func: Optional[Union[str, List[str]]] = None,
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
    ) -> Union[RollingWindowBuilderWrapper, Dict[str, Any]]:
        """Run a rolling operation directly, or return a fluent builder."""
        if func is None:
            return RollingWindowBuilderWrapper(self, column, window)
        return async_to_sync(self.arolling)(column, window, func, order_by, q)

    async def aexpanding(
        self,
        column: str,
        func: Union[str, List[str]],
        order_by: Union[str, List[str]] = None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> Dict[str, Any]:
        """Asynchronously run direct expanding window operation."""
        return await super().expanding(
            column=column,
            func=func,
            order_by=order_by,
            q=q,
            min_periods=min_periods,
        )

    @overload
    def expanding(
        self,
        column: str,
        func: None = None,
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> ExpandingWindowBuilderWrapper: ...

    @overload
    def expanding(
        self,
        column: str,
        func: Union[str, List[str]],
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> Dict[str, Any]: ...

    def expanding(
        self,
        column: str,
        func: Optional[Union[str, List[str]]] = None,
        order_by: Union[str, List[str], None] = None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> Union[ExpandingWindowBuilderWrapper, Dict[str, Any]]:
        """Run an expanding operation directly, or return a fluent builder."""
        if func is None:
            return ExpandingWindowBuilderWrapper(self, column, min_periods)
        return async_to_sync(self.aexpanding)(column, func, order_by, q, min_periods)

    async def aewm(
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
    ) -> Dict[str, Any]:
        """Asynchronously run direct exponentially weighted operation."""
        return await super().ewm(
            column=column,
            com=com,
            span=span,
            halflife=halflife,
            alpha=alpha,
            adjust=adjust,
            ignore_na=ignore_na,
            min_periods=min_periods,
            func=func,
            order_by=order_by,
        )

    @async_to_sync
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
    ) -> Dict[str, Any]:
        """Synchronously run direct exponentially weighted operation."""
        return await self.aewm(
            column=column,
            com=com,
            span=span,
            halflife=halflife,
            alpha=alpha,
            adjust=adjust,
            ignore_na=ignore_na,
            min_periods=min_periods,
            func=func,
            order_by=order_by,
        )

    def on(
        self,
        column: str,
    ) -> WindowBuilderWrapper:
        """Create a fluent window builder for the selected column."""
        return WindowBuilderWrapper(self, column)


WindowAccessor = WindowWrapper
