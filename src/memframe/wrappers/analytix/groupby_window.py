from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from memframe.core.orchestrator.analytix.groupby_window import (
    GroupByWindowOrchestrator,
    GroupByWindow,
    GroupByRolling,
    GroupByExpanding,
    GroupByEWM,
)
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")


# ----------------------------------------------------------------------
#  ROLLING WRAPPER – replaces the decorated parent method
# ----------------------------------------------------------------------
class GroupByRollingWrapper(GroupByRolling):
    """Fluent grouped rolling window builder with async/sync operations."""

    def __init__(self, group_window: GroupByWindow, window: int, order_by):
        """Initialize rolling builder with parent group window and ordering."""
        super().__init__(group_window, window, order_by)

    # ------------------------------------------------------------------
    #  helper that routes through orchestrator (keeps @record_call active)
    # ------------------------------------------------------------------
    async def _call_core(self, func_name: str, column: str, **extra):
        """Route rolling operation calls through the orchestrator API."""
        parent = self._gw._parent
        return await parent.arolling(
            column=column,
            window=self.window,
            func=func_name,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            **extra,
        )

    # --- individual stats ---
    async def amean(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling mean."""
        return await self._call_core("mean", column)

    @async_to_sync
    async def mean(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling mean."""
        return await self.amean(column)

    async def asum(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling sum."""
        return await self._call_core("sum", column)

    @async_to_sync
    async def sum(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling sum."""
        return await self.asum(column)

    async def amin(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling minimum."""
        return await self._call_core("min", column)

    @async_to_sync
    async def min(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling minimum."""
        return await self.amin(column)

    async def amax(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling maximum."""
        return await self._call_core("max", column)

    @async_to_sync
    async def max(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling maximum."""
        return await self.amax(column)

    async def astd(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling standard deviation."""
        return await self._call_core("std", column)

    @async_to_sync
    async def std(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling standard deviation."""
        return await self.astd(column)

    async def avar(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling variance."""
        return await self._call_core("var", column)

    @async_to_sync
    async def var(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling variance."""
        return await self.avar(column)

    async def acount(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling count."""
        return await self._call_core("count", column)

    @async_to_sync
    async def count(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling count."""
        return await self.acount(column)

    async def anunique(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling unique count."""
        return await self._call_core("nunique", column)

    @async_to_sync
    async def nunique(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling unique count."""
        return await self.anunique(column)

    async def afirst(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling first value."""
        return await self._call_core("first", column)

    @async_to_sync
    async def first(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling first value."""
        return await self.afirst(column)

    async def alast(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling last value."""
        return await self._call_core("last", column)

    @async_to_sync
    async def last(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling last value."""
        return await self.alast(column)

    async def aquantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling quantile."""
        return await self._call_core("quantile", column, q=q)

    @async_to_sync
    async def quantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        """Synchronously compute grouped rolling quantile."""
        return await self.aquantile(column, q)

    async def arank(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling rank."""
        return await self._call_core("rank", column)

    @async_to_sync
    async def rank(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling rank."""
        return await self.arank(column)

    async def asem(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling standard error."""
        return await self._call_core("sem", column)

    @async_to_sync
    async def sem(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped rolling standard error."""
        return await self.asem(column)

    async def aagg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Asynchronously compute grouped rolling multi-aggregation."""
        parent = self._gw._parent
        return await parent.arolling(
            column=column,
            window=self.window,
            func=funcs,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            **kwargs,
        )

    @async_to_sync
    async def agg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Synchronously compute grouped rolling multi-aggregation."""
        return await self.aagg(column, funcs, **kwargs)


# ----------------------------------------------------------------------
#  EXPANDING WRAPPER
# ----------------------------------------------------------------------
class GroupByExpandingWrapper(GroupByExpanding):
    """Fluent grouped expanding window builder with async/sync operations."""

    def __init__(self, group_window: GroupByWindow, min_periods: int, order_by):
        """Initialize expanding builder with parent group window and ordering."""
        super().__init__(group_window, min_periods, order_by)

    async def _call_core(self, func_name: str, column: str, **extra):
        """Route expanding operation calls through the orchestrator API."""
        parent = self._gw._parent
        return await parent.aexpanding(
            column=column,
            func=func_name,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            min_periods=self.min_periods,
            **extra,
        )

    # individual stats
    async def asum(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding sum."""
        return await self._call_core("sum", column)
    @async_to_sync
    async def sum(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding sum."""
        return await self.asum(column)

    async def amean(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding mean."""
        return await self._call_core("mean", column)
    @async_to_sync
    async def mean(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding mean."""
        return await self.amean(column)

    async def amin(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding minimum."""
        return await self._call_core("min", column)
    @async_to_sync
    async def min(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding minimum."""
        return await self.amin(column)

    async def amax(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding maximum."""
        return await self._call_core("max", column)
    @async_to_sync
    async def max(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding maximum."""
        return await self.amax(column)

    async def astd(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding standard deviation."""
        return await self._call_core("std", column)
    @async_to_sync
    async def std(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding standard deviation."""
        return await self.astd(column)

    async def avar(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding variance."""
        return await self._call_core("var", column)
    @async_to_sync
    async def var(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding variance."""
        return await self.avar(column)

    async def acount(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding count."""
        return await self._call_core("count", column)
    @async_to_sync
    async def count(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding count."""
        return await self.acount(column)

    async def anunique(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding unique count."""
        return await self._call_core("nunique", column)
    @async_to_sync
    async def nunique(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding unique count."""
        return await self.anunique(column)

    async def afirst(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding first value."""
        return await self._call_core("first", column)
    @async_to_sync
    async def first(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding first value."""
        return await self.afirst(column)

    async def alast(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding last value."""
        return await self._call_core("last", column)
    @async_to_sync
    async def last(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding last value."""
        return await self.alast(column)

    async def aquantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding quantile."""
        return await self._call_core("quantile", column, q=q)
    @async_to_sync
    async def quantile(self, column: str, q: float = 0.5) -> Dict[str, Any]:
        """Synchronously compute grouped expanding quantile."""
        return await self.aquantile(column, q)

    async def arank(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding rank."""
        return await self._call_core("rank", column)
    @async_to_sync
    async def rank(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding rank."""
        return await self.arank(column)

    async def asem(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding standard error."""
        return await self._call_core("sem", column)
    @async_to_sync
    async def sem(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped expanding standard error."""
        return await self.asem(column)

    async def aagg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Asynchronously compute grouped expanding multi-aggregation."""
        parent = self._gw._parent
        return await parent.aexpanding(
            column=column,
            func=funcs,
            group_cols=self._gw.group_cols,
            order_by=self.order_by,
            min_periods=self.min_periods,
            **kwargs,
        )

    @async_to_sync
    async def agg(self, column: str, funcs: List[str], **kwargs) -> Dict[str, Any]:
        """Synchronously compute grouped expanding multi-aggregation."""
        return await self.aagg(column, funcs, **kwargs)


# ----------------------------------------------------------------------
#  EWM WRAPPER
# ----------------------------------------------------------------------
class GroupByEWMWrapper(GroupByEWM):
    """Fluent grouped exponentially weighted window builder."""

    def __init__(self, *args, **kwargs):
        """Initialize grouped exponentially weighted window builder."""
        super().__init__(*args, **kwargs)

    async def _call_core(self, func_name: str, column: str):
        """Route EWM operation calls through the orchestrator API."""
        parent = self._gw._parent
        return await parent.aewm(
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
            func=func_name,
        )

    async def amean(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped exponentially weighted mean."""
        return await self._call_core("mean", column)
    @async_to_sync
    async def mean(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped exponentially weighted mean."""
        return await self.amean(column)

    async def asum(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped exponentially weighted sum."""
        return await self._call_core("sum", column)
    @async_to_sync
    async def sum(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped exponentially weighted sum."""
        return await self.asum(column)

    async def astd(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped exponentially weighted stddev."""
        return await self._call_core("std", column)
    @async_to_sync
    async def std(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped exponentially weighted stddev."""
        return await self.astd(column)

    async def avar(self, column: str) -> Dict[str, Any]:
        """Asynchronously compute grouped exponentially weighted variance."""
        return await self._call_core("var", column)
    @async_to_sync
    async def var(self, column: str) -> Dict[str, Any]:
        """Synchronously compute grouped exponentially weighted variance."""
        return await self.avar(column)

    async def aagg(self, column: str, funcs: List[str]) -> Dict[str, Any]:
        """Asynchronously compute grouped EWM multi-aggregation."""
        parent = self._gw._parent
        return await parent.aewm(
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
            func=funcs,
        )

    @async_to_sync
    async def agg(self, column: str, funcs: List[str]) -> Dict[str, Any]:
        """Synchronously compute grouped EWM multi-aggregation."""
        return await self.aagg(column, funcs)


# ----------------------------------------------------------------------
#  GROUPBY WINDOW BUILDER WRAPPER
# ----------------------------------------------------------------------
class GroupByWindowWrapper(GroupByWindow):
    """Returns the wrapped builders so sync/async methods are available."""

    def __init__(self, *args, **kwargs):
        """Initialize grouped window builder wrapper."""
        super().__init__(*args, **kwargs)

    def rolling(self, window: int, order_by=None) -> GroupByRollingWrapper:
        """Create grouped rolling window builder."""
        return GroupByRollingWrapper(self, window, order_by)

    def expanding(self, min_periods: int = 1, order_by=None) -> GroupByExpandingWrapper:
        """Create grouped expanding window builder."""
        return GroupByExpandingWrapper(self, min_periods, order_by)

    def ewm(self, order_by=None, com=None, span=None, halflife=None, alpha=None,
            adjust=True, ignore_na=False, min_periods=0) -> GroupByEWMWrapper:
        """Create grouped exponentially weighted window builder."""
        return GroupByEWMWrapper(
            self, order_by, com, span, halflife, alpha,
            adjust, ignore_na, min_periods,
        )


# ----------------------------------------------------------------------
#  STATS WRAPPER (direct unified methods + groupby builder)
# ----------------------------------------------------------------------
class GroupByWindowStatsWrapper(GroupByWindowOrchestrator):
    """Public wrapper with sync/async for the direct rolling, expanding, ewm methods."""

    def __init__(self, memframe_ops_instance):
        """Initialize the group-by window stats wrapper."""
        super().__init__(memframe_ops_instance)

    async def arolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by=None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Asynchronously run grouped rolling window operation directly."""
        return await super().rolling(column=column, window=window, func=func,
                                     group_cols=group_cols, order_by=order_by, q=q)

    @async_to_sync
    async def rolling(
        self,
        column: str,
        window: int,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by=None,
        q: float = 0.5,
    ) -> Dict[str, Any]:
        """Synchronously run grouped rolling window operation directly."""
        return await self.arolling(column=column, window=window, func=func,
                                   group_cols=group_cols, order_by=order_by, q=q)

    async def aexpanding(
        self,
        column: str,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by=None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> Dict[str, Any]:
        """Asynchronously run grouped expanding window operation directly."""
        return await super().expanding(column=column, func=func, group_cols=group_cols,
                                       order_by=order_by, q=q, min_periods=min_periods)

    @async_to_sync
    async def expanding(
        self,
        column: str,
        func: Union[str, List[str]],
        group_cols: Union[str, List[str]],
        order_by=None,
        q: float = 0.5,
        min_periods: int = 1,
    ) -> Dict[str, Any]:
        """Synchronously run grouped expanding window operation directly."""
        return await self.aexpanding(column=column, func=func, group_cols=group_cols,
                                     order_by=order_by, q=q, min_periods=min_periods)

    async def aewm(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_by=None,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
        func: Union[str, List[str]] = "mean",
    ) -> Dict[str, Any]:
        """Asynchronously run grouped EWM operation directly."""
        return await super().ewm(column=column, group_cols=group_cols, order_by=order_by,
                                 com=com, span=span, halflife=halflife, alpha=alpha,
                                 adjust=adjust, ignore_na=ignore_na, min_periods=min_periods,
                                 func=func)

    @async_to_sync
    async def ewm(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_by=None,
        com: float = None,
        span: float = None,
        halflife: float = None,
        alpha: float = None,
        adjust: bool = True,
        ignore_na: bool = False,
        min_periods: int = 0,
        func: Union[str, List[str]] = "mean",
    ) -> Dict[str, Any]:
        """Synchronously run grouped EWM operation directly."""
        return await self.aewm(column=column, group_cols=group_cols, order_by=order_by,
                               com=com, span=span, halflife=halflife, alpha=alpha,
                               adjust=adjust, ignore_na=ignore_na, min_periods=min_periods,
                               func=func)

    def groupby(self, *columns: str) -> GroupByWindowWrapper:
        """Create grouped window builder for one or more group-by columns."""
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        # Use orchestrator's groupby to get a base builder, then wrap it
        base_builder = super().groupby(*columns)
        return GroupByWindowWrapper(
            base_builder._parent,
            base_builder._memframe_ops_instance,
            base_builder.group_cols,
        )
