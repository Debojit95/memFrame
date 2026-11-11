from typing import Any, Dict, List, Optional, Union

from memframe.core.orchestrator.analytix.groupby_stats import (
    GroupBy,
    GroupByStatsOrchestrator,
)
from memframe.utils.async_sync import async_to_sync


class GroupByWrapper(GroupBy):
    """Fluent group-by stats builder with async/sync methods."""

    def __init__(
        self,
        parent: GroupByStatsOrchestrator,
        group_cols: List[str],
    ):
        """Initialize the builder with parent orchestrator and group columns."""
        super().__init__(parent, group_cols)

    # ------------------------------------------------------------------
    # agg
    # ------------------------------------------------------------------

    async def aagg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously aggregate grouped data with an aggregation mapping."""
        return await super().agg(
            agg_dict=agg_dict,
            new_table=new_table,
            map_feature=map_feature,
        )

    @async_to_sync
    async def agg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously aggregate grouped data with an aggregation mapping."""
        return await self.aagg(
            agg_dict=agg_dict,
            new_table=new_table,
            map_feature=map_feature,
        )

    # ------------------------------------------------------------------
    # convenience stats
    # ------------------------------------------------------------------

    async def asum(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise sum for a column."""
        return await super().sum(column, map_feature=map_feature)

    @async_to_sync
    async def sum(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise sum for a column."""
        return await self.asum(column, map_feature=map_feature)

    async def amean(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise mean for a column."""
        return await super().mean(column, map_feature=map_feature)

    @async_to_sync
    async def mean(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise mean for a column."""
        return await self.amean(column, map_feature=map_feature)

    async def amin(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise minimum for a column."""
        return await super().min(column, map_feature=map_feature)

    @async_to_sync
    async def min(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise minimum for a column."""
        return await self.amin(column, map_feature=map_feature)

    async def amax(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise maximum for a column."""
        return await super().max(column, map_feature=map_feature)

    @async_to_sync
    async def max(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise maximum for a column."""
        return await self.amax(column, map_feature=map_feature)

    async def acount(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise count for a column."""
        return await super().count(column, map_feature=map_feature)

    @async_to_sync
    async def count(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise count for a column."""
        return await self.acount(column, map_feature=map_feature)

    async def amedian(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise median for a column."""
        return await super().median(column, map_feature=map_feature)

    @async_to_sync
    async def median(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise median for a column."""
        return await self.amedian(column, map_feature=map_feature)

    async def amode(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise mode for a column."""
        return await super().mode(column, map_feature=map_feature)

    @async_to_sync
    async def mode(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise mode for a column."""
        return await self.amode(column, map_feature=map_feature)

    async def astd(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise standard deviation."""
        return await super().std(column, map_feature=map_feature)

    @async_to_sync
    async def std(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise standard deviation."""
        return await self.astd(column, map_feature=map_feature)

    async def avar(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise variance."""
        return await super().var(column, map_feature=map_feature)

    @async_to_sync
    async def var(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise variance."""
        return await self.avar(column, map_feature=map_feature)

    async def asem(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise standard error of mean."""
        return await super().sem(column, map_feature=map_feature)

    @async_to_sync
    async def sem(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise standard error of mean."""
        return await self.asem(column, map_feature=map_feature)

    async def anunique(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise count of unique values."""
        return await super().nunique(column, map_feature=map_feature)

    @async_to_sync
    async def nunique(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise count of unique values."""
        return await self.anunique(column, map_feature=map_feature)

    async def arange(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise range (max-min)."""
        return await super().range(column, map_feature=map_feature)

    @async_to_sync
    async def range(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise range (max-min)."""
        return await self.arange(column, map_feature=map_feature)

    async def aproduct(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Asynchronously compute group-wise product for a column."""
        return await super().product(column, map_feature=map_feature)

    @async_to_sync
    async def product(self, column: str, map_feature: bool = False) -> Dict[str, Any]:
        """Synchronously compute group-wise product for a column."""
        return await self.aproduct(column, map_feature=map_feature)

    # ------------------------------------------------------------------
    # event rate
    # ------------------------------------------------------------------

    async def aevent_rate(
        self,
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute grouped event rates over a time unit."""
        return await super().event_rate(
            datetime_col=datetime_col,
            unit=unit,
            new_table=new_table,
        )

    @async_to_sync
    async def event_rate(
        self,
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute grouped event rates over a time unit."""
        return await self.aevent_rate(
            datetime_col=datetime_col,
            unit=unit,
            new_table=new_table,
        )


class GroupByStatsWrapper(GroupByStatsOrchestrator):
    """Wrapper around `GroupByStatsOrchestrator` direct and fluent APIs."""

    def __init__(self, memframe_ops_instance):
        """Initialize the group-by stats wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # direct API
    # ------------------------------------------------------------------

    async def aagg(
        self,
        group_cols: Union[str, List[str]],
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously aggregate using explicit group columns."""
        return await super().agg(
            group_cols=group_cols,
            agg_dict=agg_dict,
            new_table=new_table,
            map_feature=map_feature,
        )

    @async_to_sync
    async def agg(
        self,
        group_cols: Union[str, List[str]],
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously aggregate using explicit group columns."""
        return await self.aagg(
            group_cols=group_cols,
            agg_dict=agg_dict,
            new_table=new_table,
            map_feature=map_feature,
        )

    async def aevent_rate(
        self,
        group_cols: Union[str, List[str]],
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute event rates using explicit group columns."""
        return await super().event_rate(
            group_cols=group_cols,
            datetime_col=datetime_col,
            unit=unit,
            new_table=new_table,
        )

    @async_to_sync
    async def event_rate(
        self,
        group_cols: Union[str, List[str]],
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute event rates using explicit group columns."""
        return await self.aevent_rate(
            group_cols=group_cols,
            datetime_col=datetime_col,
            unit=unit,
            new_table=new_table,
        )

    # ------------------------------------------------------------------
    # builder
    # ------------------------------------------------------------------

    def groupby(self, *columns: str) -> GroupByWrapper:
        """Create a fluent group-by stats builder for the given columns."""
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByWrapper(self, list(columns))


GroupbyStatsAccessor = GroupByStatsWrapper
