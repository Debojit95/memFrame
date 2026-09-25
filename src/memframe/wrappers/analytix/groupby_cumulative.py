from typing import Any, Dict, List, Optional, Union

from memframe.core.orchestrator.analytix.groupby_cumulative import (
    GroupByCumulative,
    GroupByCumulativeOrchestrator,
)
from memframe.utils.async_sync import async_to_sync


class GroupByCumulativeBuilderWrapper(GroupByCumulative):
    """Fluent group-by cumulative builder with async/sync methods."""

    def __init__(
        self,
        parent: GroupByCumulativeOrchestrator,
        group_cols: List[str],
    ):
        """Initialize the builder with parent orchestrator and group columns."""
        super().__init__(parent, group_cols)

    async def acumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative sum."""
        return await super().cumsum(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative sum."""
        return await self.acumsum(column, order_col, target_col, map_feature)

    async def acumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative product."""
        return await super().cumprod(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative product."""
        return await self.acumprod(column, order_col, target_col, map_feature)

    async def acummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative maximum."""
        return await super().cummax(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative maximum."""
        return await self.acummax(column, order_col, target_col, map_feature)

    async def acummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative minimum."""
        return await super().cummin(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative minimum."""
        return await self.acummin(column, order_col, target_col, map_feature)

    async def acummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative mean."""
        return await super().cummean(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative mean."""
        return await self.acummean(column, order_col, target_col, map_feature)

    async def acumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative count."""
        return await super().cumcount(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative count."""
        return await self.acumcount(column, order_col, target_col, map_feature)

    async def acumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative standard deviation."""
        return await super().cumstd(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative standard deviation."""
        return await self.acumstd(column, order_col, target_col, map_feature)

    async def acumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously compute group-wise cumulative variance."""
        return await super().cumvar(column, order_col, target_col, map_feature)

    @async_to_sync
    async def cumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously compute group-wise cumulative variance."""
        return await self.acumvar(column, order_col, target_col, map_feature)


class GroupByCumulativeWrapper(GroupByCumulativeOrchestrator):
    """Wrapper around `GroupByCumulativeOrchestrator` fluent APIs."""

    def __init__(self, memframe_ops_instance):
        """Initialize the group-by cumulative wrapper."""
        super().__init__(memframe_ops_instance)

    def groupby(self, *columns: str) -> GroupByCumulativeBuilderWrapper:
        """Create a cumulative builder scoped to one or more group columns."""
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByCumulativeBuilderWrapper(self, list(columns))


GroupbyCumulativeAccessor = GroupByCumulativeWrapper
