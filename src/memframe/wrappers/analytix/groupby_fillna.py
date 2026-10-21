from typing import Any, Dict, List, Optional

from memframe.core.orchestrator.analytix.groupby_fillna import (
    GroupByFillna,
    GroupByFillnaOrchestrator,
)
from memframe.utils.async_sync import async_to_sync


class GroupByFillnaBuilder(GroupByFillna):
    """Fluent group-by fillna builder with async/sync methods."""

    def __init__(self, parent: GroupByFillnaOrchestrator, group_cols: List[str]):
        super().__init__(parent, group_cols)

    async def afillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously fill missing values using group-wise statistics."""
        return await super().fillna(
            column=column,
            value=value,
            method=method,
            dtype=dtype,
        )

    @async_to_sync
    async def fillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously fill missing values using group-wise statistics."""
        return await self.afillna(
            column=column,
            value=value,
            method=method,
            dtype=dtype,
        )


class GroupByFillnaWrapper(GroupByFillnaOrchestrator):
    """Wrapper around `GroupByFillnaOrchestrator` direct and fluent APIs."""

    def __init__(self, memframe_ops_instance):
        super().__init__(memframe_ops_instance)

    def groupby(self, *columns: str) -> GroupByFillnaBuilder:
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByFillnaBuilder(self, list(columns))


GroupByFillnaAccessor = GroupByFillnaWrapper
