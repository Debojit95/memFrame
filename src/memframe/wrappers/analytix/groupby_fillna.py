from typing import Any, Dict, List, Optional, Union

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

    async def agroupby_fillna(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously fill missing values using group-wise statistics."""
        return await super().groupby_fillna(
            column=column,
            group_cols=group_cols,
            value=value,
            method=method,
            dtype=dtype,
        )

    @async_to_sync
    async def groupby_fillna(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously fill missing values using group-wise statistics."""
        return await self.agroupby_fillna(
            column=column,
            group_cols=group_cols,
            value=value,
            method=method,
            dtype=dtype,
        )

    def groupby(self, *columns: str) -> GroupByFillnaBuilder:
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByFillnaBuilder(self, list(columns))


GroupByFillnaAccessor = GroupByFillnaWrapper
