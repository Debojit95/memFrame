# sorting_wrapper.py

from typing import Any, Dict, List, Union

from memframe.core.orchestrator.analytix.sorting import SortingOrchestrator
from memframe.db_manager.context import ContextManager
from memframe.utils.async_sync import async_to_sync


class SortingWrapper(SortingOrchestrator):
    """
    Public sync/async wrapper over SortingOrchestrator.

    Supports:
        - sync  : .sort_values(...)
        - async : .asort_values(...)
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the sorting wrapper."""
        super().__init__(memframe_ops_instance)

    async def asort_values(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = "last",
        columns: Union[str, List[str]] = "*",
        chunk_size: int = None,
        upload_as_source: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously sort rows by one or more columns."""
        result = await super().sort_values(
            by=by,
            ascending=ascending,
            na_position=na_position,
            columns=columns,
            chunk_size=chunk_size,
            upload_as_source=upload_as_source,
        )
        if isinstance(result, dict) and result.get("dataset_id"):
            return ContextManager(self._memframe, data_id=result["dataset_id"])
        return result

    @async_to_sync
    async def sort_values(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = "last",
        columns: Union[str, List[str]] = "*",
        chunk_size: int = None,
        upload_as_source: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously sort rows by one or more columns."""
        return await self.asort_values(
            by=by,
            ascending=ascending,
            na_position=na_position,
            columns=columns,
            chunk_size=chunk_size,
            upload_as_source=upload_as_source,
        )


SortingAccessor = SortingWrapper
