# wrappers/index_wrapper.py

from typing import Any, Dict, List, Optional

from memframe.core.orchestrator.analytix.index import IndexOrchestrator
from memframe.utils.async_sync import async_to_sync


class IndexWrapper(IndexOrchestrator):
    """
    Sync + async wrapper over IndexOrchestrator.

    Metadata-only logical index (tracked in the registry, never DDL).
    Naming convention:
        async -> aset_index(), areset_index(), aget_index(), areindex(), ...
        sync  -> set_index(), reset_index(), get_index(), reindex(), ...
    The `ctx.index` property returns the bare label values.
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the index wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # set_index
    # ------------------------------------------------------------------

    async def aset_index(
        self,
        keys=None,
        columns: Optional[List[str]] = None,
        append: bool = False,
        drop: bool = True,
        verify_integrity: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously track key column(s) as the logical index."""
        return await super().set_index(
            keys,
            columns=columns,
            append=append,
            drop=drop,
            verify_integrity=verify_integrity,
        )

    @async_to_sync
    async def set_index(
        self,
        keys=None,
        columns: Optional[List[str]] = None,
        append: bool = False,
        drop: bool = True,
        verify_integrity: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously track key column(s) as the logical index."""
        return await self.aset_index(
            keys,
            columns=columns,
            append=append,
            drop=drop,
            verify_integrity=verify_integrity,
        )

    # ------------------------------------------------------------------
    # reset_index
    # ------------------------------------------------------------------

    async def areset_index(
        self,
        level=None,
        drop: bool = False,
        names=None,
    ) -> Dict[str, Any]:
        """Asynchronously clear the tracked index, or a subset of levels."""
        return await super().reset_index(level=level, drop=drop, names=names)

    @async_to_sync
    async def reset_index(
        self,
        level=None,
        drop: bool = False,
        names=None,
    ) -> Dict[str, Any]:
        """Synchronously clear the tracked index, or a subset of levels."""
        return await self.areset_index(level=level, drop=drop, names=names)

    # ------------------------------------------------------------------
    # get_index
    # ------------------------------------------------------------------

    async def aget_index(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Asynchronously read back index columns and labels."""
        return await super().get_index(limit=limit)

    @async_to_sync
    async def get_index(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Synchronously read back index columns and labels."""
        return await self.aget_index(limit=limit)

    # ------------------------------------------------------------------
    # reindex
    # ------------------------------------------------------------------

    async def areindex(
        self,
        labels=None,
        index=None,
        columns=None,
        axis=None,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously conform rows/columns to new labels."""
        return await super().reindex(
            labels,
            index=index,
            columns=columns,
            axis=axis,
            method=method,
            fill_value=fill_value,
            limit=limit,
        )

    @async_to_sync
    async def reindex(
        self,
        labels=None,
        index=None,
        columns=None,
        axis=None,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously conform rows/columns to new labels."""
        return await self.areindex(
            labels,
            index=index,
            columns=columns,
            axis=axis,
            method=method,
            fill_value=fill_value,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # reindex_like
    # ------------------------------------------------------------------

    async def areindex_like(
        self,
        other: str,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously conform to another dataset's index and columns."""
        return await super().reindex_like(
            other, method=method, fill_value=fill_value, limit=limit
        )

    @async_to_sync
    async def reindex_like(
        self,
        other: str,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously conform to another dataset's index and columns."""
        return await self.areindex_like(
            other, method=method, fill_value=fill_value, limit=limit
        )


IndexAccessor = IndexWrapper
