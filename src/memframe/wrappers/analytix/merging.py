# merge_wrapper.py

from typing import Any, Dict, Optional, Tuple

from memframe.core.orchestrator.analytix.merging import MergeOrchestrator
from memframe.utils.async_sync import async_to_sync


class MergeWrapper(MergeOrchestrator):
    """
    Public sync + async wrapper for MergeOrchestrator.

    Sync methods:
        - merge()
        - join()
        - concat()

    Async methods:
        - amerge()
        - ajoin()
        - aconcat()
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the merge wrapper."""
        super().__init__(memframe_ops_instance)

    def __call__(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Allow direct call style:
            ops.merge(other_ops, on="id")
        """
        return self.merge(
            right_ops=right_ops,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # MERGE
    # ------------------------------------------------------------------
    async def amerge(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously merge two datasets using key-based joins."""
        return await super().merge(
            right_ops=right_ops,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def merge(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously merge two datasets using key-based joins."""
        return await self.amerge(
            right_ops=right_ops,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # JOIN
    # ------------------------------------------------------------------
    async def ajoin(
        self,
        right_ops,
        how: str = "left",
        on=None,
        lsuffix: str = "",
        rsuffix: str = "",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously join another dataset by index or key."""
        return await super().join(
            right_ops=right_ops,
            how=how,
            on=on,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def join(
        self,
        right_ops,
        how: str = "left",
        on=None,
        lsuffix: str = "",
        rsuffix: str = "",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously join another dataset by index or key."""
        return await self.ajoin(
            right_ops=right_ops,
            how=how,
            on=on,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # CONCAT
    # ------------------------------------------------------------------
    async def aconcat(
        self,
        other_ops_list,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously concatenate multiple datasets along an axis."""
        return await super().concat(
            other_ops_list=other_ops_list,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def concat(
        self,
        other_ops_list,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously concatenate multiple datasets along an axis."""
        return await self.aconcat(
            other_ops_list=other_ops_list,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            chunk_size=chunk_size,
        )


MergeAccessor = MergeWrapper
