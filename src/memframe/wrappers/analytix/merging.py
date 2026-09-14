# merge_wrapper.py

from typing import Any, Optional, Tuple

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

    Successful calls return a live ``ContextManager`` bound to the new output
    table (chainable: ``merged.head()``, ``merged.merge(...)``); error envelopes
    are returned unchanged as dicts.
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the merge wrapper."""
        super().__init__(memframe_ops_instance)

    def _merged_context(self, response):
        """Wrap a successful merge envelope's output table in a ContextManager."""
        # ponytail: local import — context.py lazily imports wrappers, so a
        # top-level import would be circular.
        from memframe.db_manager.context import ContextManager

        if (
            not isinstance(response, dict)
            or response.get("is_error")
            or not response.get("new_table")
        ):
            return response
        data_id = self._data_id or self._memframe._active_id
        return ContextManager(
            self._memframe, data_id=data_id, _table_override=response["new_table"]
        )

    async def _resolve_right_ref(self, right_ops):
        table, schema = await self._get_table_and_schema(right_ops)
        return (getattr(right_ops, "_data_id", None), table, schema)

    def __call__(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: Tuple[str, str] = ("_x", "_y"),
        chunk_size: Optional[int] = None,
    ) -> Any:
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
    ) -> Any:
        """Asynchronously merge two datasets; returns a ContextManager on success."""
        response = await super().merge(
            right_ops=right_ops,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            chunk_size=chunk_size,
            right_ref=await self._resolve_right_ref(right_ops),
        )
        return self._merged_context(response)

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
    ) -> Any:
        """Synchronously merge two datasets; returns a ContextManager on success."""
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
    ) -> Any:
        """Asynchronously join another dataset; returns a ContextManager on success."""
        response = await super().join(
            right_ops=right_ops,
            how=how,
            on=on,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            chunk_size=chunk_size,
            right_ref=await self._resolve_right_ref(right_ops),
        )
        return self._merged_context(response)

    @async_to_sync
    async def join(
        self,
        right_ops,
        how: str = "left",
        on=None,
        lsuffix: str = "",
        rsuffix: str = "",
        chunk_size: Optional[int] = None,
    ) -> Any:
        """Synchronously join another dataset; returns a ContextManager on success."""
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
    ) -> Any:
        """Asynchronously concatenate datasets; returns a ContextManager on success."""
        others_ref = []
        for other in other_ops_list:
            table, schema = await self._get_table_and_schema(other)
            others_ref.append((getattr(other, "_data_id", None), table, schema))
        response = await super().concat(
            other_ops_list=other_ops_list,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            chunk_size=chunk_size,
            others_ref=tuple(others_ref),
        )
        return self._merged_context(response)

    @async_to_sync
    async def concat(
        self,
        other_ops_list,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        chunk_size: Optional[int] = None,
    ) -> Any:
        """Synchronously concatenate datasets; returns a ContextManager on success."""
        return await self.aconcat(
            other_ops_list=other_ops_list,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            chunk_size=chunk_size,
        )


MergeAccessor = MergeWrapper
