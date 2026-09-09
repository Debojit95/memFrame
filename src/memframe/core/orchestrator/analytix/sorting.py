from typing import Any, Dict, Union

from memframe.core.analytix.sorting import DataSortingOps
from memframe.core.analytix._response import fail
from memframe.cache import record_call


class SortingOrchestrator:
    """
    Pandas-like sorting API.
    Accessed via the ContextManager dispatch (ctx.sort_values / ctx.asort_values).
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe  # MemFrame
        self._data_id = memframe_ops_instance._data_id
        self._sorting_ops = None

    async def _ensure_ops(self) -> DataSortingOps:
        if self._sorting_ops is None:
            await self._ops_parent._ensure_adapter()
            self._sorting_ops = DataSortingOps(self._ops_parent._adapter)
        return self._sorting_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    # --------------------------------------------------
    @record_call(deep_cache=True)
    async def sort_values(
        self,
        by: Union[str, list],
        ascending: Union[bool, list] = True,
        na_position: str = "last",
        columns: Any = "*",
        chunk_size: int = None,
    ) -> Dict[str, Any]:
        if isinstance(chunk_size, bool) or (
            chunk_size is not None and (not isinstance(chunk_size, int) or chunk_size <= 0)
        ):
            return fail("chunk_size must be a positive integer")

        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        return await ops.sort_values(
            table=table,
            schema=schema,
            by=by,
            ascending=ascending,
            na_position=na_position,
            columns=columns,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
        )


SortingAccessor = SortingOrchestrator
