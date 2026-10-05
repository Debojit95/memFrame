from typing import Any, Dict, List, Optional

from memframe.core.analytix.index import DataIndexOps, make_index_ops
from memframe.cache import record_call


class IndexOrchestrator:
    """Metadata-only index + read-time reindex API.

    Accessed via the ContextManager dispatch (ctx.set_index / ctx.aset_index
    once wrappers land). Metadata writes are signature-only in the cache;
    reindex reads persist under deep_cache via the returned DataFrame.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe  # MemFrame
        self._data_id = memframe_ops_instance._data_id
        self._index_ops = None

    async def _ensure_ops(self) -> DataIndexOps:
        if self._index_ops is None:
            await self._ops_parent._ensure_adapter()
            self._index_ops = make_index_ops(self._ops_parent._adapter)
        return self._index_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    def _ids(self):
        return self._memframe._backend, self._data_id or self._memframe._active_id

    @record_call(deep_cache=False)
    async def set_index(
        self,
        keys=None,
        *,
        columns: Optional[List[str]] = None,
        append: bool = False,
        drop: bool = True,
        verify_integrity: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend, data_id = self._ids()
        return await ops.set_index(
            table, schema, keys, columns=columns, backend=backend,
            data_id=data_id, append=append, drop=drop,
            verify_integrity=verify_integrity,
        )

    @record_call(deep_cache=False)
    async def reset_index(
        self,
        level=None,
        drop: bool = False,
        names=None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend, data_id = self._ids()
        return await ops.reset_index(
            table, schema, backend=backend, data_id=data_id,
            level=level, drop=drop, names=names,
        )

    @record_call(deep_cache=True)
    async def get_index(self, limit: Optional[int] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend, data_id = self._ids()
        return await ops.get_index(
            table, schema, backend=backend, data_id=data_id, limit=limit,
        )

    @record_call(deep_cache=True)
    async def reindex(
        self,
        labels=None,
        *,
        index=None,
        columns=None,
        axis=None,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend, data_id = self._ids()
        return await ops.reindex(
            table, schema, labels, index=index, columns=columns, axis=axis,
            method=method, fill_value=fill_value, limit=limit,
            backend=backend, data_id=data_id,
        )

    @record_call(deep_cache=True)
    async def reindex_like(
        self,
        other: str,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend, data_id = self._ids()
        return await ops.reindex_like(
            table, schema, other, backend=backend, data_id=data_id,
            method=method, fill_value=fill_value, limit=limit,
        )


IndexAccessor = IndexOrchestrator
