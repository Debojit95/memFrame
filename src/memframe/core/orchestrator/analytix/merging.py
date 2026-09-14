from typing import Any, Dict, Optional

from memframe.core.analytix.merging import DataMergeOps, make_merge_ops
from memframe.cache import record_call


class MergeOrchestrator:
    """
    Pandas-style merge/join/concat API.
    Access: ctx.merge(...), ctx.join(...), ctx.concat(...)
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance          # ContextManager
        self._memframe = memframe_ops_instance.memframe   # MemFrame instance
        self._data_id = memframe_ops_instance._data_id
        self._merge_ops: Optional[DataMergeOps] = None

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)

    async def _ensure_ops(self) -> DataMergeOps:
        if self._merge_ops is None:
            await self._ops_parent._ensure_adapter()
            self._merge_ops = make_merge_ops(self._ops_parent._adapter)
        return self._merge_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    def _resolve_right(self, right_ops) -> tuple[str, str]:
        # right_ops is a ContextManager (dataset context)
        if hasattr(right_ops, "_get_active_context"):
            # ponytail: dataset contexts expose _get_active_context; duck-type to avoid circular import
            raise TypeError("right_ops must be awaitable context — use 'await right_ops._get_active_context()' via orchestrator")
        raise TypeError("right_ops must be a ContextManager dataset")

    async def _get_table_and_schema(self, ops) -> tuple[str, str]:
        if hasattr(ops, "_get_active_context"):
            return await ops._get_active_context()
        # fallback: ops is a ContextManager-like with _get_active_context
        return await ops._get_active_context()

    @record_call
    async def merge(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: tuple = ("_x", "_y"),
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        # right table may be in same backend but different data_id; schema is shared upload schema
        right_table, right_schema = await self._get_table_and_schema(right_ops)
        # ponytail: merges are within one backend/schema; right_schema should equal schema
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        return await ops.merge(
            left_table=left_table,
            right_table=right_table,
            schema=schema,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
        )

    @record_call
    async def join(
        self,
        right_ops,
        how: str = "left",
        on=None,
        lsuffix: str = "",
        rsuffix: str = "",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        right_table, _ = await self._get_table_and_schema(right_ops)
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        return await ops.join(
            left_table=left_table,
            right_table=right_table,
            schema=schema,
            how=how,
            on=on,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
        )

    @record_call
    async def concat(
        self,
        other_ops_list,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        # other_ops_list is list[ContextManager]
        tables = [left_table]
        for other in other_ops_list:
            t, _ = await self._get_table_and_schema(other)
            tables.append(t)
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        return await ops.concat(
            tables=tables,
            schema=schema,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
        )


MergeAccessor = MergeOrchestrator
