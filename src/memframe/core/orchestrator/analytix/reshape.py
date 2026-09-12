from typing import List, Union
from memframe.core.analytix.reshape import ReshapingOps, make_reshaping_ops
from memframe.cache import record_call


class ReshapingOrchestrator:
    """
    Pandas‑style reshape API.
    Access: ctx.reshape.explode(...), ctx.reshape.pivot(...), etc.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance          # ContextManager
        self._memframe = memframe_ops_instance.memframe   # MemFrame instance
        self._data_id = memframe_ops_instance._data_id
        self._reshape_ops = None

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)

    async def _ensure_ops(self) -> ReshapingOps:
        if self._reshape_ops is None:
            await self._ops_parent._ensure_adapter()
            self._reshape_ops = make_reshaping_ops(self._ops_parent._adapter)
        return self._reshape_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    @record_call
    async def explode(self, column: Union[str, List[str]], chunk_size=None):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.explode(table, schema, column, backend, data_id, chunk_size)
        return res

    @record_call
    async def melt(
        self,
        id_vars,
        value_vars,
        var_name="variable",
        value_name="value",
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.melt(
            table, schema,
            id_vars, value_vars,
            backend, data_id,
            var_name, value_name,
            chunk_size,
        )
        return res

    @record_call
    async def pivot(
        self,
        index,
        columns,
        values,
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.pivot(
            table, schema,
            index, columns, values,
            backend, data_id,
            chunk_size,
        )
        return res

    @record_call
    async def pivot_table(
        self,
        index=None,
        columns=None,
        values=None,
        aggfunc="mean",
        fill_value=None,
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.pivot_table(
            table, schema,
            index, columns, values,
            aggfunc, fill_value,
            backend, data_id,
            chunk_size,
        )
        return res

    @record_call
    async def crosstab(
        self,
        index,
        columns,
        values=None,
        aggfunc=None,
        margins=False,
        normalize=False,
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.crosstab(
            table, schema,
            index, columns,
            values, aggfunc,
            margins,
            normalize=normalize,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
        )
        return res

    @record_call
    async def transpose(self, chunk_size=None):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.transpose(
            table, schema,
            backend, data_id,
            chunk_size,
        )
        return res

    @record_call
    async def rank(
        self,
        columns,
        method="average",
        na_option="keep",
        ascending=True,
        pct=False,
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.rank(
            table, schema,
            columns,
            method, na_option, ascending, pct,
            backend, data_id,
            chunk_size,
        )
        return res

    @record_call
    async def groupby_rank(
        self,
        groupby,
        columns,
        method="average",
        ascending=True,
        na_option="keep",
        pct=False,
        chunk_size=None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.groupby_rank(
            table, schema,
            groupby, columns,
            method, ascending, na_option, pct,
            backend, data_id,
            chunk_size,
        )
        return res


# Alias for convenience
ReshapeAccessor = ReshapingOrchestrator