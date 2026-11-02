from typing import List, Union
from memframe.core.analytix.reshape import ReshapingOps, make_reshaping_ops
from memframe.core.ingestion.datatype_detector import Backend, _generate_6char_id
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

    async def _ensure_ops(self) -> ReshapingOps:
        if self._reshape_ops is None:
            await self._ops_parent._ensure_adapter()
            self._reshape_ops = make_reshaping_ops(self._ops_parent._adapter)
        return self._reshape_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    async def _register_dataset(self, response, schema, filename):
        """Move a reshape output into upload as a registered dataset."""
        if not isinstance(response, dict) or response.get("is_error"):
            return response
        # ponytail: streaming responses keep their lazy table; renaming it
        # would invalidate the captured iterator. Chunked reshape stays
        # uncached/legacy for now.
        if response.get("iterator") is not None:
            return response
        bare = response.get("new_table")
        if not bare:
            return response
        backend = self._memframe._backend
        be = getattr(backend, "backend", None)
        new_id = _generate_6char_id()
        q_new = f"`{new_id}`" if be == Backend.CLICKHOUSE else f'"{new_id}"'
        src = backend.qualify_table(schema, bare)
        dst = backend.qualify_table(backend.upload_schema, new_id)
        if schema != backend.upload_schema:
            if be == Backend.CLICKHOUSE:
                await backend.execute(f"RENAME TABLE {src} TO {dst}")
            elif be == Backend.DUCKDB:
                await backend.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
                await backend.execute(f"DROP TABLE {src}")
            else:
                await backend.execute(f"ALTER TABLE {src} SET SCHEMA {backend.upload_schema}")
                await backend.execute(
                    f"ALTER TABLE {backend.qualify_table(backend.upload_schema, bare)} "
                    f"RENAME TO {q_new}"
                )
        else:
            if be == Backend.CLICKHOUSE:
                await backend.execute(f"RENAME TABLE {src} TO {dst}")
            else:
                await backend.execute(
                    f"ALTER TABLE {src} RENAME TO {q_new}"
                )
        result = response.get("result")
        try:
            row_count = len(result) if result is not None else 0
        except Exception:
            row_count = 0
        await backend.execute(
            f"INSERT INTO {backend.csv_registry_table} "
            f"(data_id, filename, table_name, row_count, is_upload_success, schema) "
            f"VALUES ({backend.placeholder(1)}, {backend.placeholder(2)}, "
            f"{backend.placeholder(3)}, {backend.placeholder(4)}, "
            f"{backend.placeholder(5)}, {backend.placeholder(6)})",
            new_id, filename, new_id, row_count, True, backend.upload_schema,
        )
        response["dataset_id"] = new_id
        response["new_table"] = new_id
        return response

    async def explode(self, column: Union[str, List[str]], chunk_size=None):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        res = await ops.explode(table, schema, column, backend, data_id, chunk_size)
        return res

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
        return await self._register_dataset(res, schema, f"melt_{data_id}")

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
        return await self._register_dataset(res, schema, f"pivot_{data_id}")

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
        return await self._register_dataset(res, schema, f"pivot_table_{data_id}")

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
        return await self._register_dataset(res, schema, f"crosstab_{data_id}")

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
        return await self._register_dataset(res, schema, f"transpose_{data_id}")

    @record_call(deep_cache=True)
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

    @record_call(deep_cache=True)
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