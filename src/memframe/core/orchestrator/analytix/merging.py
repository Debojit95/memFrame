from typing import Any, Dict, Optional

from memframe.core.analytix.merging import DataMergeOps, make_merge_ops
from memframe.core.ingestion.datatype_detector import _generate_6char_id
from memframe.core.ingestion.datatype_detector import Backend


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

    async def _move_to_upload(self, bare: str, schema: str) -> None:
        if schema == self._memframe._backend.upload_schema:
            return
        backend = self._memframe._backend
        be = getattr(backend, "backend", None)
        src = backend.qualify_table(schema, bare)
        dst = backend.qualify_table(backend.upload_schema, bare)
        if be == Backend.CLICKHOUSE:
            await backend.execute(f"RENAME TABLE {src} TO {dst}")
        elif be == Backend.DUCKDB:
            await backend.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
            await backend.execute(f"DROP TABLE {src}")
        else:
            await backend.execute(f"ALTER TABLE {src} SET SCHEMA {backend.upload_schema}")

    async def _register_dataset(self, response: Dict[str, Any], schema: str, filename: str) -> Dict[str, Any]:
        if not isinstance(response, dict) or response.get("is_error"):
            return response
        bare = response.get("new_table")
        if not bare:
            return response
        backend = self._memframe._backend
        await self._move_to_upload(bare, schema)
        result = response.get("result")
        try:
            row_count = len(result) if result is not None else 0
        except Exception:
            row_count = 0
        q_new = backend.qualify_table(backend.upload_schema, bare)
        await backend.execute(f'ALTER TABLE {q_new} DROP COLUMN IF EXISTS "_mf_idx"')
        await backend.execute(
            f"INSERT INTO {backend.csv_registry_table} "
            f"(data_id, filename, table_name, row_count, is_upload_success, schema) "
            f"VALUES ({backend.placeholder(1)}, {backend.placeholder(2)}, "
            f"{backend.placeholder(3)}, {backend.placeholder(4)}, "
            f"{backend.placeholder(5)}, {backend.placeholder(6)})",
            bare, filename, bare, row_count, True, backend.upload_schema,
        )
        uploader = getattr(self._memframe, "_uploader", None)
        if uploader is not None:
            await uploader._ensure_mf_idx(bare, bare, row_count)
        response["dataset_id"] = bare
        response["new_table"] = bare
        return response

    async def merge(
        self,
        right_ops,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        suffixes: tuple = ("_x", "_y"),
        chunk_size: Optional[int] = None,
        right_ref: Optional[tuple] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        # ponytail: sides may live in different schemas (a merged cache
        # table plus an upload table), so each side keeps its own schema.
        right_table, right_schema = await self._get_table_and_schema(right_ops)
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        out_id = _generate_6char_id()
        result = await ops.merge(
            left_table=left_table,
            right_table=right_table,
            schema=schema,
            right_schema=right_schema,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
            requested_table=out_id,
        )
        return await self._register_dataset(
            result, schema, f"merge_{data_id}_{getattr(right_ops, '_data_id', 'x')}"
        )

    async def join(
        self,
        right_ops,
        how: str = "left",
        on=None,
        lsuffix: str = "",
        rsuffix: str = "",
        chunk_size: Optional[int] = None,
        right_ref: Optional[tuple] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        right_table, right_schema = await self._get_table_and_schema(right_ops)
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        out_id = _generate_6char_id()
        result = await ops.join(
            left_table=left_table,
            right_table=right_table,
            schema=schema,
            right_schema=right_schema,
            how=how,
            on=on,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
            requested_table=out_id,
        )
        return await self._register_dataset(
            result, schema, f"join_{data_id}_{getattr(right_ops, '_data_id', 'x')}"
        )

    async def concat(
        self,
        other_ops_list,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        chunk_size: Optional[int] = None,
        others_ref: Optional[tuple] = None,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        left_table, schema = await self._get_context()
        # other_ops_list is list[ContextManager]
        tables = [left_table]
        schemas = [schema]
        for other in other_ops_list:
            t, s = await self._get_table_and_schema(other)
            tables.append(t)
            schemas.append(s)
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        out_id = _generate_6char_id()
        result = await ops.concat(
            tables=tables,
            schema=schema,
            table_schemas=schemas,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
            requested_table=out_id,
        )
        return await self._register_dataset(
            result, schema, f"concat_{data_id}_{len(other_ops_list)}"
        )


MergeAccessor = MergeOrchestrator
