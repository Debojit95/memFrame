from typing import Any, Dict, Union

from memframe.core.analytix.sorting import DataSortingOps, make_sorting_ops
from memframe.core.analytix._response import fail
from memframe.core.indexing import MF_IDX, assert_user_columns
from memframe.core.ingestion.datatype_detector import _generate_6char_id
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
            self._sorting_ops = make_sorting_ops(self._ops_parent._adapter)
        return self._sorting_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    @staticmethod
    def _q(col, backend):
        safe = str(col).replace('"', '""').replace("`", "``")
        return f"`{safe}`" if getattr(backend, "backend", None) == "clickhouse" else f'"{safe}"'

    async def _materialize_sorted(self, result, table, schema):
        """Persist a sort permutation as a new upload dataset."""
        backend = self._memframe._backend
        new_id = _generate_6char_id()
        cols = result["output_columns"]
        src = backend.qualify_table(schema, table)
        order = backend.qualify_table(
            result["order_schema"], result["order_table"]
        )
        dst = backend.qualify_table(backend.upload_schema, new_id)
        select = ", ".join(f"s.{self._q(c, backend)}" for c in cols)
        join = (
            f"SELECT {select} FROM {src} s JOIN {order} o "
            f"ON s.{self._q(MF_IDX, backend)} = o.{self._q(MF_IDX, backend)} "
            f'ORDER BY o."position"'
        )
        await backend.execute(f"CREATE TABLE {dst} AS {join}")
        sample = result.get("result")
        row_count = len(sample) if sample is not None else 0
        await backend.execute(
            f"INSERT INTO {backend.csv_registry_table} "
            f"(data_id, filename, table_name, row_count, is_upload_success, schema, mf_source_id) "
            f"VALUES ({backend.placeholder(1)}, {backend.placeholder(2)}, "
            f"{backend.placeholder(3)}, {backend.placeholder(4)}, "
            f"{backend.placeholder(5)}, {backend.placeholder(6)}, {backend.placeholder(7)})",
            new_id, f"sort_{table}", new_id, row_count, True,
            backend.upload_schema, table,
        )
        await self._memframe._uploader._ensure_mf_idx(new_id, new_id, row_count)
        try:
            await backend.drop_table(order)
        except Exception:
            pass
        result["dataset_id"] = new_id
        result["new_table"] = new_id
        return result

    # --------------------------------------------------
    @record_call(deep_cache=True)
    async def sort_values(
        self,
        by: Union[str, list],
        ascending: Union[bool, list] = True,
        na_position: str = "last",
        columns: Any = "*",
        chunk_size: int = None,
        upload_as_source: bool = False,
    ) -> Dict[str, Any]:
        if isinstance(chunk_size, bool) or (
            chunk_size is not None and (not isinstance(chunk_size, int) or chunk_size <= 0)
        ):
            return fail("chunk_size must be a positive integer")
        try:
            assert_user_columns([by] if isinstance(by, str) else by)
        except ValueError as exc:
            return fail(str(exc))

        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        result = await ops.sort_values(
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
        if upload_as_source and isinstance(result, dict) and not result.get("is_error"):
            return await self._materialize_sorted(result, table, schema)
        return result


SortingAccessor = SortingOrchestrator
