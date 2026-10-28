from __future__ import annotations

from typing import Any, Dict, List, Optional

from memframe.core.analytix._response import fail
from memframe.core.analytix.index import DataIndexOps, make_index_ops


class IndexOrchestrator:
    """Database-native index management for the active dataset.

    Index DDL is executed directly against the backend. No index metadata is
    written to memFrame's registry and index operations are not cached.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._index_ops = None

    async def _ensure_ops(self) -> DataIndexOps:
        if self._index_ops is None:
            await self._ops_parent._ensure_adapter()
            self._index_ops = make_index_ops(self._ops_parent._adapter)
        return self._index_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    async def create_index(
        self,
        columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = True,
        *,
        expression: Optional[str] = None,
        method: Optional[str] = None,
        where: Optional[str] = None,
        include: Optional[List[str]] = None,
        concurrently: bool = False,
        index_type: Optional[str] = None,
        granularity: int = 1,
        materialize: bool = True,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.create_index(
            table,
            schema,
            columns=columns,
            name=name,
            unique=unique,
            if_not_exists=if_not_exists,
            expression=expression,
            method=method,
            where=where,
            include=include,
            concurrently=concurrently,
            index_type=index_type,
            granularity=granularity,
            materialize=materialize,
        )

    async def drop_index(
        self,
        name: str,
        if_exists: bool = True,
        concurrently: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.drop_index(
            table,
            schema,
            name,
            if_exists=if_exists,
            concurrently=concurrently,
        )

    async def list_indexes(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.list_indexes(table, schema)

    async def set_order_by(
        self,
        columns: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        dry_run: bool = True,
        keep_backup: bool = False,
        verify: str = "count",
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        if getattr(ops, "backend", None) != "clickhouse":
            return fail("set_order_by is only supported for ClickHouse")
        table, schema = await self._get_context()
        backend = getattr(self._memframe, "_backend", None)
        data_id = self._data_id or getattr(self._memframe, "_active_id", None)
        if backend is not None and data_id:
            async with backend._locked_op(data_id):
                return await ops.set_order_by(
                    table,
                    schema,
                    columns=columns,
                    primary_key=primary_key,
                    dry_run=dry_run,
                    keep_backup=keep_backup,
                    verify=verify,
                )
        return await ops.set_order_by(
            table,
            schema,
            columns=columns,
            primary_key=primary_key,
            dry_run=dry_run,
            keep_backup=keep_backup,
            verify=verify,
        )


IndexAccessor = IndexOrchestrator
