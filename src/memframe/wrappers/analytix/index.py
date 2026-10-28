from __future__ import annotations

from typing import Any, Dict, List, Optional

from memframe.core.orchestrator.analytix.index import IndexOrchestrator
from memframe.utils.async_sync import async_to_sync


class IndexWrapper(IndexOrchestrator):
    """Sync + async database-native index API.

    Indexes are owned by the backend; ``set_order_by`` is the ClickHouse-only
    physical table rebuild for changing ORDER BY/PRIMARY KEY.
    """

    def __init__(self, memframe_ops_instance):
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # create
    # ------------------------------------------------------------------
    async def acreate_index(
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
        """Asynchronously create a database index."""
        return await super().create_index(
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

    @async_to_sync
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
        """Synchronously create a database index."""
        return await self.acreate_index(
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

    # ------------------------------------------------------------------
    # drop
    # ------------------------------------------------------------------
    async def adrop_index(
        self,
        name: str,
        if_exists: bool = True,
        concurrently: bool = False,
    ) -> Dict[str, Any]:
        """Asynchronously drop a database index."""
        return await super().drop_index(
            name, if_exists=if_exists, concurrently=concurrently
        )

    @async_to_sync
    async def drop_index(
        self,
        name: str,
        if_exists: bool = True,
        concurrently: bool = False,
    ) -> Dict[str, Any]:
        """Synchronously drop a database index."""
        return await self.adrop_index(
            name, if_exists=if_exists, concurrently=concurrently
        )

    # ------------------------------------------------------------------
    # list
    # ------------------------------------------------------------------
    async def alist_indexes(self) -> Dict[str, Any]:
        """Asynchronously list indexes on the active table."""
        return await super().list_indexes()

    @async_to_sync
    async def list_indexes(self) -> Dict[str, Any]:
        """Synchronously list indexes on the active table."""
        return await self.alist_indexes()

    # ------------------------------------------------------------------
    # ClickHouse physical ORDER BY rebuild
    # ------------------------------------------------------------------
    async def aset_order_by(
        self,
        columns: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        dry_run: bool = True,
        keep_backup: bool = False,
        verify: str = "count",
    ) -> Dict[str, Any]:
        """Asynchronously rebuild a ClickHouse table with a new ORDER BY."""
        return await super().set_order_by(
            columns=columns,
            primary_key=primary_key,
            dry_run=dry_run,
            keep_backup=keep_backup,
            verify=verify,
        )

    @async_to_sync
    async def set_order_by(
        self,
        columns: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        dry_run: bool = True,
        keep_backup: bool = False,
        verify: str = "count",
    ) -> Dict[str, Any]:
        """Synchronously rebuild a ClickHouse table with a new ORDER BY."""
        return await self.aset_order_by(
            columns=columns,
            primary_key=primary_key,
            dry_run=dry_run,
            keep_backup=keep_backup,
            verify=verify,
        )


IndexAccessor = IndexWrapper
