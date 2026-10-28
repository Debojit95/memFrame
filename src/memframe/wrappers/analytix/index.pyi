from __future__ import annotations

from typing import Any, Dict, List, Optional


class IndexWrapper:

    def __init__(self, memframe_ops_instance) -> None: ...

    async def acreate_index(
        self,
        columns: Optional[List[str]] = ...,
        name: Optional[str] = ...,
        unique: bool = ...,
        if_not_exists: bool = ...,
        *,
        expression: Optional[str] = ...,
        method: Optional[str] = ...,
        where: Optional[str] = ...,
        include: Optional[List[str]] = ...,
        concurrently: bool = ...,
        index_type: Optional[str] = ...,
        granularity: int = ...,
        materialize: bool = ...,
    ) -> Dict[str, Any]: ...

    def create_index(
        self,
        columns: Optional[List[str]] = ...,
        name: Optional[str] = ...,
        unique: bool = ...,
        if_not_exists: bool = ...,
        *,
        expression: Optional[str] = ...,
        method: Optional[str] = ...,
        where: Optional[str] = ...,
        include: Optional[List[str]] = ...,
        concurrently: bool = ...,
        index_type: Optional[str] = ...,
        granularity: int = ...,
        materialize: bool = ...,
    ) -> Dict[str, Any]: ...

    async def adrop_index(
        self,
        name: str,
        if_exists: bool = ...,
        concurrently: bool = ...,
    ) -> Dict[str, Any]: ...

    def drop_index(
        self,
        name: str,
        if_exists: bool = ...,
        concurrently: bool = ...,
    ) -> Dict[str, Any]: ...

    async def alist_indexes(self) -> Dict[str, Any]: ...
    def list_indexes(self) -> Dict[str, Any]: ...

    async def aset_order_by(
        self,
        columns: Optional[List[str]] = ...,
        primary_key: Optional[List[str]] = ...,
        dry_run: bool = ...,
        keep_backup: bool = ...,
        verify: str = ...,
    ) -> Dict[str, Any]: ...

    def set_order_by(
        self,
        columns: Optional[List[str]] = ...,
        primary_key: Optional[List[str]] = ...,
        dry_run: bool = ...,
        keep_backup: bool = ...,
        verify: str = ...,
    ) -> Dict[str, Any]: ...


IndexAccessor = IndexWrapper
