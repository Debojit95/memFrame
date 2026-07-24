from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union


class SelectionWrapper:
    def __init__(self, memframe_ops_instance: Any) -> None: ...

    async def aasof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    def asof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    async def aat(
        self,
        row_label: Any,
        column_label: str,
        index_column: str = None,
    ) -> Dict[str, Any]: ...

    def at(
        self,
        row_label: Any,
        column_label: str,
        index_column: str = None,
    ) -> Dict[str, Any]: ...

    async def aiat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ) -> Dict[str, Any]: ...

    def iat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ) -> Dict[str, Any]: ...

    async def aget(
        self,
        keys: Union[str, List[str]],
        default: Any = None,
    ) -> Dict[str, Any]: ...

    def get(
        self,
        keys: Union[str, List[str]],
        default: Any = None,
    ) -> Dict[str, Any]: ...

    async def aloc(
        self,
        row_selector: Any,
        columns: Any = None,
        index_column: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    def loc(
        self,
        row_selector: Any,
        columns: Any = None,
        index_column: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    async def awhere(
        self,
        cond: str,
        other: Optional[Any] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    def where(
        self,
        cond: str,
        other: Optional[Any] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    async def aselect_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    def select_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    async def ailoc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = None,
        col_indexer: Union[int, List[int], slice, list, str] = None,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> Dict[str, Any]: ...

    def iloc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = None,
        col_indexer: Union[int, List[int], slice, list, str] = None,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> Dict[str, Any]: ...

    async def atake(
        self,
        indices: List[int],
        axis: int = 0,
    ) -> Dict[str, Any]: ...

    def take(
        self,
        indices: List[int],
        axis: int = 0,
    ) -> Dict[str, Any]: ...


SelectionAccessor = SelectionWrapper
