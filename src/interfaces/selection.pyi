# interfaces/selection_wrapper.pyi

from typing import Any, List, Optional, Tuple, Union


class SelectionWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    # ------------------------------------------------------------------
    # asof
    # ------------------------------------------------------------------

    async def aasof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = ...,
        chunk_size: int = ...,
    ): ...

    def asof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = ...,
        chunk_size: int = ...,
    ): ...

    # ------------------------------------------------------------------
    # at
    # ------------------------------------------------------------------

    async def aat(
        self,
        row_label,
        column_label: str,
        index_column: str = ...,
    ): ...

    def at(
        self,
        row_label,
        column_label: str,
        index_column: str = ...,
    ): ...

    # ------------------------------------------------------------------
    # iat
    # ------------------------------------------------------------------

    async def aiat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ): ...

    def iat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ): ...

    # ------------------------------------------------------------------
    # get
    # ------------------------------------------------------------------

    async def aget(
        self,
        keys: Union[str, List[str]],
        default: Any = ...,
    ): ...

    def get(
        self,
        keys: Union[str, List[str]],
        default: Any = ...,
    ): ...

    # ------------------------------------------------------------------
    # loc
    # ------------------------------------------------------------------

    async def aloc(
        self,
        row_selector,
        columns=...,
        index_column: str = ...,
        chunk_size: int = ...,
    ): ...

    def loc(
        self,
        row_selector,
        columns=...,
        index_column: str = ...,
        chunk_size: int = ...,
    ): ...

    # ------------------------------------------------------------------
    # where
    # ------------------------------------------------------------------

    async def awhere(
        self,
        cond: str,
        other: Optional[Any] = ...,
        chunk_size: int = ...,
    ): ...

    def where(
        self,
        cond: str,
        other: Optional[Any] = ...,
        chunk_size: int = ...,
    ): ...

    # ------------------------------------------------------------------
    # select_dtypes
    # ------------------------------------------------------------------

    async def aselect_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = ...,
        exclude: Optional[Union[str, List[str]]] = ...,
        chunk_size: int = ...,
    ): ...

    def select_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = ...,
        exclude: Optional[Union[str, List[str]]] = ...,
        chunk_size: int = ...,
    ): ...

    # ------------------------------------------------------------------
    # iloc
    # ------------------------------------------------------------------

    async def ailoc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = ...,
        col_indexer: Union[int, List[int], slice, list, str] = ...,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = ...,
    ): ...

    def iloc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = ...,
        col_indexer: Union[int, List[int], slice, list, str] = ...,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = ...,
    ): ...

    # ------------------------------------------------------------------
    # take
    # ------------------------------------------------------------------

    async def atake(
        self,
        indices: List[int],
        axis: int = ...,
    ): ...

    def take(
        self,
        indices: List[int],
        axis: int = ...,
    ): ...


SelectionAccessor = SelectionWrapper