# merge_wrapper.pyi

from typing import Any, Dict, Optional, Tuple


class MergeWrapper:
    def __init__(self, totem_ops_instance) -> None: ...

    def __call__(
        self,
        right_ops,
        how: str = ...,
        on=...,
        left_on=...,
        right_on=...,
        suffixes: Tuple[str, str] = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    async def amerge(
        self,
        right_ops,
        how: str = ...,
        on=...,
        left_on=...,
        right_on=...,
        suffixes: Tuple[str, str] = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    def merge(
        self,
        right_ops,
        how: str = ...,
        on=...,
        left_on=...,
        right_on=...,
        suffixes: Tuple[str, str] = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    async def ajoin(
        self,
        right_ops,
        how: str = ...,
        on=...,
        lsuffix: str = ...,
        rsuffix: str = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    def join(
        self,
        right_ops,
        how: str = ...,
        on=...,
        lsuffix: str = ...,
        rsuffix: str = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    async def aconcat(
        self,
        other_ops_list,
        axis: int = ...,
        join: str = ...,
        ignore_index: bool = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...

    def concat(
        self,
        other_ops_list,
        axis: int = ...,
        join: str = ...,
        ignore_index: bool = ...,
        chunk_size: Optional[int] = ...,
    ) -> Dict[str, Any]: ...


MergeAccessor = MergeWrapper
