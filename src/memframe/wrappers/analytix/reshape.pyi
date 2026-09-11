# interfaces/reshaping_wrapper.pyi

from typing import List, Optional, Union


class ReshapingWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    @classmethod
    def replay_create(
        cls,
        memframe,
        data_id: str,
    ) -> "ReshapingWrapper": ...

    @classmethod
    def from_context(
        cls,
        memframe,
        data_id,
    ) -> "ReshapingWrapper": ...

    # ------------------------------------------------------------------
    # explode
    # ------------------------------------------------------------------

    async def aexplode(
        self,
        column: Union[str, List[str]],
        chunk_size: Optional[int] = ...,
    ): ...

    def explode(
        self,
        column: Union[str, List[str]],
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # melt
    # ------------------------------------------------------------------

    async def amelt(
        self,
        id_vars,
        value_vars,
        var_name: str = ...,
        value_name: str = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    def melt(
        self,
        id_vars,
        value_vars,
        var_name: str = ...,
        value_name: str = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # pivot
    # ------------------------------------------------------------------

    async def apivot(
        self,
        index,
        columns,
        values,
        chunk_size: Optional[int] = ...,
    ): ...

    def pivot(
        self,
        index,
        columns,
        values,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # pivot_table
    # ------------------------------------------------------------------

    async def apivot_table(
        self,
        index=...,
        columns=...,
        values=...,
        aggfunc: str = ...,
        fill_value=...,
        chunk_size: Optional[int] = ...,
    ): ...

    def pivot_table(
        self,
        index=...,
        columns=...,
        values=...,
        aggfunc: str = ...,
        fill_value=...,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # crosstab
    # ------------------------------------------------------------------

    async def acrosstab(
        self,
        index,
        columns,
        values=...,
        aggfunc=...,
        margins: bool = ...,
        normalize: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    def crosstab(
        self,
        index,
        columns,
        values=...,
        aggfunc=...,
        margins: bool = ...,
        normalize: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # transpose
    # ------------------------------------------------------------------

    async def atranspose(
        self,
        chunk_size: Optional[int] = ...,
    ): ...

    def transpose(
        self,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # rank
    # ------------------------------------------------------------------

    async def arank(
        self,
        columns,
        method: str = ...,
        na_option: str = ...,
        ascending: bool = ...,
        pct: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    def rank(
        self,
        columns,
        method: str = ...,
        na_option: str = ...,
        ascending: bool = ...,
        pct: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    # ------------------------------------------------------------------
    # groupby_rank
    # ------------------------------------------------------------------

    async def agroupby_rank(
        self,
        groupby,
        columns,
        method: str = ...,
        ascending: bool = ...,
        na_option: str = ...,
        pct: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...

    def groupby_rank(
        self,
        groupby,
        columns,
        method: str = ...,
        ascending: bool = ...,
        na_option: str = ...,
        pct: bool = ...,
        chunk_size: Optional[int] = ...,
    ): ...


ReshapeAccessor = ReshapingWrapper