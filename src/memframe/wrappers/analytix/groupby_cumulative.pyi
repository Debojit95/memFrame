from typing import Any, Dict, List, Optional, Union


class GroupByCumulativeBuilderWrapper:
    group_cols: List[str]

    async def acumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    async def acumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def cumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = ...,
        target_col: Optional[str] = ...,
    ) -> Dict[str, Any]: ...


class GroupByCumulativeWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    @classmethod
    def replay_create(
        cls,
        memframe,
        data_id: str,
    ) -> "GroupByCumulativeWrapper": ...

    @classmethod
    def from_context(
        cls,
        memframe,
        data_id,
    ) -> "GroupByCumulativeWrapper": ...

    def groupby(
        self,
        *columns: str,
    ) -> GroupByCumulativeBuilderWrapper: ...


GroupbyCumulativeAccessor = GroupByCumulativeWrapper
