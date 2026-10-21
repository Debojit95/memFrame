from typing import Any, Dict, List, Optional, Union


class GroupByFillnaBuilder:
    group_cols: List[str]

    async def afillna(
        self,
        column: str,
        value: Any = ...,
        method: str = ...,
        dtype: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def fillna(
        self,
        column: str,
        value: Any = ...,
        method: str = ...,
        dtype: Optional[str] = ...,
    ) -> Dict[str, Any]: ...


class GroupByFillnaWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    async def agroupby_fillna(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        value: Any = ...,
        method: str = ...,
        dtype: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def groupby_fillna(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        value: Any = ...,
        method: str = ...,
        dtype: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def groupby(
        self,
        *columns: str,
    ) -> GroupByFillnaBuilder: ...
