from typing import Any, Dict, List, Optional


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

    def groupby(
        self,
        *columns: str,
    ) -> GroupByFillnaBuilder: ...
