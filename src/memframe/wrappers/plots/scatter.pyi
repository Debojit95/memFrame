from __future__ import annotations
from typing import Any


class ScatterWrapper:
    """Public sync + async scatter plotting interface."""

    def __init__(self, memframe_ops_instance) -> None: ...


    async def ascatter(
        self,
        *,
        x: Any = ...,
        y: Any = ...,
        color: Any = ...,
        symbol: Any = ...,
        size: Any = ...,
        hover_name: Any = ...,
        hover_data: Any = ...,
        text: Any = ...,
        facet_row: Any = ...,
        facet_col: Any = ...,
        error_x: Any = ...,
        error_x_minus: Any = ...,
        error_y: Any = ...,
        error_y_minus: Any = ...,
        animation_frame: Any = ...,
        animation_group: Any = ...,
        custom_data: Any = ...,
        **kwargs: Any,
    ) -> Any: ...

    def __call__(
        self,
        *,
        x: Any = ...,
        y: Any = ...,
        color: Any = ...,
        symbol: Any = ...,
        size: Any = ...,
        hover_name: Any = ...,
        hover_data: Any = ...,
        text: Any = ...,
        facet_row: Any = ...,
        facet_col: Any = ...,
        error_x: Any = ...,
        error_x_minus: Any = ...,
        error_y: Any = ...,
        error_y_minus: Any = ...,
        animation_frame: Any = ...,
        animation_group: Any = ...,
        custom_data: Any = ...,
        **kwargs: Any,
    ) -> Any: ...

    def scatter(
        self,
        *,
        x: Any = ...,
        y: Any = ...,
        color: Any = ...,
        symbol: Any = ...,
        size: Any = ...,
        hover_name: Any = ...,
        hover_data: Any = ...,
        text: Any = ...,
        facet_row: Any = ...,
        facet_col: Any = ...,
        error_x: Any = ...,
        error_x_minus: Any = ...,
        error_y: Any = ...,
        error_y_minus: Any = ...,
        animation_frame: Any = ...,
        animation_group: Any = ...,
        custom_data: Any = ...,
        **kwargs: Any,
    ) -> Any: ...
