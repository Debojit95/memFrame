from __future__ import annotations

from typing import Any


class LineWrapper:
    """Public sync + async line plotting interface."""

    
    async def aline(
        self,
        *,
        x: Any = ...,
        y: Any = ...,
        line_group: Any = ...,
        color: Any = ...,
        line_dash: Any = ...,
        symbol: Any = ...,
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
        line_group: Any = ...,
        color: Any = ...,
        line_dash: Any = ...,
        symbol: Any = ...,
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

    def line(
        self,
        *,
        x: Any = ...,
        y: Any = ...,
        line_group: Any = ...,
        color: Any = ...,
        line_dash: Any = ...,
        symbol: Any = ...,
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
