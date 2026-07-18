from __future__ import annotations

from typing import Any

from src.core.orchestrator.plots.line import LineOrchestrator
from src.utils.async_sync import async_to_sync


class LineWrapper(LineOrchestrator):
    """Public sync + async wrapper for line plotting."""

    def __call__(self, **kwargs: Any):
        """Allow property-style access to be called like a method."""
        return self.line(**kwargs)

    async def aline(
        self,
        *,
        x: Any = None,
        y: Any = None,
        line_group: Any = None,
        color: Any = None,
        line_dash: Any = None,
        symbol: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        text: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        error_x: Any = None,
        error_x_minus: Any = None,
        error_y: Any = None,
        error_y_minus: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        custom_data: Any = None,
        **kwargs: Any,
    ):
        return await super().line(
            x=x,
            y=y,
            line_group=line_group,
            color=color,
            line_dash=line_dash,
            symbol=symbol,
            hover_name=hover_name,
            hover_data=hover_data,
            text=text,
            facet_row=facet_row,
            facet_col=facet_col,
            error_x=error_x,
            error_x_minus=error_x_minus,
            error_y=error_y,
            error_y_minus=error_y_minus,
            animation_frame=animation_frame,
            animation_group=animation_group,
            custom_data=custom_data,
            **kwargs,
        )

    @async_to_sync
    async def line(
        self,
        *,
        x: Any = None,
        y: Any = None,
        line_group: Any = None,
        color: Any = None,
        line_dash: Any = None,
        symbol: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        text: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        error_x: Any = None,
        error_x_minus: Any = None,
        error_y: Any = None,
        error_y_minus: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        custom_data: Any = None,
        **kwargs: Any,
    ):
        return await self.aline(
            x=x,
            y=y,
            line_group=line_group,
            color=color,
            line_dash=line_dash,
            symbol=symbol,
            hover_name=hover_name,
            hover_data=hover_data,
            text=text,
            facet_row=facet_row,
            facet_col=facet_col,
            error_x=error_x,
            error_x_minus=error_x_minus,
            error_y=error_y,
            error_y_minus=error_y_minus,
            animation_frame=animation_frame,
            animation_group=animation_group,
            custom_data=custom_data,
            **kwargs,
        )
