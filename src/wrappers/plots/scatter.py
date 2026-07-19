from __future__ import annotations
from typing import Any

from src.core.orchestrator.plots.scatter import ScatterOrchestrator
from src.utils.async_sync import async_to_sync


class ScatterWrapper(ScatterOrchestrator):
    """Public sync + async wrapper for scatter plotting."""

    def __call__(self, **kwargs: Any):
        """Allow property-style access to be called like a method."""
        return self.scatter(**kwargs)

    async def ascatter(
        self,
        *,
        x: Any = None,
        y: Any = None,
        color: Any = None,
        symbol: Any = None,
        size: Any = None,
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
        return await super().scatter(
            x=x,
            y=y,
            color=color,
            symbol=symbol,
            size=size,
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
    async def scatter(
        self,
        *,
        x: Any = None,
        y: Any = None,
        color: Any = None,
        symbol: Any = None,
        size: Any = None,
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
        return await self.ascatter(
            x=x,
            y=y,
            color=color,
            symbol=symbol,
            size=size,
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
