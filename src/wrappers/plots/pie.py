from __future__ import annotations

from typing import Any

from core.orchestrator.plots.pie import PieOrchestrator
from utils.async_sync import async_to_sync


class PieWrapper(PieOrchestrator):
    """Public sync + async wrapper for pie chart plotting."""

    def __call__(self, **kwargs: Any):
        return self.pie(**kwargs)

    async def apie(
        self,
        *,
        names: Any = None,
        values: Any = None,
        color: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        facet_col_wrap: int = 0,
        facet_row_spacing: float | None = None,
        facet_col_spacing: float | None = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        opacity: float | None = None,
        hole: float | None = None,
        **kwargs: Any,
    ):
        return await super().pie(
            names=names,
            values=values,
            color=color,
            facet_row=facet_row,
            facet_col=facet_col,
            facet_col_wrap=facet_col_wrap,
            facet_row_spacing=facet_row_spacing,
            facet_col_spacing=facet_col_spacing,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
            category_orders=category_orders,
            labels=labels,
            title=title,
            subtitle=subtitle,
            template=template,
            width=width,
            height=height,
            opacity=opacity,
            hole=hole,
            **kwargs,
        )

    @async_to_sync
    async def pie(
        self,
        *,
        names: Any = None,
        values: Any = None,
        color: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        facet_col_wrap: int = 0,
        facet_row_spacing: float | None = None,
        facet_col_spacing: float | None = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        opacity: float | None = None,
        hole: float | None = None,
        **kwargs: Any,
    ):
        return await self.apie(
            names=names,
            values=values,
            color=color,
            facet_row=facet_row,
            facet_col=facet_col,
            facet_col_wrap=facet_col_wrap,
            facet_row_spacing=facet_row_spacing,
            facet_col_spacing=facet_col_spacing,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
            category_orders=category_orders,
            labels=labels,
            title=title,
            subtitle=subtitle,
            template=template,
            width=width,
            height=height,
            opacity=opacity,
            hole=hole,
            **kwargs,
        )


__all__ = ["PieWrapper"]