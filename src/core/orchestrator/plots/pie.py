from __future__ import annotations

from typing import Any

from core.plots.pie import PiePlotCore


class PieOrchestrator:
    """Orchestrator layer for pie chart plotting."""

    def __init__(self, memframe_ops_instance=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ops_parent = memframe_ops_instance
        self._memframe = getattr(memframe_ops_instance, "memframe", None)
        self._data_id = getattr(memframe_ops_instance, "_data_id", None)
        self._pie_ops = None

    async def _ensure_ops(self) -> PiePlotCore:
        if self._ops_parent is None:
            raise RuntimeError(
                "PieOrchestrator is not bound to a ContextManager instance."
            )
        if self._pie_ops is None:
            await self._ops_parent._ensure_adapter()
            self._pie_ops = PiePlotCore(self._ops_parent._adapter)
        return self._pie_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

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
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.pie(
            table,
            schema,
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


__all__ = ["PieOrchestrator"]