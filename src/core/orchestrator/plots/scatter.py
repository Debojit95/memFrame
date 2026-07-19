from __future__ import annotations

from typing import Any
from core.plots.scatter import ScatterPlotCore


class ScatterOrchestrator:
    """Orchestrator layer for scatter plotting."""

    def __init__(self, memframe_ops_instance=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ops_parent = memframe_ops_instance
        self._memframe = getattr(memframe_ops_instance, "memframe", None)
        self._data_id = getattr(memframe_ops_instance, "_data_id", None)
        self._scatter_ops = None


    async def _ensure_ops(self) -> ScatterPlotCore:
        if self._ops_parent is None:
            raise RuntimeError(
                "ScatterOrchestrator is not bound to a ContextManager instance."
            )
        if self._scatter_ops is None:
            await self._ops_parent._ensure_adapter()
            self._scatter_ops = ScatterPlotCore(self._ops_parent._adapter)
        return self._scatter_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

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
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.scatter(
            table,
            schema,
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
