from __future__ import annotations

from typing import Any

from core.plots.scatter_3d import Scatter3DPlotCore


class Scatter3DOrchestrator:
    """Orchestrator layer for 3D scatter plotting."""

    def __init__(self, memframe_ops_instance=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ops_parent = memframe_ops_instance
        self._memframe = getattr(memframe_ops_instance, "memframe", None)
        self._data_id = getattr(memframe_ops_instance, "_data_id", None)
        self._scatter_3d_ops = None

    async def _ensure_ops(self) -> Scatter3DPlotCore:
        if self._ops_parent is None:
            raise RuntimeError(
                "Scatter3DOrchestrator is not bound to a ContextManager instance."
            )
        if self._scatter_3d_ops is None:
            await self._ops_parent._ensure_adapter()
            self._scatter_3d_ops = Scatter3DPlotCore(self._ops_parent._adapter)
        return self._scatter_3d_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    async def scatter_3d(
        self,
        *,
        x: Any = None,
        y: Any = None,
        z: Any = None,
        color: Any = None,
        symbol: Any = None,
        size: Any = None,
        text: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        error_x: Any = None,
        error_x_minus: Any = None,
        error_y: Any = None,
        error_y_minus: Any = None,
        error_z: Any = None,
        error_z_minus: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        size_max: int | None = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        color_continuous_scale: Any = None,
        range_color: Any = None,
        color_continuous_midpoint: Any = None,
        symbol_sequence: Any = None,
        symbol_map: Any = None,
        opacity: float | None = None,
        log_x: bool = False,
        log_y: bool = False,
        log_z: bool = False,
        range_x: Any = None,
        range_y: Any = None,
        range_z: Any = None,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        **kwargs: Any,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.scatter_3d(
            table,
            schema,
            x=x,
            y=y,
            z=z,
            color=color,
            symbol=symbol,
            size=size,
            text=text,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
            error_x=error_x,
            error_x_minus=error_x_minus,
            error_y=error_y,
            error_y_minus=error_y_minus,
            error_z=error_z,
            error_z_minus=error_z_minus,
            animation_frame=animation_frame,
            animation_group=animation_group,
            category_orders=category_orders,
            labels=labels,
            size_max=size_max,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            color_continuous_scale=color_continuous_scale,
            range_color=range_color,
            color_continuous_midpoint=color_continuous_midpoint,
            symbol_sequence=symbol_sequence,
            symbol_map=symbol_map,
            opacity=opacity,
            log_x=log_x,
            log_y=log_y,
            log_z=log_z,
            range_x=range_x,
            range_y=range_y,
            range_z=range_z,
            title=title,
            subtitle=subtitle,
            template=template,
            width=width,
            height=height,
            **kwargs,
        )


__all__ = ["Scatter3DOrchestrator"]