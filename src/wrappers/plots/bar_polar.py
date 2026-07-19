from __future__ import annotations

from typing import Any

from core.orchestrator.plots.bar_polar import BarPolarOrchestrator
from utils.async_sync import async_to_sync


class BarPolarWrapper(BarPolarOrchestrator):
    """Public sync + async wrapper for bar polar (wind rose) plotting."""

    def __call__(self, **kwargs: Any):
        return self.bar_polar(**kwargs)

    async def abar_polar(
        self,
        *,
        r: Any = None,
        theta: Any = None,
        color: Any = None,
        pattern_shape: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        base: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        color_continuous_scale: Any = None,
        pattern_shape_sequence: Any = None,
        pattern_shape_map: Any = None,
        range_color: Any = None,
        color_continuous_midpoint: Any = None,
        barnorm: str | None = None,
        barmode: str = "relative",
        direction: str = "clockwise",
        start_angle: int = 90,
        range_r: Any = None,
        range_theta: Any = None,
        log_r: bool = False,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        **kwargs: Any,
    ):
        return await super().bar_polar(
            r=r, theta=theta, color=color, pattern_shape=pattern_shape,
            hover_name=hover_name, hover_data=hover_data, custom_data=custom_data,
            base=base, animation_frame=animation_frame, animation_group=animation_group,
            category_orders=category_orders, labels=labels,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            color_continuous_scale=color_continuous_scale,
            pattern_shape_sequence=pattern_shape_sequence,
            pattern_shape_map=pattern_shape_map,
            range_color=range_color,
            color_continuous_midpoint=color_continuous_midpoint,
            barnorm=barnorm, barmode=barmode,
            direction=direction, start_angle=start_angle,
            range_r=range_r, range_theta=range_theta,
            log_r=log_r,
            title=title, subtitle=subtitle,
            template=template, width=width, height=height,
            **kwargs,
        )

    @async_to_sync
    async def bar_polar(
        self,
        *,
        r: Any = None,
        theta: Any = None,
        color: Any = None,
        pattern_shape: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        base: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        color_continuous_scale: Any = None,
        pattern_shape_sequence: Any = None,
        pattern_shape_map: Any = None,
        range_color: Any = None,
        color_continuous_midpoint: Any = None,
        barnorm: str | None = None,
        barmode: str = "relative",
        direction: str = "clockwise",
        start_angle: int = 90,
        range_r: Any = None,
        range_theta: Any = None,
        log_r: bool = False,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        **kwargs: Any,
    ):
        return await self.abar_polar(
            r=r, theta=theta, color=color, pattern_shape=pattern_shape,
            hover_name=hover_name, hover_data=hover_data, custom_data=custom_data,
            base=base, animation_frame=animation_frame, animation_group=animation_group,
            category_orders=category_orders, labels=labels,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            color_continuous_scale=color_continuous_scale,
            pattern_shape_sequence=pattern_shape_sequence,
            pattern_shape_map=pattern_shape_map,
            range_color=range_color,
            color_continuous_midpoint=color_continuous_midpoint,
            barnorm=barnorm, barmode=barmode,
            direction=direction, start_angle=start_angle,
            range_r=range_r, range_theta=range_theta,
            log_r=log_r,
            title=title, subtitle=subtitle,
            template=template, width=width, height=height,
            **kwargs,
        )


__all__ = ["BarPolarWrapper"]