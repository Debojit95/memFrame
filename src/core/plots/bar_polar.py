from __future__ import annotations

from typing import Any

import pandas as pd

from src.db_manager.adapters.base import DatabaseAdapter
from src.utils.helper import SQLIdentifierSanitizer

try:
    import plotly.express as px
except ImportError:
    px = None


class BarPolarPlotCore:
    """Core bar polar chart engine – fetches data and delegates to plotly.express.bar_polar."""

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table, allow_qualified=False)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema, allow_qualified=False)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    def _ensure_plotly(self) -> None:
        if px is None:
            raise ImportError(
                "Plotly is required for plotting. Install it with: pip install plotly"
            )

    def _collect_column_value(self, value: Any, out: set[str]) -> None:
        if isinstance(value, str) and value:
            out.add(SQLIdentifierSanitizer.sanitize(value, allow_qualified=False))
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if isinstance(item, str) and item:
                    out.add(SQLIdentifierSanitizer.sanitize(item, allow_qualified=False))

    def _extract_requested_columns(
        self,
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
        **_,
    ) -> list[str] | None:
        requested: set[str] = set()
        for value in (r, theta, color, pattern_shape, hover_name, base,
                      animation_frame, animation_group, custom_data):
            self._collect_column_value(value, requested)
        if isinstance(hover_data, dict):
            self._collect_column_value(list(hover_data.keys()), requested)
        else:
            self._collect_column_value(hover_data, requested)
        return sorted(requested) if requested else None

    async def _fetch_dataframe(
        self,
        table: str,
        schema: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        column_types = await self.db.get_column_types(table, schema)
        available_columns = list(column_types.keys())

        if columns is None:
            selected = available_columns
        else:
            selected = [col for col in columns if col in column_types]
            if not selected:
                selected = available_columns

        if selected:
            quoted_cols = [
                self.db.quote_identifier(
                    SQLIdentifierSanitizer.sanitize(col, allow_qualified=False)
                )
                for col in selected
            ]
            column_clause = ", ".join(quoted_cols)
        else:
            column_clause = "*"

        rows = await self.db.fetch(f"SELECT {column_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        if not records and selected:
            return pd.DataFrame(columns=selected)
        return pd.DataFrame.from_records(records)

    async def bar_polar(
        self,
        table: str,
        schema: str,
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
    ) -> Any:
        """Build a bar polar (wind rose) chart using plotly.express.bar_polar."""
        self._ensure_plotly()

        if "data_frame" in kwargs:
            raise ValueError(
                "Do not pass 'data_frame' to ops.bar_polar(); it is derived from the active context."
            )

        requested_columns = self._extract_requested_columns(
            r=r, theta=theta, color=color, pattern_shape=pattern_shape,
            hover_name=hover_name, hover_data=hover_data, custom_data=custom_data,
            base=base, animation_frame=animation_frame, animation_group=animation_group,
        )
        df = await self._fetch_dataframe(table=table, schema=schema, columns=requested_columns)

        return px.bar_polar(
            df,
            r=r,
            theta=theta,
            color=color,
            pattern_shape=pattern_shape,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
            base=base,
            animation_frame=animation_frame,
            animation_group=animation_group,
            category_orders=category_orders,
            labels=labels,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            color_continuous_scale=color_continuous_scale,
            pattern_shape_sequence=pattern_shape_sequence,
            pattern_shape_map=pattern_shape_map,
            range_color=range_color,
            color_continuous_midpoint=color_continuous_midpoint,
            barnorm=barnorm,
            barmode=barmode,
            direction=direction,
            start_angle=start_angle,
            range_r=range_r,
            range_theta=range_theta,
            log_r=log_r,
            title=title,
            subtitle=subtitle,
            template=template,
            width=width,
            height=height,
            **kwargs,
        )


__all__ = ["BarPolarPlotCore"]