"""Pydantic models describing an AI-designed dashboard layout.

Dependency-light (pydantic only) so both the core HTML renderer
(``memframe``) and the designer agent (``memframe_ai``) can import them
without pulling in pydantic-ai.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field


class ChartType(str, Enum):
    LINE = "line"
    BAR = "bar"
    SCATTER = "scatter"
    PIE = "pie"
    HISTOGRAM = "histogram"
    BOX = "box"
    KEEP_EXISTING = "keep_existing"


class FigureDesign(BaseModel):
    chart_type: ChartType = ChartType.KEEP_EXISTING
    x: Optional[str] = None
    y: Optional[str] = None
    width: int = Field(default=600, ge=300, le=1200)
    height: int = Field(default=400, ge=250, le=800)
    title: str = ""
    subtitle: Optional[str] = None
    xaxis_title: Optional[str] = None
    yaxis_title: Optional[str] = None
    legend_title: Optional[str] = None
    color_scale: Literal["Viridis", "Cividis", "Plasma", "Blues", "Reds", "Set2"] = "Viridis"
    show_legend: bool = True


class MetricDesign(BaseModel):
    prefix: Optional[str] = None
    suffix: Optional[str] = None
    decimal_places: int = 2
    font_size: int = 48
    color: Literal["primary", "success", "danger", "warning", "info"] = "primary"


class TableDesign(BaseModel):
    page_size: int = 10
    max_rows: Optional[int] = None


class WidgetDesign(BaseModel):
    result_index: int
    kind: Literal["plot", "table", "metric", "text"]
    col_span: int = Field(default=6, ge=1, le=12)
    title: str = ""
    description: Optional[str] = None
    plot_design: Optional[FigureDesign] = None
    metric_design: Optional[MetricDesign] = None
    table_design: Optional[TableDesign] = None


class DashboardDesign(BaseModel):
    dashboard_title: str = "Dashboard"
    global_theme: Literal["light", "dark"] = "light"
    widgets: list[WidgetDesign]
