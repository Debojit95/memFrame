"""Dashboard API: collect query results, design a layout, render as HTML.

Public surface: ``memframe.dashboard.DashboardManager`` (see ``manager.py``).
The Pydantic models here are dependency-light (pydantic only) so both the core
renderer and the ``memframe_ai`` designer agent can import them without pulling
in pydantic-ai.
"""

from memframe.dashboard.manager import DashboardManager
from memframe.dashboard.models import (
    ChartType,
    DashboardDesign,
    FigureDesign,
    MetricDesign,
    TableDesign,
    WidgetDesign,
)

__all__ = [
    "DashboardManager",
    "ChartType",
    "DashboardDesign",
    "FigureDesign",
    "MetricDesign",
    "TableDesign",
    "WidgetDesign",
]
