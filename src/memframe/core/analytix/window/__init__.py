from __future__ import annotations

from memframe.core.analytix.window.base import WindowOps
from memframe.core.analytix.window.duckdb import DuckDBWindowOps
from memframe.core.analytix.window.postgres import PostgresWindowOps
from memframe.core.analytix.window.clickhouse import ClickHouseWindowOps
from memframe.core.analytix.window.factory import make_window_ops

__all__ = [
    "WindowOps",
    "DuckDBWindowOps",
    "PostgresWindowOps",
    "ClickHouseWindowOps",
    "make_window_ops",
]
