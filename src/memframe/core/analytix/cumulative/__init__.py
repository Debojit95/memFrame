from __future__ import annotations

from memframe.core.analytix.cumulative.base import CumulativeOps
from memframe.core.analytix.cumulative.duckdb import DuckDBCumulativeOps
from memframe.core.analytix.cumulative.postgres import PostgresCumulativeOps
from memframe.core.analytix.cumulative.clickhouse import ClickHouseCumulativeOps
from memframe.core.analytix.cumulative.factory import make_cumulative_ops

__all__ = [
    "CumulativeOps",
    "DuckDBCumulativeOps",
    "PostgresCumulativeOps",
    "ClickHouseCumulativeOps",
    "make_cumulative_ops",
]
