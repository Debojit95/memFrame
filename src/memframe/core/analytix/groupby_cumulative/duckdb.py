from __future__ import annotations

from memframe.core.analytix.groupby_cumulative.base import GroupbyCumulativeOps


class DuckDBGroupbyCumulativeOps(GroupbyCumulativeOps):
    """DuckDB backend — inherits the DuckDB-flavoured defaults from base."""
