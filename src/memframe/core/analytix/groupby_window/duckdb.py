from __future__ import annotations

from memframe.core.analytix.groupby_window.base import GroupbyWindowOps


class DuckDBGroupbyWindowOps(GroupbyWindowOps):
    """DuckDB backend — inherits the DuckDB-flavoured defaults from base."""
