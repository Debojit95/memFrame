from __future__ import annotations

from memframe.core.analytix.groupby_stats.base import GroupByStatsOps


class DuckDBGroupByStatsOps(GroupByStatsOps):
    """DuckDB backend — inherits the DuckDB-flavoured defaults from base."""
