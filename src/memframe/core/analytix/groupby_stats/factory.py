from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.groupby_stats.duckdb import DuckDBGroupByStatsOps
from memframe.core.analytix.groupby_stats.postgres import PostgresGroupByStatsOps
from memframe.core.analytix.groupby_stats.clickhouse import ClickHouseGroupByStatsOps
from memframe.core.analytix.groupby_stats.base import GroupByStatsOps


def make_groupby_stats_ops(db_adapter) -> GroupByStatsOps:
    """Return the backend‑specific group-by stats operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBGroupByStatsOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresGroupByStatsOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseGroupByStatsOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for groupby stats operations: {db_adapter.__class__.__name__}"
    )
