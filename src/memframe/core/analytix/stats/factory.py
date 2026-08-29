from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.stats.duckdb import DuckDBDataStatsOps
from memframe.core.analytix.stats.postgres import PostgresDataStatsOps
from memframe.core.analytix.stats.clickhouse import ClickHouseDataStatsOps
from memframe.core.analytix.stats.base import DataStatsOps


def make_stats_ops(db_adapter) -> DataStatsOps:
    """Return the backend‑specific statistics operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBDataStatsOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresDataStatsOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseDataStatsOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for stats operations: {db_adapter.__class__.__name__}"
    )
