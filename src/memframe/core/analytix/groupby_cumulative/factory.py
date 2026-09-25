from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.groupby_cumulative.duckdb import DuckDBGroupbyCumulativeOps
from memframe.core.analytix.groupby_cumulative.postgres import PostgresGroupbyCumulativeOps
from memframe.core.analytix.groupby_cumulative.clickhouse import ClickHouseGroupbyCumulativeOps
from memframe.core.analytix.groupby_cumulative.base import GroupbyCumulativeOps


def make_groupby_cumulative_ops(db_adapter) -> GroupbyCumulativeOps:
    """Return the backend‑specific group-by cumulative operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBGroupbyCumulativeOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresGroupbyCumulativeOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseGroupbyCumulativeOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for groupby cumulative operations: {db_adapter.__class__.__name__}"
    )
