from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.groupby_window.duckdb import DuckDBGroupbyWindowOps
from memframe.core.analytix.groupby_window.postgres import PostgresGroupbyWindowOps
from memframe.core.analytix.groupby_window.clickhouse import ClickHouseGroupbyWindowOps
from memframe.core.analytix.groupby_window.base import GroupbyWindowOps


def make_groupby_window_ops(db_adapter) -> GroupbyWindowOps:
    """Return the backend‑specific group-by window operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBGroupbyWindowOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresGroupbyWindowOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseGroupbyWindowOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for groupby window operations: {db_adapter.__class__.__name__}"
    )
