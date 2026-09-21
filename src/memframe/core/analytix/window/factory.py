from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.window.duckdb import DuckDBWindowOps
from memframe.core.analytix.window.postgres import PostgresWindowOps
from memframe.core.analytix.window.clickhouse import ClickHouseWindowOps
from memframe.core.analytix.window.base import WindowOps


def make_window_ops(db_adapter) -> WindowOps:
    """Return the backend‑specific window operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBWindowOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresWindowOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseWindowOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for window operations: {db_adapter.__class__.__name__}"
    )
