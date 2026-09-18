from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.cumulative.duckdb import DuckDBCumulativeOps
from memframe.core.analytix.cumulative.postgres import PostgresCumulativeOps
from memframe.core.analytix.cumulative.clickhouse import ClickHouseCumulativeOps
from memframe.core.analytix.cumulative.base import CumulativeOps


def make_cumulative_ops(db_adapter) -> CumulativeOps:
    """Return the backend‑specific cumulative operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBCumulativeOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresCumulativeOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseCumulativeOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for cumulative operations: {db_adapter.__class__.__name__}"
    )
