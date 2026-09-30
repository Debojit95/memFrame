from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.filter_II.duckdb import DuckDBFilteringOps
from memframe.core.analytix.filter_II.postgres import PostgresFilteringOps
from memframe.core.analytix.filter_II.clickhouse import ClickHouseFilteringOps
from memframe.core.analytix.filter_II.base import DataFilteringOps


def make_filtering_ops(db_adapter, backend) -> DataFilteringOps:
    """Return the backend‑specific filtering operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBFilteringOps(db_adapter, backend)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresFilteringOps(db_adapter, backend)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseFilteringOps(db_adapter, backend)
    raise NotImplementedError(
        f"Unsupported database backend for filtering operation: {db_adapter.__class__.__name__}"
    )
