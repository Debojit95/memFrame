from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.comparison.duckdb import DuckDBComparisonOps
from memframe.core.analytix.comparison.postgres import PostgresComparisonOps
from memframe.core.analytix.comparison.clickhouse import ClickHouseComparisonOps
from memframe.core.analytix.comparison.base import ComparisonOps


def make_comparison_ops(db_adapter) -> ComparisonOps:
    """Return the backend‑specific comparison operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBComparisonOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresComparisonOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseComparisonOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for comparison operations: {db_adapter.__class__.__name__}"
    )
