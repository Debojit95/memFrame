"""
Factory dispatching a DataSortingOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.sorting.duckdb import DuckDBSortingOps
from memframe.core.analytix.sorting.postgres import PostgresSortingOps
from memframe.core.analytix.sorting.clickhouse import ClickHouseSortingOps


def make_sorting_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBSortingOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresSortingOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseSortingOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for sorting operation: {db_adapter.__class__.__name__}"
    )
