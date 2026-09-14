"""
Factory dispatching a DataMergeOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.merging.duckdb import DuckDBMergeOps
from memframe.core.analytix.merging.postgres import PostgresMergeOps
from memframe.core.analytix.merging.clickhouse import ClickHouseMergeOps


def make_merge_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBMergeOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresMergeOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseMergeOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for merge operation: {db_adapter.__class__.__name__}"
    )
