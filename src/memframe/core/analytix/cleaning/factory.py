"""
Factory dispatching a DataCleaningOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.cleaning.duckdb import DuckDBCleaningOps
from memframe.core.analytix.cleaning.postgres import PostgresCleaningOps
from memframe.core.analytix.cleaning.clickhouse import ClickHouseCleaningOps


def make_cleaning_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBCleaningOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresCleaningOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseCleaningOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for cleaning operation: {db_adapter.__class__.__name__}"
    )
