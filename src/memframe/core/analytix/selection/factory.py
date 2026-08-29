"""
Factory dispatching a DataSelectionOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.selection.duckdb import DuckDBSelectionOps
from memframe.core.analytix.selection.postgres import PostgresSelectionOps
from memframe.core.analytix.selection.clickhouse import ClickHouseSelectionOps


def make_selection_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBSelectionOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresSelectionOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseSelectionOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for selection operation: {db_adapter.__class__.__name__}"
    )
