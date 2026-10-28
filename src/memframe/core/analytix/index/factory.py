"""Factory dispatching a DataIndexOps subclass by adapter type."""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.index.base import DataIndexOps
from memframe.core.analytix.index.duckdb import DuckDBIndexOps
from memframe.core.analytix.index.postgres import PostgresIndexOps
from memframe.core.analytix.index.clickhouse import ClickHouseIndexOps


def make_index_ops(db_adapter) -> DataIndexOps:
    """Return the backend-specific DataIndexOps subclass for ``db_adapter``."""
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseIndexOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresIndexOps(db_adapter)
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBIndexOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for index operation: {db_adapter.__class__.__name__}"
    )
