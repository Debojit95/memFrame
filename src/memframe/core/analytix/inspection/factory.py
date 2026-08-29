from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from .base import GeneralTableOps
from .duckdb import DuckDBTableOps
from .postgres import PostgresTableOps
from .clickhouse import ClickHouseTableOps


def make_table_ops(db_adapter) -> GeneralTableOps:
    """Return the backend-specific GeneralTableOps subclass for ``db_adapter``."""
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseTableOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresTableOps(db_adapter)
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBTableOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for table operation: {db_adapter.__class__.__name__}"
    )
