"""
Factory dispatching a DatetimeOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from .base import DatetimeOps
from .duckdb import DuckDBDatetimeOps
from .postgres import PostgresDatetimeOps
from .clickhouse import ClickHouseDatetimeOps


def make_datetime_ops(db_adapter) -> DatetimeOps:
    """Return the backend-specific DatetimeOps subclass for ``db_adapter``."""
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseDatetimeOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresDatetimeOps(db_adapter)
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBDatetimeOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for datetime operation: {db_adapter.__class__.__name__}"
    )
