"""
Factory dispatching a ReshapingOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.reshape.duckdb import DuckDBReshapingOps
from memframe.core.analytix.reshape.postgres import PostgresReshapingOps
from memframe.core.analytix.reshape.clickhouse import ClickHouseReshapingOps


def make_reshaping_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBReshapingOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresReshapingOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseReshapingOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for reshape operation: {db_adapter.__class__.__name__}"
    )
