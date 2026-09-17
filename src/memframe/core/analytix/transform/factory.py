"""
Factory dispatching a TransformOps subclass by adapter type.
"""

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.transform.duckdb import DuckDBTransformOps
from memframe.core.analytix.transform.postgres import PostgresTransformOps
from memframe.core.analytix.transform.clickhouse import ClickHouseTransformOps


def make_transform_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBTransformOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresTransformOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseTransformOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for transform operation: {db_adapter.__class__.__name__}"
    )
