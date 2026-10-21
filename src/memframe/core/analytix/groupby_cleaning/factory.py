from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.groupby_cleaning.duckdb import DuckDBGroupByCleaningOps
from memframe.core.analytix.groupby_cleaning.postgres import PostgresGroupByCleaningOps
from memframe.core.analytix.groupby_cleaning.clickhouse import ClickHouseGroupByCleaningOps


def make_groupby_cleaning_ops(db_adapter):
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBGroupByCleaningOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresGroupByCleaningOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseGroupByCleaningOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for groupby cleaning operations: {db_adapter.__class__.__name__}"
    )
