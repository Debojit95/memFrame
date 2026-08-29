"""
Cleaning operations subpackage.

DataCleaningOps (base.py) holds all shared logic and backend-specific branches;
DuckDB/Postgres/ClickHouse subclasses currently inherit it unchanged. Construct
via make_cleaning_ops(db_adapter) rather than instantiating directly.
"""

from memframe.core.analytix.cleaning.base import DataCleaningOps
from memframe.core.analytix.cleaning.duckdb import DuckDBCleaningOps
from memframe.core.analytix.cleaning.postgres import PostgresCleaningOps
from memframe.core.analytix.cleaning.clickhouse import ClickHouseCleaningOps
from memframe.core.analytix.cleaning.factory import make_cleaning_ops

__all__ = [
    "DataCleaningOps",
    "DuckDBCleaningOps",
    "PostgresCleaningOps",
    "ClickHouseCleaningOps",
    "make_cleaning_ops",
]
