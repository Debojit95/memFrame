"""Database-native index operations.

Indexes are created, listed, and dropped by the backend itself; memFrame stores
no index metadata. ClickHouse additionally supports a physical ORDER BY rebuild.
"""

from memframe.core.analytix.index.base import DataIndexOps
from memframe.core.analytix.index.duckdb import DuckDBIndexOps
from memframe.core.analytix.index.postgres import PostgresIndexOps
from memframe.core.analytix.index.clickhouse import ClickHouseIndexOps
from memframe.core.analytix.index.factory import make_index_ops

__all__ = [
    "DataIndexOps",
    "DuckDBIndexOps",
    "PostgresIndexOps",
    "ClickHouseIndexOps",
    "make_index_ops",
]
