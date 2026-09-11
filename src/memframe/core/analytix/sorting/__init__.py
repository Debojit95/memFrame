"""
Sorting operations subpackage.

DataSortingOps (base.py) holds shared logic with DuckDB-flavoured defaults;
ClickHouse overrides two small dialect hooks (_order_term,
_create_sort_table_sql). DuckDB/Postgres inherit unchanged. Construct via
make_sorting_ops(db_adapter) rather than instantiating directly.
"""

from memframe.core.analytix.sorting.base import DataSortingOps
from memframe.core.analytix.sorting.duckdb import DuckDBSortingOps
from memframe.core.analytix.sorting.postgres import PostgresSortingOps
from memframe.core.analytix.sorting.clickhouse import ClickHouseSortingOps
from memframe.core.analytix.sorting.factory import make_sorting_ops

__all__ = [
    "DataSortingOps",
    "DuckDBSortingOps",
    "PostgresSortingOps",
    "ClickHouseSortingOps",
    "make_sorting_ops",
]
