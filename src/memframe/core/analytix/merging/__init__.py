"""
Merge operations subpackage.

``DataMergeOps`` (base.py) holds the shared merge/join/concat SQL with
DuckDB/PostgreSQL-flavoured defaults; ClickHouse overrides two small dialect
hooks (``_auto_cast_join_columns``, ``_create_table_as``). Construct via
``make_merge_ops(db_adapter)`` rather than instantiating directly.
"""

from memframe.core.analytix.merging.base import DataMergeOps
from memframe.core.analytix.merging.duckdb import DuckDBMergeOps
from memframe.core.analytix.merging.postgres import PostgresMergeOps
from memframe.core.analytix.merging.clickhouse import ClickHouseMergeOps
from memframe.core.analytix.merging.factory import make_merge_ops

__all__ = [
    "DataMergeOps",
    "DuckDBMergeOps",
    "PostgresMergeOps",
    "ClickHouseMergeOps",
    "make_merge_ops",
]
