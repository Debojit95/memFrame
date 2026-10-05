"""Index operations subpackage (metadata-only logical index).

DataIndexOps (base.py) holds the shared logic; all three dialect subclasses
inherit unchanged — the fill logic uses the portable islands idiom
(COUNT(col) + MAX(col) windows) and `nearest` resolves labels in Python.
Construct via make_index_ops(db_adapter) rather than instantiating directly.
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
