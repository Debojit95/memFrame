"""
Selection operations subpackage.

DataSelectionOps (base.py) holds all shared logic plus backend-specific hooks;
DuckDB/Postgres/ClickHouse subclasses override only the divergent hooks.
Construct via make_selection_ops(db_adapter) rather than instantiating directly.
"""

from memframe.core.analytix.selection.base import DataSelectionOps
from memframe.core.analytix.selection.duckdb import DuckDBSelectionOps
from memframe.core.analytix.selection.postgres import PostgresSelectionOps
from memframe.core.analytix.selection.clickhouse import ClickHouseSelectionOps
from memframe.core.analytix.selection.factory import make_selection_ops

__all__ = [
    "DataSelectionOps",
    "DuckDBSelectionOps",
    "PostgresSelectionOps",
    "ClickHouseSelectionOps",
    "make_selection_ops",
]
