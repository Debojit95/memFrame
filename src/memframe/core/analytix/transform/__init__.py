"""
Transform operations subpackage.

TransformOps (base.py) holds shared helpers and DuckDB-flavoured defaults;
Postgres/ClickHouse subclasses override where SQL differs.
Construct via make_transform_ops(db_adapter) rather than instantiating directly.
"""

from memframe.core.analytix.transform.base import TransformOps
from memframe.core.analytix.transform.duckdb import DuckDBTransformOps
from memframe.core.analytix.transform.postgres import PostgresTransformOps
from memframe.core.analytix.transform.clickhouse import ClickHouseTransformOps
from memframe.core.analytix.transform.factory import make_transform_ops

__all__ = [
    "TransformOps",
    "DuckDBTransformOps",
    "PostgresTransformOps",
    "ClickHouseTransformOps",
    "make_transform_ops",
]
