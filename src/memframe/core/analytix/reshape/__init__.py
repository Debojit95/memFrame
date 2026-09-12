"""
Reshape operations subpackage.

ReshapingOps (base.py) holds shared logic with DuckDB-flavoured defaults;
Postgres/ClickHouse override small dialect hooks (_safe_numeric_expr,
_get_table_columns, _build_explode_sql, _filtered_agg_expr, transpose
unpivot source, _pct_rank_expr). Construct via make_reshaping_ops(db_adapter)
rather than instantiating directly.
"""

from memframe.core.analytix.reshape.base import ReshapingOps
from memframe.core.analytix.reshape.duckdb import DuckDBReshapingOps
from memframe.core.analytix.reshape.postgres import PostgresReshapingOps
from memframe.core.analytix.reshape.clickhouse import ClickHouseReshapingOps
from memframe.core.analytix.reshape.factory import make_reshaping_ops

__all__ = [
    "ReshapingOps",
    "DuckDBReshapingOps",
    "PostgresReshapingOps",
    "ClickHouseReshapingOps",
    "make_reshaping_ops",
]
