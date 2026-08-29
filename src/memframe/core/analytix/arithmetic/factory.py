from __future__ import annotations

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter

from memframe.core.analytix.arithmetic.duckdb import DuckDBArithmeticOps
from memframe.core.analytix.arithmetic.postgres import PostgresArithmeticOps
from memframe.core.analytix.arithmetic.clickhouse import ClickHouseArithmeticOps
from memframe.core.analytix.arithmetic.base import ArithmeticOps


def make_arithmetic_ops(db_adapter) -> ArithmeticOps:
    """Return the backend‑specific arithmetic operations object."""
    if isinstance(db_adapter, DuckDBAdapter):
        return DuckDBArithmeticOps(db_adapter)
    if isinstance(db_adapter, PostgresAdapter):
        return PostgresArithmeticOps(db_adapter)
    if isinstance(db_adapter, ClickHouseAdapter):
        return ClickHouseArithmeticOps(db_adapter)
    raise NotImplementedError(
        f"Unsupported database backend for arithmetic operations: {db_adapter.__class__.__name__}"
    )
