from memframe.db_manager.setup.base import DatabaseBackend
from memframe.db_manager.setup.duckdb import DuckDBBackend
from memframe.db_manager.setup.postgres import PostgresBackend
from memframe.db_manager.setup.clickhouse import ClickHouseBackend
from memframe.core.ingestion.datatype_detector import Backend
from memframe.exceptions import BackendNotSupported

__all__ = [
    "DatabaseBackend",
    "DuckDBBackend",
    "PostgresBackend",
    "ClickHouseBackend",
    "Backend",
    "create_backend",
]


def create_backend(backend: Backend, conn_params: dict) -> DatabaseBackend:
    if backend == Backend.DUCKDB:
        return DuckDBBackend(conn_params)
    elif backend == Backend.POSTGRES:
        return PostgresBackend(conn_params)
    elif backend == Backend.CLICKHOUSE:
        return ClickHouseBackend(conn_params)
    else:
        raise BackendNotSupported(f"Unsupported backend: {backend}")
