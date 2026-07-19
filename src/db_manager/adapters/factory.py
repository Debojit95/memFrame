from typing import Any, Dict, Tuple

from core.ingestion.datatype_detector import Backend

from .clickhouse import ClickHouseAdapter
from .duckdb import DuckDBAdapter
from .postgresql import PostgresAdapter


def resolve_backend_config(
    connection_type: str,
    connection_params: Dict[str, Any],
) -> Tuple[Backend, Dict[str, Any]]:
    if connection_type == "local":
        return Backend.DUCKDB, DuckDBAdapter.connection_params(connection_params)

    if connection_type != "remote":
        raise ValueError("connection_type must be 'local' or 'remote'")

    backend_type = connection_params.get("backend")
    if backend_type == "postgres":
        return Backend.POSTGRES, PostgresAdapter.connection_params(connection_params)
    if backend_type == "clickhouse":
        return Backend.CLICKHOUSE, ClickHouseAdapter.connection_params(connection_params)

    raise ValueError("Remote backend must be 'postgres' or 'clickhouse'")
