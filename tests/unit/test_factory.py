import pytest

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.adapters.factory import resolve_backend_config
from memframe.db_manager.setup import create_backend
from memframe.db_manager.connection import create_pool
from memframe.exceptions import ConfigurationError


def test_resolve_local_is_duckdb():
    backend, params = resolve_backend_config("local", {})
    assert backend == Backend.DUCKDB
    assert "db_path" in params


def test_resolve_remote_postgres():
    backend, params = resolve_backend_config(
        "remote", {"backend": "postgres", "host": "h", "user": "u", "password": "p", "database": "d"}
    )
    assert backend == Backend.POSTGRES
    assert params["host"] == "h"
    assert params["port"] == 5432
    assert params["database"] == "d"


def test_resolve_remote_clickhouse():
    backend, params = resolve_backend_config(
        "remote", {"backend": "clickhouse", "host": "h", "user": "u", "password": "p"}
    )
    assert backend == Backend.CLICKHOUSE


def test_resolve_unknown_connection_type_raises():
    with pytest.raises(ConfigurationError):
        resolve_backend_config("cloud", {})


def test_resolve_unknown_remote_backend_raises():
    with pytest.raises(ConfigurationError):
        resolve_backend_config("remote", {"backend": "mysql"})


def test_resolve_remote_missing_backend_raises():
    with pytest.raises(ConfigurationError):
        resolve_backend_config("remote", {"host": "h"})


def test_create_backend_duckdb():
    from memframe.db_manager.setup import DuckDBBackend
    assert isinstance(create_backend(Backend.DUCKDB, {"db_path": ":memory:"}), DuckDBBackend)


def test_create_backend_unknown_raises():
    with pytest.raises(Exception):
        create_backend("mysql", {})


def test_create_pool_duckdb():
    from memframe.db_manager.connection import DuckDBPool
    assert isinstance(create_pool(Backend.DUCKDB, {"db_path": ":memory:"}), DuckDBPool)


def test_create_pool_unknown_raises():
    with pytest.raises(Exception):
        create_pool("mysql", {})
