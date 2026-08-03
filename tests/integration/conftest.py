# tests/integration/conftest.py
# Shared backend configuration and fixtures for Layer 4 integration tests.

import asyncio
import json
import os
from typing import Any, Dict, List

import pandas as pd
import pytest

from memframe.main import MemFrame

LOCAL_DB = "local"
REMOTE_DB = "remote"
DUCKDB_BACKEND = "duckdb"
POSTGRES_BACKEND = "postgres"
CLICKHOUSE_BACKEND = "clickhouse"

BACKEND_PARAMS = {
    LOCAL_DB: {"connection_type": "local", "params": {}},
    REMOTE_DB: {
        "connection_type": "remote",
        "params": {
            "backend": "postgres",
            "host": os.getenv("PGHOST", "localhost"),
            "port": int(os.getenv("PGPORT", 5432)),
            "user": os.getenv("PGUSER", "postgres"),
            "password": os.getenv("PGPASSWORD", "postgres"),
            "database": os.getenv("PGDATABASE", "memframe_test"),
        },
    },
}

BACKEND_ALIASES = {
    LOCAL_DB: DUCKDB_BACKEND,
    REMOTE_DB: POSTGRES_BACKEND,
    DUCKDB_BACKEND: DUCKDB_BACKEND,
    POSTGRES_BACKEND: POSTGRES_BACKEND,
    CLICKHOUSE_BACKEND: CLICKHOUSE_BACKEND,
}

TEST_BACKENDS = [
    backend.strip()
    for backend in os.getenv("MEMFRAME_TEST_BACKENDS", "local").split(",")
    if backend.strip()
]


def _usage_error(message: str) -> pytest.UsageError:
    return pytest.UsageError(f"Invalid integration DB configuration: {message}")


def _parse_connection_params(raw_params: str) -> Dict[str, Any]:
    if not raw_params:
        return {}
    try:
        params = json.loads(raw_params)
    except json.JSONDecodeError as exc:
        raise _usage_error(f"--db-params must be valid JSON: {exc}") from exc
    if not isinstance(params, dict):
        raise _usage_error("--db-params must be a JSON object")
    return params


def _normalize_backend_name(backend_name: str) -> str:
    normalized = str(backend_name).strip().lower()
    if normalized not in BACKEND_ALIASES:
        allowed = ", ".join(sorted(BACKEND_ALIASES))
        raise _usage_error(f"unknown backend '{backend_name}'. Use one of: {allowed}")
    return BACKEND_ALIASES[normalized]


def _infer_backend_from_params(params: Dict[str, Any]) -> str:
    backend = params.get("backend")
    if backend is not None:
        return _normalize_backend_name(str(backend))
    if "db_path" in params:
        return DUCKDB_BACKEND
    raise _usage_error("--db-params was provided without --db-backend")


def _validate_duckdb_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"db_path"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"DuckDB does not accept params: {', '.join(unknown)}")
    db_path = params.get("db_path", "memFrame_new.duckdb")
    if not isinstance(db_path, str) or not db_path.strip():
        raise _usage_error("DuckDB param 'db_path' must be a non-empty string")
    return {"db_path": db_path}


def _validate_port(value: Any) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise _usage_error("Postgres/ClickHouse param 'port' must be an integer") from exc
    if port < 1 or port > 65535:
        raise _usage_error("Postgres/ClickHouse param 'port' must be between 1 and 65535")
    return port


def _validate_postgres_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"backend", "host", "port", "user", "password", "database", "schema_prefix"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"Postgres does not accept params: {', '.join(unknown)}")
    merged = dict(BACKEND_PARAMS[REMOTE_DB]["params"])
    merged.update(params)
    merged["backend"] = POSTGRES_BACKEND
    for key in ("host", "user", "database"):
        value = merged.get(key)
        if not isinstance(value, str) or not value.strip():
            raise _usage_error(f"Postgres param '{key}' must be a non-empty string")
    password = merged.get("password")
    if not isinstance(password, str):
        raise _usage_error("Postgres param 'password' must be a string")
    merged["port"] = _validate_port(merged.get("port", 5432))
    return merged


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _validate_clickhouse_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"backend", "host", "port", "user", "password", "database", "secure", "timeout", "schema_prefix"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"ClickHouse does not accept params: {', '.join(unknown)}")
    merged: Dict[str, Any] = {
        "backend": CLICKHOUSE_BACKEND,
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv("CLICKHOUSE_PORT", 8123),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "secure": _env_bool("CLICKHOUSE_SECURE", False),
    }
    if os.getenv("CLICKHOUSE_DATABASE"):
        merged["database"] = os.getenv("CLICKHOUSE_DATABASE")
    if os.getenv("CLICKHOUSE_TIMEOUT"):
        merged["timeout"] = os.getenv("CLICKHOUSE_TIMEOUT")
    merged.update(params)
    merged["backend"] = CLICKHOUSE_BACKEND
    for key in ("host", "user"):
        value = merged.get(key)
        if not isinstance(value, str) or not value.strip():
            raise _usage_error(f"ClickHouse param '{key}' must be a non-empty string")
    password = merged.get("password")
    if not isinstance(password, str):
        raise _usage_error("ClickHouse param 'password' must be a string")
    database = merged.get("database")
    if database is not None and (not isinstance(database, str) or not database.strip()):
        raise _usage_error("ClickHouse param 'database' must be a non-empty string")
    secure = merged.get("secure", False)
    if isinstance(secure, str):
        secure = secure.strip().lower() in {"1", "true", "yes", "on"}
    if not isinstance(secure, bool):
        raise _usage_error("ClickHouse param 'secure' must be a boolean")
    merged["secure"] = secure
    if "timeout" in merged:
        try:
            merged["timeout"] = float(merged["timeout"])
        except (TypeError, ValueError) as exc:
            raise _usage_error("ClickHouse param 'timeout' must be a number") from exc
    merged["port"] = _validate_port(merged.get("port", 8123))
    return merged


def _build_backend_config(backend_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    backend = _normalize_backend_name(backend_name)
    if backend == DUCKDB_BACKEND:
        return {
            "backend": DUCKDB_BACKEND,
            "connection_type": "local",
            "params": _validate_duckdb_params(params),
        }
    if backend == POSTGRES_BACKEND:
        return {
            "backend": POSTGRES_BACKEND,
            "connection_type": "remote",
            "params": _validate_postgres_params(params),
        }
    return {
        "backend": CLICKHOUSE_BACKEND,
        "connection_type": "remote",
        "params": _validate_clickhouse_params(params),
    }


def _selected_backend_configs(config) -> List[Dict[str, Any]]:
    raw_params = config.getoption("--db-params")
    params = _parse_connection_params(raw_params)
    cli_backend = config.getoption("--db-backend")

    if cli_backend:
        return [_build_backend_config(cli_backend, params)]
    if raw_params:
        return [_build_backend_config(_infer_backend_from_params(params), params)]
    return [_build_backend_config(backend_name, {}) for backend_name in TEST_BACKENDS]


def pytest_generate_tests(metafunc):
    if "backend_config" not in metafunc.fixturenames:
        return
    if getattr(metafunc.module, "pytest_generate_tests", None):
        return
    configs = _selected_backend_configs(metafunc.config)
    ids = [config["backend"] for config in configs]
    metafunc.parametrize("backend_config", configs, ids=ids, indirect=True)


@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """Reference DataFrame for integration tests."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4],
        "a": [10, 20, 30, 40],
        "b": [1.5, 2.5, 3.5, 4.5],
        "cat": ["x", "y", "x", "y"],
        "flag": [True, False, True, False],
    })


@pytest.fixture(scope="function")
def backend_config(request) -> Dict[str, Any]:
    """Return the connection configuration for the current test."""
    config = getattr(request, "param", None)
    if config is None:
        config = _selected_backend_configs(request.config)[0]
    return {
        "backend": config["backend"],
        "connection_type": config["connection_type"],
        "params": dict(config.get("params", {})),
    }


@pytest.fixture(scope="function")
def connected_memframe(backend_config) -> MemFrame:
    """Create a MemFrame connected to the desired backend."""
    mf = MemFrame(
        connection_type=backend_config["connection_type"],
        connection_params=backend_config.get("params", {}),
    )
    asyncio.run(mf.aconnect())
    try:
        yield mf
    finally:
        asyncio.run(mf.aclose())


@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, sample_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(sample_df, filename="integration_dataset")


@pytest.fixture(scope="function")
def get_result_df():
    """Return the helper that extracts a pandas DataFrame from a result."""

    def _get_result_df(result: Any) -> pd.DataFrame:
        if isinstance(result, pd.DataFrame):
            return result
        if hasattr(result, "full_table"):
            return _get_result_df(result.full_table())
        if hasattr(result, "to_pandas"):
            return result.to_pandas()
        if hasattr(result, "collect"):
            collected = result.collect()
            if isinstance(collected, pd.DataFrame):
                return collected
        if isinstance(result, dict):
            if result.get("is_error"):
                raise AssertionError(result.get("error_message") or f"Operation failed: {result}")
            if "result" in result and isinstance(result["result"], pd.DataFrame):
                return result["result"]
            if "data" in result and isinstance(result["data"], pd.DataFrame):
                return result["data"]
        raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")

    return _get_result_df
