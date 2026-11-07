"""DuckDB single-writer lock handling."""

import asyncio

import duckdb
import pytest

from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.factory import resolve_backend_config
from memframe.db_manager.connection.pool import DuckDBPool, create_pool
from memframe.exceptions import ConfigurationError


def _lock_error(path):
    return duckdb.IOException(
        f'IO Error: Could not set lock on file "{path}": '
        "Conflicting lock is held in /usr/bin/duckdb (PID 123). "
        "See https://duckdb.org/docs/stable/connect/concurrency"
    )


def test_conflicting_lock_raises_actionable_error(monkeypatch):
    def fake_connect(path, *args, **kwargs):
        raise _lock_error(path)

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    pool = DuckDBPool("/tmp/locked.duckdb")

    with pytest.raises(ConfigurationError) as excinfo:
        asyncio.run(pool.connect())

    message = str(excinfo.value)
    assert "/tmp/locked.duckdb" in message
    assert "PID 123" in message
    assert "Close the DuckDB UI" in message
    assert "PostgreSQL or ClickHouse" in message


def test_lock_timeout_retries_then_connects(monkeypatch):
    class _Conn:
        def __init__(self):
            self.sql = []

        def execute(self, sql):
            self.sql.append(sql)

        def close(self):
            pass

    calls = {"n": 0}
    conn = _Conn()

    def fake_connect(path, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise _lock_error(path)
        return conn

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    pool = DuckDBPool("/tmp/locked.duckdb", lock_timeout=2)
    asyncio.run(pool.connect())

    assert calls["n"] == 3
    assert pool.conn is conn
    assert any("enable_progress_bar" in sql for sql in conn.sql)
    asyncio.run(pool.close())


def test_lock_timeout_survives_config_resolution():
    backend, params = resolve_backend_config(
        "local", {"db_path": "locked.duckdb", "lock_timeout": 3}
    )
    assert params["lock_timeout"] == 3
    assert DuckDBAdapter.connection_params(
        {"db_path": "locked.duckdb", "lock_timeout": 3}
    )["lock_timeout"] == 3

    pool = create_pool(backend, params)
    assert isinstance(pool, DuckDBPool)
    assert pool.lock_timeout == 3
