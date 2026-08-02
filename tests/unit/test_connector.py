import asyncio

import pytest

from memframe.db_manager.connection import ConnectorManager
from memframe.db_manager.context import ContextManager


def _make_connector(**params):
    return ConnectorManager(
        "local",
        params or {"db_path": ":memory:"},
        context_factory=lambda data_id: ContextManager(object(), data_id=data_id),
    )


class TestConnectorLifecycle:
    def test_initial_state(self):
        conn = _make_connector()
        assert conn.backend is None
        assert conn.pool is None

    def test_aconnect_initializes_backend_and_pool(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        assert conn.backend is not None
        assert conn.backend.backend == "duckdb"
        assert conn.pool is not None
        asyncio.run(conn.close())

    def test_close_resets_state(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        asyncio.run(conn.close())
        assert conn.backend is None
        assert conn.pool is None

    def test_close_idempotent(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        asyncio.run(conn.close())
        asyncio.run(conn.close())

    def test_is_duckdb_after_connect(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        assert conn.is_duckdb() is True
        asyncio.run(conn.close())

    def test_is_duckdb_before_connect(self):
        assert _make_connector().is_duckdb() is False


class TestConnectorPlaceholder:
    def test_placeholder_before_connect_raises(self):
        conn = _make_connector()
        with pytest.raises(RuntimeError):
            conn._placeholder(1)

    def test_placeholder_after_connect(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        assert conn._placeholder(1) == "?"
        asyncio.run(conn.close())


class TestConnectorUploader:
    def test_uploader_wired_with_backend(self):
        conn = _make_connector()
        asyncio.run(conn.aconnect())
        uploader = conn._uploader
        assert uploader._backend is conn.backend
        assert uploader._type_detector is conn.backend._type_detector
        assert uploader._memframe_from_data_id is not None
        asyncio.run(conn.close())
