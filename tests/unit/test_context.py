import asyncio

import pytest

from memframe.db_manager.context import ContextManager
from memframe.exceptions import ConnectionNotReady, DataNotFound


class _FakeMemFrame:
    """Minimal MemFrame stand-in exposing only what ContextManager touches."""

    def __init__(self, backend=None, pool=None, active_id=None):
        self._backend = backend
        self._pool = pool
        self._active_id = active_id


def _make_ctx(memframe=None, data_id=None):
    mf = memframe or _FakeMemFrame()
    ctx = ContextManager(mf, data_id=data_id)
    return ctx, mf


class TestAttrDispatch:
    def test_unknown_attribute_raises(self):
        ctx, _ = _make_ctx()
        with pytest.raises(AttributeError):
            ctx.this_does_not_exist

    def test_dir_includes_wrapper_methods(self):
        ctx, _ = _make_ctx()
        assert "head" in ctx.__dir__()


class TestActiveContext:
    def test_no_active_id_raises(self):
        ctx, _ = _make_ctx()
        with pytest.raises(DataNotFound):
            asyncio.run(ctx._get_active_context())

    def test_data_id_from_memframe_active(self, monkeypatch):
        mf = _FakeMemFrame(active_id="abc123")

        class _Backend:
            csv_registry_table = "memframe_csv_registry.memframe_csv_registry"
            upload_schema = "memframe_upload"
            placeholder = lambda self, i: "?"

            async def fetch(self, q, *params):
                assert params == ("abc123",)
                return [("abc123", "memframe_upload")]

        mf._backend = _Backend()
        ctx, _ = _make_ctx(mf)
        table, schema = asyncio.run(ctx._get_active_context())
        assert table == "abc123"
        assert schema == "memframe_upload"


class TestEnsureAdapter:
    def test_requires_backend_and_pool(self):
        ctx, _ = _make_ctx()
        with pytest.raises(ConnectionNotReady):
            asyncio.run(ctx._ensure_adapter())

    def test_backend_without_pool_raises(self):
        mf = _FakeMemFrame(backend=object())
        ctx, _ = _make_ctx(mf)
        with pytest.raises(ConnectionNotReady):
            asyncio.run(ctx._ensure_adapter())
