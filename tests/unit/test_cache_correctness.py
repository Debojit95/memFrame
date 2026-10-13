"""Finding 6: deep cache must not replay stale or wrong tables."""

import asyncio

import pandas as pd
import pytest

from memframe.cache import record_call
from memframe.main import MemFrame


@pytest.fixture
def mf():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    try:
        yield m
    finally:
        asyncio.run(m.aclose())


@pytest.fixture
def uploaded(mf):
    ctx = mf.upload_df(pd.DataFrame({"x": [1, 2, 3]}), filename="cache_dataset")
    return ctx


def _ops_count(mf, data_id):
    return asyncio.run(mf._backend.fetchval(
        f"SELECT COUNT(*) FROM {mf._backend.transient_registry_table} "
        f"WHERE data_id = {mf._placeholder(1)}",
        data_id,
    ))


class TestCacheKeys:
    def test_content_hash_is_full_vector(self):
        from memframe.cache.cache_manager import CacheManager

        h1 = CacheManager._content_hash(pd.DataFrame({"x": [1, 2]}))
        h2 = CacheManager._content_hash(pd.DataFrame({"x": [2, 1]}))
        assert isinstance(h1, str) and len(h1) == 64  # sha256 hex, not a bare sum
        assert h1 != h2  # order matters
        assert h1 == CacheManager._content_hash(pd.DataFrame({"x": [1, 2]}))

    def test_repr_fallback_never_collides(self):
        from memframe.cache.cache_manager import CacheManager

        m = CacheManager()
        assert m._signature((object(),)) != m._signature((object(),))

    def test_context_manager_cache_key(self):
        from memframe.db_manager.context import ContextManager

        assert ContextManager(object(), data_id="abc123").cache_key() == ["abc123", None]
        assert ContextManager(object(), data_id="abc123", _table_override="ov").cache_key() == ["abc123", "ov"]

    def test_transient_registry_has_validity_columns(self, mf):
        rows = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_csv_registry' "
            "AND table_name = 'memframe_transient_registry'"
        ))
        assert {"base_fingerprint", "table_scope"} <= {r[0] for r in rows}


class DeepOps:
    """Minimal @record_call user hitting the DataFrame-persist path."""

    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self, i):
        await asyncio.sleep(0)
        return {"is_error": False, "result": pd.DataFrame({"n": [i]}), "n": i}


class TestFingerprint:
    def test_hit_then_stale_after_external_edit(self, mf, uploaded):
        data_id = uploaded._data_id
        ops = DeepOps(mf, data_id)

        first = asyncio.run(ops.do(1))
        assert "result_metadata" not in first
        assert _ops_count(mf, data_id) == 1

        second = asyncio.run(ops.do(1))
        assert second["result_metadata"]["from_cache"] is True
        assert _ops_count(mf, data_id) == 1  # HIT stores nothing new

        # Out-of-band edit changes the base: same call must MISS and heal.
        asyncio.run(mf._backend.execute(
            f'INSERT INTO memframe_upload."{data_id}" VALUES (4)'
        ))
        third = asyncio.run(ops.do(1))
        assert "result_metadata" not in third
        assert _ops_count(mf, data_id) == 1  # stale row evicted, fresh row stored

        fourth = asyncio.run(ops.do(1))
        assert fourth["result_metadata"]["from_cache"] is True


class ScopeOps:
    """Reads whichever override table its context points at."""

    def __init__(self, mf, ctx):
        self._memframe = mf
        self._data_id = ctx._data_id
        self._ops_parent = ctx

    @record_call(deep_cache=True)
    async def do(self, i):
        table, schema = await self._ops_parent._get_active_context()
        rows = await self._memframe._backend.fetch(f'SELECT x FROM "{schema}"."{table}"')
        return {"is_error": False, "result": pd.DataFrame(rows, columns=["x"])}


class TestOverrideScope:
    def test_same_parent_different_overrides_stay_isolated(self, mf, uploaded):
        from memframe.db_manager.context import ContextManager

        data_id = uploaded._data_id
        asyncio.run(mf._backend.execute(
            "CREATE TABLE memframe_transient.ov1 (x INTEGER); INSERT INTO memframe_transient.ov1 VALUES (1)"
        ))
        asyncio.run(mf._backend.execute(
            "CREATE TABLE memframe_transient.ov2 (x INTEGER); INSERT INTO memframe_transient.ov2 VALUES (2)"
        ))
        ops1 = ScopeOps(mf, ContextManager(mf, data_id=data_id, _table_override="ov1"))
        ops2 = ScopeOps(mf, ContextManager(mf, data_id=data_id, _table_override="ov2"))

        r1 = asyncio.run(ops1.do(0))
        r2 = asyncio.run(ops2.do(0))
        assert r1["result"]["x"].tolist() == [1]
        assert r2["result"]["x"].tolist() == [2]

        # Repeats HIT their own tables, not each other's.
        r1b = asyncio.run(ops1.do(0))
        r2b = asyncio.run(ops2.do(0))
        assert r1b["result"]["x"].tolist() == [1]
        assert r2b["result"]["x"].tolist() == [2]
        assert r1b["result_metadata"]["from_cache"] is True
        assert r2b["result_metadata"]["from_cache"] is True
