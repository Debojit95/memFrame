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
        f"SELECT COUNT(*) FROM {mf._backend.cache_registry_table} "
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

    def test_cache_registry_has_validity_columns(self, mf):
        rows = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_csv_registry' "
            "AND table_name = 'memframe_cache_registry'"
        ))
        assert {"base_fingerprint", "table_scope"} <= {r[0] for r in rows}

    def test_cache_registry_has_actual_ref_columns(self, mf):
        rows = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_csv_registry' "
            "AND table_name = 'memframe_cache_registry'"
        ))
        expected = {
            "cache_kind", "ref_schema", "ref_table", "involved_cols",
            "generated_cols", "result_cols", "owns_table", "epoch", "input_refs",
            "order_schema", "order_table", "order_position_dtype",
            "order_kind", "order_limit",
        }
        assert expected <= {r[0] for r in rows}

    def test_csv_registry_has_index_metadata(self, mf):
        rows = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_csv_registry' "
            "AND table_name = 'memframe_csv_registry'"
        ))
        expected = {"mf_idx_dtype", "mf_idx_max", "mf_source_id", "mf_idx_ready"}
        assert expected <= {r[0] for r in rows}

    def test_cache_payloads_table_exists(self, mf):
        rows = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_csv_registry' "
            "AND table_name = 'memframe_cache_payloads'"
        ))
        assert {"data_id", "opidx", "format", "payload", "created_at"} <= {
            r[0] for r in rows
        }


class DeepOps:
    """Minimal @record_call user hitting the DataFrame-persist path."""

    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self, i):
        await asyncio.sleep(0)
        return {"is_error": False, "result": pd.DataFrame({"n": [i]}), "n": i}


class CanonicalOps:
    """Tracks whether positional and keyword call styles share one key."""

    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id
        self.calls = 0

    @record_call(deep_cache=True)
    async def do(self, a, b=2):
        self.calls += 1
        return {"is_error": False, "result": pd.DataFrame({"n": [a + b]})}


class TestCanonicalKey:
    def test_positional_and_keyword_calls_share_one_entry(self, mf, uploaded):
        ops = CanonicalOps(mf, uploaded._data_id)

        first = asyncio.run(ops.do(1, b=3))
        second = asyncio.run(ops.do(a=1, b=3))
        third = asyncio.run(ops.do(1))

        assert first["result"]["n"].tolist() == [4]
        assert second["result_metadata"]["from_cache"] is True
        assert third["result"]["n"].tolist() == [3]
        assert ops.calls == 2  # 1+3 and 1+2 are distinct logical calls


class MetadataOps:
    """Returns the operation-envelope keys a real orchestrator would."""

    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self, i):
        return {
            "is_error": False,
            "result": pd.DataFrame({"n": [i]}),
            "new_columns": ["n"],
            "new_column": "n",
            "mapped_table": "source_table",
            "mapped_columns": ["n"],
        }


class TestHitEnvelope:
    def test_hit_replays_operation_metadata(self, mf, uploaded):
        ops = MetadataOps(mf, uploaded._data_id)

        first = asyncio.run(ops.do(1))
        second = asyncio.run(ops.do(1))

        assert first["new_columns"] == ["n"]
        assert second["result_metadata"]["from_cache"] is True
        assert second["new_columns"] == ["n"]
        assert second["new_column"] == "n"
        assert second["mapped_table"] == "source_table"
        assert second["mapped_columns"] == ["n"]
        assert second["generated_cols"] == ["n"]


class TestSyncAsyncBridge:
    def test_window_sync_then_async_hits(self, mf, uploaded):
        from memframe.wrappers.analytix.window import WindowWrapper

        wrapper = WindowWrapper(uploaded)
        first = wrapper.rolling("x", window=2, func="mean")
        second = asyncio.run(wrapper.arolling("x", window=2, func="mean"))

        assert second["result_metadata"]["from_cache"] is True
        assert first["result"].equals(second["result"])
        assert second["new_columns"] == ["x_rolling_mean_w2"]

    def test_groupby_sync_then_async_hits(self, mf, uploaded):
        from memframe.wrappers.analytix.groupby_stats import GroupByStatsWrapper

        builder = GroupByStatsWrapper(uploaded).groupby("x")
        first = builder.sum("x")
        second = asyncio.run(builder.asum("x"))

        assert second["result_metadata"]["from_cache"] is True
        assert first["result"].equals(second["result"])
        assert second["new_columns"] == ["x_sum"]

    def test_groupby_cumulative_sync_then_async_hits(self, mf, uploaded):
        from memframe.wrappers.analytix.groupby_cumulative import (
            GroupByCumulativeWrapper,
        )

        builder = GroupByCumulativeWrapper(uploaded).groupby("x")
        first = builder.cumsum("x")
        second = asyncio.run(builder.acumsum("x"))

        assert second["result_metadata"]["from_cache"] is True
        assert first["result"].equals(second["result"])
        assert second["new_columns"] == ["cum_x_sum_by_x"]


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
            "CREATE TABLE memframe_cache.ov1 (x INTEGER); INSERT INTO memframe_cache.ov1 VALUES (1)"
        ))
        asyncio.run(mf._backend.execute(
            "CREATE TABLE memframe_cache.ov2 (x INTEGER); INSERT INTO memframe_cache.ov2 VALUES (2)"
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


class RefOps:
    """Annotated Out_2 ref: the source table already holds the columns."""

    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self, column):
        rows = await self._memframe._backend.fetch(
            f'SELECT "{column}" FROM memframe_upload."{self._data_id}"'
        )
        return {
            "is_error": False,
            "cache_kind": "ref",
            "ref_schema": "memframe_upload",
            "ref_table": self._data_id,
            "involved_cols": [column],
            "generated_cols": [],
            "result_cols": [column],
            "result": pd.DataFrame(rows, columns=[column]),
        }


class ActualOps:
    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self):
        return {
            "is_error": False,
            "cache_kind": "actual",
            "result": {"a": 1, "b": [1, 2], "when": pd.Timestamp("2024-01-01")},
        }


class ActualFrameOps:
    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self):
        df = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.Index(["a", "b"], name="label"))
        return {"is_error": False, "cache_kind": "actual", "result": df}


class MutatingActualOps:
    def __init__(self, mf, data_id):
        self._memframe = mf
        self._data_id = data_id

    @record_call(deep_cache=True)
    async def do(self, i):
        return {
            "is_error": False,
            "cache_kind": "actual",
            "mutates": True,
            "result": {"i": i},
        }


class TestAnnotatedKinds:
    def test_ref_hit_reads_source_columns(self, mf, uploaded):
        ops = RefOps(mf, uploaded._data_id)
        first = asyncio.run(ops.do("x"))
        second = asyncio.run(ops.do("x"))

        assert first["result"]["x"].tolist() == [1, 2, 3]
        assert second["result"]["x"].tolist() == [1, 2, 3]
        assert second["result_metadata"]["cache_kind"] == "ref"
        # The ref must not create a cache-schema copy.
        rows = asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_cache'"
        ))
        assert rows == []

    def test_actual_dict_round_trips(self, mf, uploaded):
        ops = ActualOps(mf, uploaded._data_id)
        first = asyncio.run(ops.do())
        second = asyncio.run(ops.do())

        assert first["result"]["a"] == 1
        assert second["result"]["a"] == 1
        assert second["result"]["b"] == [1, 2]
        assert second["result"]["when"] == pd.Timestamp("2024-01-01")
        assert second["result_metadata"]["cache_kind"] == "actual"
        fmt = asyncio.run(mf._backend.fetchval(
            "SELECT format FROM memframe_csv_registry.memframe_cache_payloads"
        ))
        assert fmt == "json"

    def test_actual_dataframe_preserves_index(self, mf, uploaded):
        ops = ActualFrameOps(mf, uploaded._data_id)
        first = asyncio.run(ops.do())
        second = asyncio.run(ops.do())

        assert second["result"].equals(first["result"])
        assert list(second["result"].index) == ["a", "b"]
        fmt = asyncio.run(mf._backend.fetchval(
            "SELECT format FROM memframe_csv_registry.memframe_cache_payloads"
        ))
        assert fmt == "parquet"

    def test_clear_cache_removes_actual_payloads(self, mf, uploaded):
        ops = ActualOps(mf, uploaded._data_id)
        asyncio.run(ops.do())
        assert asyncio.run(mf._backend.fetchval(
            "SELECT COUNT(*) FROM memframe_csv_registry.memframe_cache_payloads"
        )) == 1

        asyncio.run(mf._aclear_cache(uploaded._data_id))

        assert asyncio.run(mf._backend.fetchval(
            "SELECT COUNT(*) FROM memframe_csv_registry.memframe_cache_payloads"
        )) == 0
        assert asyncio.run(mf._backend.fetchval(
            "SELECT COUNT(*) FROM memframe_csv_registry.memframe_cache_registry"
        )) == 0

    def test_stats_scalar_is_actual_payload(self, mf, uploaded):
        from memframe.wrappers.analytix.stats import StatsWrapper

        stats = StatsWrapper(uploaded)
        first = asyncio.run(stats.amean("x"))
        second = asyncio.run(stats.amean("x"))

        assert first["result"] == 2.0
        assert second["result"] == 2.0
        assert second["result_metadata"]["cache_kind"] == "actual"
        fmt = asyncio.run(mf._backend.fetchval(
            "SELECT format FROM memframe_csv_registry.memframe_cache_payloads "
            "WHERE opidx = 1"
        ))
        assert fmt == "json"

    def test_mutation_epoch_invalidates_previous_entries(self, mf, uploaded):
        ops = MutatingActualOps(mf, uploaded._data_id)
        first = asyncio.run(ops.do(1))
        assert "result_metadata" not in first
        assert asyncio.run(ops.do(1))["result_metadata"]["from_cache"] is True

        # A different mutating call bumps the dataset epoch.
        asyncio.run(ops.do(2))
        third = asyncio.run(ops.do(1))
        assert "result_metadata" not in third
        assert third["result"]["i"] == 1


class TestInstanceStateKey:
    def test_groupby_group_cols_do_not_share_cache(self, mf):
        ctx = mf.upload_df(
            pd.DataFrame(
                {
                    "region": ["N", "N", "S", "S"],
                    "category": ["a", "b", "a", "b"],
                    "sales": [1, 2, 10, 20],
                }
            ),
            filename="groupby_cache_key",
        )

        by_region = asyncio.run(ctx.groupby("region").asum("sales"))
        by_category = asyncio.run(ctx.groupby("category").asum("sales"))

        assert by_region["result"]["region"].tolist() == ["N", "S"]
        assert by_region["result"]["sales_sum"].tolist() == [3.0, 30.0]
        assert by_category["result"]["category"].tolist() == ["a", "b"]
        assert by_category["result"]["sales_sum"].tolist() == [11.0, 22.0]
        assert not (by_category.get("result_metadata") or {}).get("from_cache")

        # The same builder must still replay.
        again = asyncio.run(ctx.groupby("region").asum("sales"))
        assert again["result_metadata"]["from_cache"] is True
