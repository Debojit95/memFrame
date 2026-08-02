"""Layer 4: operation indexing and cache behavior.

Sequential opidx monotonicity and the cold→warm cache transition. Concurrent
writers on the same data_id are a documented limitation (MAX(opidx)+1 can race);
those are not tested here.
"""

import asyncio

import pytest

pytestmark = pytest.mark.integration


class TestOperationIndexMonotonic:
    def test_sequential_opidx_monotonic(self, connected_memframe, uploaded_ctx):
        data_id = uploaded_ctx._data_id
        asyncio.run(uploaded_ctx.aadd("a", "b"))
        asyncio.run(uploaded_ctx.amul("a", "b"))
        asyncio.run(uploaded_ctx.aadd("a", "b"))

        ops = asyncio.run(connected_memframe.alist_operations(data_id))
        idxs = [o["opidx"] for o in ops]
        assert idxs == sorted(idxs)
        assert len(set(idxs)) == len(idxs)


class TestCacheTransition:
    def test_cold_then_warm(self, uploaded_ctx):
        r1 = asyncio.run(uploaded_ctx.amul("a", "b"))
        assert r1.get("result_metadata", {}).get("from_cache", False) is False

        r2 = asyncio.run(uploaded_ctx.amul("a", "b"))
        assert r2.get("result_metadata", {}).get("from_cache") is True

    def test_warm_result_equals_cold(self, uploaded_ctx, get_result_df):
        r1 = asyncio.run(uploaded_ctx.amul("a", "b"))
        r2 = asyncio.run(uploaded_ctx.amul("a", "b"))

        cold = get_result_df(r1)
        warm = get_result_df(r2)

        # The cold result carries only involved columns; a cache hit reloads the
        # whole saved table (all source columns + the generated one). The
        # generated column values must agree.
        cold_gen = cold.select_dtypes(include="number").iloc[:, -1]
        warm_gen = warm[cold_gen.name]
        assert warm_gen.tolist() == cold_gen.tolist()

    def test_different_args_miss(self, uploaded_ctx):
        r1 = asyncio.run(uploaded_ctx.amul("a", "b"))
        assert r1.get("result_metadata", {}).get("from_cache", False) is False

        r2 = asyncio.run(uploaded_ctx.aadd("a", "b"))
        assert r2.get("result_metadata", {}).get("from_cache", False) is False
