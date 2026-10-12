"""Phase-2 concurrency/lifecycle regression tests."""

import asyncio

import pandas as pd
import pytest

from memframe.db_manager.connection.pool import DuckDBPool
from memframe.exceptions import ConnectionNotReady
from memframe.main import MemFrame
from memframe.utils import async_sync
from memframe.utils.async_sync import async_to_sync


# ── shared background loop ───────────────────────────────────────────────


@async_to_sync
async def _current_loop_id():
    return id(asyncio.get_running_loop())


def test_sync_calls_run_on_shared_background_loop():
    first = _current_loop_id()
    second = _current_loop_id()
    assert first == id(async_sync._loop)
    assert second == first  # stable across calls, not a fresh loop each time


def test_sync_result_and_exception_propagate():
    @async_to_sync
    async def boom():
        raise ValueError("kaboom")

    assert _current_loop_id() == id(async_sync._loop)
    with pytest.raises(ValueError, match="kaboom"):
        boom()


def test_sync_from_shared_loop_raises_with_guidance():
    @async_to_sync
    async def pretend_db_op():
        return 1

    async def offender():
        pretend_db_op()  # sync call from a coroutine on the shared loop

    fut = asyncio.run_coroutine_threadsafe(offender(), async_sync._loop)
    with pytest.raises(RuntimeError, match="async form"):
        fut.result(timeout=10)


def test_duckdb_connection_reused_for_sync_calls_from_running_loop():
    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(mf.aconnect())
    try:
        conn_before = mf._pool.conn

        async def drive():
            ctx = mf.upload_df(pd.DataFrame({"a": [1, 2, 3]}))
            await asyncio.sleep(0)  # let the user loop yield mid-flight
            res = ctx.head(n=2)
            assert len(res) == 2  # public wrapper returns the unwrapped DataFrame
            return mf._pool.conn

        conn_after = asyncio.run(drive())
        assert conn_after is conn_before
    finally:
        asyncio.run(mf.aclose())


# ── pool lifecycle ───────────────────────────────────────────────────────


def test_duckdb_connect_is_idempotent():
    pool = DuckDBPool(":memory:")
    asyncio.run(pool.connect())
    conn1 = pool.conn
    asyncio.run(pool.connect())  # must close the old handle, not leak it
    assert pool.conn is not conn1
    asyncio.run(pool.close())
    assert pool.conn is None


def test_duckdb_pool_guard_before_connect():
    pool = DuckDBPool(":memory:")
    with pytest.raises(ConnectionNotReady):
        asyncio.run(pool.execute("SELECT 1"))
    with pytest.raises(ConnectionNotReady):
        asyncio.run(pool.fetch("SELECT 1"))


def test_aconnect_failure_cleans_up_and_allows_retry(monkeypatch):
    import memframe.db_manager.connection.connector as connector_mod

    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})

    def broken_backend(*args, **kwargs):
        raise RuntimeError("backend init exploded")

    monkeypatch.setattr(connector_mod, "create_backend", broken_backend)
    with pytest.raises(RuntimeError, match="backend init exploded"):
        asyncio.run(mf.aconnect())
    assert mf._connector._pool is None
    assert mf._connector._backend is None

    # retry without the fault must succeed
    monkeypatch.undo()
    asyncio.run(mf.aconnect())
    try:
        assert mf._connector._backend is not None
    finally:
        asyncio.run(mf.aclose())


# ── context binding ──────────────────────────────────────────────────────


def test_context_snapshots_active_dataset_at_creation():
    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(mf.aconnect())
    try:
        loose = mf.memFrame()  # nothing active yet -> stays None, follows later
        assert loose._data_id is None

        ctx_a = asyncio.run(mf.aupload_df(pd.DataFrame({"a": [1]}), filename="ctx_a"))
        asyncio.run(mf.aset_active(ctx_a._data_id))

        pinned = mf.memFrame()
        assert pinned._data_id == ctx_a._data_id

        ctx_b = asyncio.run(mf.aupload_df(pd.DataFrame({"b": [1]}), filename="ctx_b"))
        asyncio.run(mf.aset_active(ctx_b._data_id))

        assert pinned._data_id == ctx_a._data_id  # not retargeted mid-flight
        assert loose._data_id is None  # created empty -> still follows active

        table, _ = asyncio.run(loose._get_active_context())
        assert table == ctx_b._data_id  # MemFrame-level "active" semantics intact
    finally:
        asyncio.run(mf.aclose())


# ── concurrent DuckDB use (finding 4) ──────────────────────────────────


def _check_uploads_isolated(mf, ctx_a, ctx_b, res_a, res_b):
    assert sorted(res_a["x"].tolist()) == [1, 2, 3]
    assert sorted(res_b["x"].tolist()) == [10, 20]
    views = asyncio.run(mf._backend.fetch(
        "SELECT view_name FROM duckdb_views() WHERE view_name LIKE 'arrow_temp%'"
    ))
    assert views == []  # every temp view unregistered in finally


def test_concurrent_duckdb_uploads_stay_isolated():
    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(mf.aconnect())
    try:
        async def _go():
            ctx_a, ctx_b = await asyncio.gather(
                mf.aupload_df(pd.DataFrame({"x": [1, 2, 3]}), filename="conc_a"),
                mf.aupload_df(pd.DataFrame({"x": [10, 20]}), filename="conc_b"),
            )
            res_a, res_b = await asyncio.gather(
                ctx_a.ahead(n=10),
                ctx_b.ahead(n=10),
            )
            return ctx_a, ctx_b, res_a, res_b

        _check_uploads_isolated(mf, *asyncio.run(_go()))
    finally:
        asyncio.run(mf.aclose())


def test_concurrent_duckdb_uploads_with_slow_pool():
    """Same as above but every pool call yields, maximizing the interleave window."""
    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(mf.aconnect())
    pool = mf._backend.pool
    orig_execute = pool.execute
    orig_fetch = pool.fetch

    async def slow_execute(sql, *args):
        await asyncio.sleep(0)
        await orig_execute(sql, *args)

    async def slow_fetch(sql, *args):
        await asyncio.sleep(0)
        return await orig_fetch(sql, *args)

    pool.execute = slow_execute
    pool.fetch = slow_fetch
    try:
        async def _go():
            ctx_a, ctx_b = await asyncio.gather(
                mf.aupload_df(pd.DataFrame({"x": [1, 2, 3]}), filename="conc_slow_a"),
                mf.aupload_df(pd.DataFrame({"x": [10, 20]}), filename="conc_slow_b"),
            )
            res_a, res_b = await asyncio.gather(
                ctx_a.ahead(n=10),
                ctx_b.ahead(n=10),
            )
            return ctx_a, ctx_b, res_a, res_b

        _check_uploads_isolated(mf, *asyncio.run(_go()))
    finally:
        pool.execute = orig_execute
        pool.fetch = orig_fetch
        asyncio.run(mf.aclose())
