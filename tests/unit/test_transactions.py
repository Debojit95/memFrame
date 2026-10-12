"""Finding 5: multi-step writes must be atomic — failure mid-sequence leaves no half-state."""

import asyncio

import pandas as pd
import pytest

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
    ctx = mf.upload_df(pd.DataFrame({"a": [1, 2], "b": [3, 4]}), filename="txn_dataset")
    return ctx


def _qualified_upload(data_id: str) -> str:
    return f'memframe_upload."{data_id}"'


async def _upload_tables(mf):
    rows = await mf._backend.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'memframe_upload'"
    )
    return {r[0] for r in rows}


async def _registry_rows(mf, data_id: str):
    return await mf._backend.fetch(
        f"SELECT opidx FROM {mf._backend.transient_registry_table} "
        f"WHERE data_id = {mf._placeholder(1)}",
        data_id,
    )


class TestCastAtomicity:
    def test_cast_failure_rolls_back(self, mf, uploaded, monkeypatch):
        data_id = uploaded._data_id
        final_table = _qualified_upload(data_id)
        schema = {
            "a": {"postgres_type": "INTEGER", "clickhouse_type": "Int32", "is_nullable": True},
            "b": {"postgres_type": "INTEGER", "clickhouse_type": "Int32", "is_nullable": True},
        }

        calls = 0
        orig = mf._uploader.execute

        async def failing(sql, *args):
            nonlocal calls
            calls += 1
            if calls == 2:  # CREATE tmp ok, INSERT blows up
                raise RuntimeError("boom mid-cast")
            await orig(sql, *args)

        monkeypatch.setattr(mf._uploader, "execute", failing)
        with pytest.raises(RuntimeError, match="boom mid-cast"):
            asyncio.run(mf._uploader._cast_table_in_place(final_table, ["a", "b"], schema))

        # Original table intact, no tmp leftover.
        assert asyncio.run(mf._backend.table_exists(final_table))
        assert asyncio.run(mf._backend.fetchval(f"SELECT COUNT(*) FROM {final_table}")) == 2
        assert not asyncio.run(
            mf._backend.table_exists(f'memframe_upload."{data_id}_tmp"')
        )

    def test_nested_transaction_joins_outer(self, mf):
        async def _go():
            async with mf._backend.transaction():
                await mf._backend.execute("CREATE TABLE main.nest_t (x INTEGER)")
                async with mf._backend.transaction():
                    await mf._backend.execute("INSERT INTO main.nest_t VALUES (1)")

        asyncio.run(_go())
        assert asyncio.run(mf._backend.fetchval("SELECT COUNT(*) FROM main.nest_t")) == 1

    def test_transaction_rolls_back_on_error(self, mf):
        async def _go():
            async with mf._backend.transaction():
                await mf._backend.execute("CREATE TABLE main.rb_t (x INTEGER)")
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(_go())
        assert not asyncio.run(mf._backend.table_exists("main.rb_t"))


class TestUploadAtomicity:
    def test_upload_failure_leaves_no_orphan(self, mf, monkeypatch):
        before_tables = asyncio.run(_upload_tables(mf))
        before_registry = asyncio.run(mf.alist_tables())

        async def failing(*args, **kwargs):
            raise RuntimeError("boom mid-upload")

        monkeypatch.setattr(mf._uploader, "_insert_arrow_table", failing)
        with pytest.raises(RuntimeError, match="boom mid-upload"):
            asyncio.run(mf.aupload_df(pd.DataFrame({"x": [1]}), filename="orphan"))

        assert asyncio.run(_upload_tables(mf)) == before_tables
        assert asyncio.run(mf.alist_tables()) == before_registry


class TestDeleteAtomicity:
    def test_delete_failure_rolls_back(self, mf, uploaded, monkeypatch):
        data_id = uploaded._data_id
        asyncio.run(mf._arecord_operation(data_id, "op", "t1"))
        asyncio.run(mf._arecord_operation(data_id, "op", "t2"))

        calls = 0
        orig_drop = mf._backend.drop_table

        async def failing_drop(name):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("boom mid-delete")
            await orig_drop(name)

        monkeypatch.setattr(mf._backend, "drop_table", failing_drop)
        with pytest.raises(RuntimeError, match="boom mid-delete"):
            asyncio.run(mf.adelete_table(data_id=data_id))

        # Everything still there: table, csv row, both transient rows.
        assert asyncio.run(mf._backend.table_exists(_qualified_upload(data_id)))
        assert any(t["data_id"] == data_id for t in asyncio.run(mf.alist_tables()))
        assert len(asyncio.run(_registry_rows(mf, data_id))) == 2


class TestDeepCacheAtomicity:
    def test_store_failure_leaves_no_orphan(self, mf, uploaded, monkeypatch):
        from memframe.cache import record_call

        data_id = uploaded._data_id

        async def failing(*args, **kwargs):
            raise RuntimeError("boom mid-store")

        monkeypatch.setattr(mf._uploader, "_insert_arrow_table", failing)
        with pytest.raises(RuntimeError, match="boom mid-store"):
            asyncio.run(record_call._create_deep_cache_table(
                mf, mf._backend, data_id, pd.DataFrame({"x": [1]})
            ))

        rows = asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_transient'"
        ))
        assert rows == []
        assert asyncio.run(_registry_rows(mf, data_id)) == []


class TestClickHouseCompensationHelper:
    def test_noop_on_duckdb(self, mf, uploaded):
        # Must not drop anything when transactions handle the rollback.
        asyncio.run(mf._uploader._drop_upload_on_failure("whatever"))
        assert asyncio.run(mf._backend.table_exists(_qualified_upload(uploaded._data_id)))

    def test_drops_on_clickhouse(self):
        from memframe.core.ingestion.upload.base import Uploader
        from memframe.db_manager.setup.clickhouse import ClickHouseBackend

        uploader = Uploader()
        ch = ClickHouseBackend({"database": "x"})
        dropped = []

        async def fake_drop(name):
            dropped.append(name)

        ch.drop_table = fake_drop
        uploader._backend = ch
        asyncio.run(uploader._drop_upload_on_failure("mytable"))
        assert dropped == ["mytable"]
