import asyncio

import pytest

from memframe.main import MemFrame
from memframe.exceptions import ConfigurationError, DataNotFound


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
    import pandas as pd
    ctx = mf.upload_df(pd.DataFrame({"a": [1, 2], "b": [3, 4]}), filename="ops_dataset")
    return ctx


class TestListTables:
    def test_empty_before_upload(self, mf):
        assert asyncio.run(mf.alist_tables()) == []

    def test_after_upload(self, mf, uploaded):
        tables = asyncio.run(mf.alist_tables())
        assert any(t["data_id"] == uploaded._data_id for t in tables)
        assert any(t["filename"] == "ops_dataset" for t in tables)

    def test_not_connected_raises(self):
        m = MemFrame()
        with pytest.raises(Exception):
            asyncio.run(m.alist_tables())


class TestActiveManagement:
    def test_set_and_get_active(self, mf, uploaded):
        result = asyncio.run(mf.aset_active(uploaded._data_id))
        from memframe.db_manager.context import ContextManager

        assert isinstance(result, ContextManager)
        assert result._data_id == uploaded._data_id
        assert asyncio.run(mf.aget_active_table()) == uploaded._data_id

    def test_set_active_unknown_data_id_raises(self, mf):
        with pytest.raises(DataNotFound):
            asyncio.run(mf.aset_active("zzzzzz"))


class TestDeleteTable:
    def test_delete_by_data_id(self, mf, uploaded):
        asyncio.run(mf.adelete_table(data_id=uploaded._data_id))
        assert asyncio.run(mf.alist_tables()) == []

    def test_delete_by_filename(self, mf, uploaded):
        asyncio.run(mf.adelete_table(filename="ops_dataset"))
        assert asyncio.run(mf.alist_tables()) == []

    def test_delete_missing_both_raises(self, mf):
        with pytest.raises(ConfigurationError):
            asyncio.run(mf.adelete_table())

    def test_delete_unknown_filename_raises(self, mf):
        with pytest.raises(DataNotFound):
            asyncio.run(mf.adelete_table(filename="nope"))


class TestOperationRecording:
    def test_record_and_list_operations(self, mf, uploaded):
        data_id = uploaded._data_id
        op = asyncio.run(mf._arecord_operation(data_id, "test_op", "tbl_x"))
        assert op == 1

        ops = asyncio.run(mf.alist_operations(data_id))
        assert len(ops) == 1
        assert ops[0]["operation_type"] == "test_op"
        assert ops[0]["table_name"] == "tbl_x"

    def test_record_method_call(self, mf, uploaded):
        data_id = uploaded._data_id
        op = asyncio.run(mf._arecord_method_call(
            data_id, "Class", "method", "[]", "{}",
            generated_table_name="tbl_y", is_deep_cache=True, schema="memframe_transient",
        ))
        assert op == 1
        ops = asyncio.run(mf.alist_operations(data_id))
        assert ops[0]["operation_type"] == "method_call"

    def test_retrieve_operation(self, mf, uploaded):
        data_id = uploaded._data_id
        asyncio.run(mf._arecord_operation(data_id, "test_op", "tbl_z"))
        assert asyncio.run(mf.aretrieve_operation(data_id, 1)) == "tbl_z"

    def test_retrieve_missing_operation_raises(self, mf, uploaded):
        with pytest.raises(DataNotFound):
            asyncio.run(mf.aretrieve_operation(uploaded._data_id, 999))

    def test_operation_index_monotonic(self, mf, uploaded):
        data_id = uploaded._data_id
        a = asyncio.run(mf._arecord_operation(data_id, "op", "t1"))
        b = asyncio.run(mf._arecord_operation(data_id, "op", "t2"))
        c = asyncio.run(mf._arecord_method_call(data_id, "C", "m", "()", "{}"))
        assert a == 1 and b == 2 and c == 3


class TestConcurrentRecording:
    """Finding 3: MAX(opidx)+1 read-then-insert must not hand out one opidx twice."""

    @staticmethod
    async def _remote_like_fetchval(backend, *args, **kwargs):
        """Wrap fetchval with a yield, like a real network round-trip.

        Local DuckDB calls never suspend, so coroutines can't interleave
        between the MAX read and the INSERT without this.
        """
        result = await type(backend).fetchval(backend, *args, **kwargs)
        await asyncio.sleep(0)
        return result

    @pytest.fixture
    def remote_like(self, mf, monkeypatch):
        monkeypatch.setattr(
            mf._backend, "fetchval",
            lambda *a, **k: self._remote_like_fetchval(mf._backend, *a, **k),
        )
        return mf

    def test_concurrent_method_calls_get_distinct_opidx(self, remote_like, uploaded):
        mf = remote_like
        data_id = uploaded._data_id

        async def _go():
            return await asyncio.gather(*[
                mf._arecord_method_call(data_id, "C", "m", f"[{i}]", "{}")
                for i in range(10)
            ])

        assert sorted(asyncio.run(_go())) == list(range(1, 11))
        rows = asyncio.run(mf.alist_operations(data_id))
        assert sorted(r["opidx"] for r in rows) == list(range(1, 11))

    def test_concurrent_decorated_calls_serialize(self, remote_like, uploaded):
        from memframe.cache import record_call

        mf = remote_like
        class FakeOps:
            def __init__(self, mf, data_id):
                self._memframe = mf
                self._data_id = data_id

            @record_call(deep_cache=False)
            async def do(self, i):
                await asyncio.sleep(0)  # force interleave
                return {"is_error": False, "n": i}

        ops = FakeOps(mf, uploaded._data_id)

        async def _go():
            return await asyncio.gather(*[ops.do(i) for i in range(10)])

        results = asyncio.run(_go())
        assert sorted(r["n"] for r in results) == list(range(10))
        rows = asyncio.run(mf.alist_operations(uploaded._data_id))
        assert sorted(r["opidx"] for r in rows) == list(range(1, 11))


class TestClearCache:
    def test_clear_cache_drops_tables_and_rows(self, mf, uploaded):
        data_id = uploaded._data_id
        asyncio.run(mf._arecord_operation(data_id, "op", "some_transient"))
        asyncio.run(mf._aclear_cache(data_id))
        assert asyncio.run(mf.alist_operations(data_id)) == []


class TestTransientDropQualification:
    """Finding 7: bare registry names must drop from the transient schema."""

    @staticmethod
    def _seed_transient(mf, data_id):
        be = mf._backend
        ph = mf._placeholder
        asyncio.run(be.execute(f'CREATE TABLE memframe_transient."{data_id}_1" (x INTEGER)'))
        asyncio.run(mf._arecord_method_call(
            data_id, "C", "m", "[]", "{}",
            generated_table_name=f"{data_id}_1", is_deep_cache=True, schema="memframe_transient",
        ))
        # Legacy row: NULL schema falls back to the transient schema.
        asyncio.run(be.execute(f'CREATE TABLE memframe_transient."{data_id}_2" (x INTEGER)'))
        asyncio.run(be.execute(
            f"INSERT INTO {be.transient_registry_table} "
            f"(data_id, opidx, operation_type, generated_table_name) "
            f"VALUES ({ph(1)}, {ph(2)}, {ph(3)}, {ph(4)})",
            data_id, 99, "method_call", f"{data_id}_2",
        ))
        # Shallow signature-only row: NULL name must not crash the drop loop.
        asyncio.run(mf._arecord_method_call(data_id, "C", "n", "[]", "{}"))

    def test_qualify_table_round_trips(self, mf):
        assert mf._backend.qualify_table("memframe_transient", "abc_1") == 'memframe_transient."abc_1"'

    def test_delete_drops_transient_tables(self, mf, uploaded):
        data_id = uploaded._data_id
        self._seed_transient(mf, data_id)
        asyncio.run(mf.adelete_table(data_id=data_id))
        assert not asyncio.run(mf._backend.table_exists(f'memframe_transient."{data_id}_1"'))
        assert not asyncio.run(mf._backend.table_exists(f'memframe_transient."{data_id}_2"'))
        assert asyncio.run(mf.alist_tables()) == []

    def test_clear_cache_drops_transient_tables(self, mf, uploaded):
        data_id = uploaded._data_id
        self._seed_transient(mf, data_id)
        asyncio.run(mf._aclear_cache(data_id))
        assert not asyncio.run(mf._backend.table_exists(f'memframe_transient."{data_id}_1"'))
        assert not asyncio.run(mf._backend.table_exists(f'memframe_transient."{data_id}_2"'))
        assert asyncio.run(mf.alist_operations(data_id)) == []
        # The upload table itself survives a cache clear.
        assert asyncio.run(mf._backend.table_exists(f'memframe_upload."{data_id}"'))


class TestRegisterTables:
    """SyncDB: register pre-existing, non-empty tables into csv_registry."""

    @staticmethod
    def _seed(mf):
        asyncio.run(mf._backend.execute(
            "CREATE TABLE main.real_users (id INTEGER, name TEXT); "
            "INSERT INTO main.real_users VALUES (1, 'a'), (2, 'b'), (3, 'c');"
        ))
        asyncio.run(mf._backend.execute(
            "CREATE TABLE main.empty_tbl (id INTEGER);"
        ))

    def test_registers_only_non_empty(self, mf):
        self._seed(mf)
        registered = asyncio.run(mf.aregister_tables())
        tables = {t["table_name"] for t in registered.get("main", [])}
        assert "real_users" in tables
        assert "empty_tbl" not in tables

    def test_registered_usable_via_active(self, mf):
        self._seed(mf)
        registered = asyncio.run(mf.aregister_tables())
        data_id = next(t["data_id"] for t in registered["main"] if t["table_name"] == "real_users")
        asyncio.run(mf.aset_active(data_id))
        ds = mf.memFrame()
        head = asyncio.run(ds.ahead())
        assert len(head) == 3

    def test_idempotent(self, mf):
        self._seed(mf)
        asyncio.run(mf.aregister_tables())
        again = asyncio.run(mf.aregister_tables())
        assert again == {}

    def test_delete_keeps_real_table(self, mf):
        self._seed(mf)
        registered = asyncio.run(mf.aregister_tables())
        data_id = next(t["data_id"] for t in registered["main"] if t["table_name"] == "real_users")
        asyncio.run(mf.adelete_table(data_id=data_id))
        # is_external guard: the user's real table must survive delete.
        remaining = asyncio.run(mf._backend.fetchval("SELECT COUNT(*) FROM main.real_users"))
        assert remaining == 3
        assert asyncio.run(mf.alist_tables()) == []
