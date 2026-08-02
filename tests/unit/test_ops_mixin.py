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
        asyncio.run(m.close())


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
        assert asyncio.run(mf.aset_active(uploaded._data_id)) == uploaded._data_id
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
            generated_table_name="tbl_y", is_deep_cache=True, schema="transient",
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


class TestClearCache:
    def test_clear_cache_drops_tables_and_rows(self, mf, uploaded):
        data_id = uploaded._data_id
        asyncio.run(mf._arecord_operation(data_id, "op", "some_transient"))
        asyncio.run(mf._aclear_cache(data_id))
        assert asyncio.run(mf.alist_operations(data_id)) == []
