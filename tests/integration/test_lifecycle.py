"""Layer 4: full dataset lifecycle per backend.

Covers upload → list → set-active → operate → history → delete → cache-clear,
the end-to-end lifecycle a production user runs against each backend.
"""

import asyncio

import pandas as pd
import pytest

from memframe.exceptions import DataNotFound

pytestmark = pytest.mark.integration


class TestUploadLifecycle:
    def test_upload_then_list(self, connected_memframe, sample_df):
        ctx = connected_memframe.upload_df(sample_df, filename="lifecycle_dataset")
        tables = asyncio.run(connected_memframe.alist_tables())
        assert any(t["data_id"] == ctx._data_id for t in tables)
        assert any(t["filename"].startswith("lifecycle_dataset") for t in tables)

    def test_upload_context_carries_data_id(self, connected_memframe, sample_df):
        ctx = connected_memframe.upload_df(sample_df)
        assert isinstance(ctx._data_id, str)
        assert len(ctx._data_id) == 6


class TestActiveLifecycle:
    def test_set_active_then_get_active(self, connected_memframe, uploaded_ctx):
        data_id = uploaded_ctx._data_id
        assert asyncio.run(connected_memframe.aset_active(data_id)) == data_id
        assert asyncio.run(connected_memframe.aget_active_table()) == data_id

    def test_set_active_unknown_raises(self, connected_memframe):
        with pytest.raises(DataNotFound):
            asyncio.run(connected_memframe.aset_active("zzzzzz"))


class TestOperationLifecycle:
    def test_operation_then_history_then_retrieve(self, connected_memframe, uploaded_ctx):
        data_id = uploaded_ctx._data_id
        result = asyncio.run(uploaded_ctx.aadd("a", "b"))
        assert isinstance(result, pd.DataFrame)

        ops = asyncio.run(connected_memframe.alist_operations(data_id))
        method_calls = [o for o in ops if o["operation_type"] == "method_call"]
        assert method_calls, ops

        table = asyncio.run(connected_memframe.aretrieve_operation(data_id, method_calls[0]["opidx"]))
        assert table is not None and table != ""

    def test_deep_cache_hit_roundtrip(self, connected_memframe, uploaded_ctx):
        r1 = asyncio.run(uploaded_ctx.amul("a", "b"))
        r2 = asyncio.run(uploaded_ctx.amul("a", "b"))
        assert isinstance(r2, pd.DataFrame)
        assert r2.iloc[:, -1].tolist() == r1.iloc[:, -1].tolist()


class TestDeleteLifecycle:
    def test_delete_by_data_id(self, connected_memframe, uploaded_ctx):
        asyncio.run(connected_memframe.adelete_table(data_id=uploaded_ctx._data_id))
        tables = asyncio.run(connected_memframe.alist_tables())
        assert all(t["data_id"] != uploaded_ctx._data_id for t in tables)

    def test_delete_then_operation_list_empty(self, connected_memframe, uploaded_ctx):
        data_id = uploaded_ctx._data_id
        asyncio.run(uploaded_ctx.aadd("a", "b"))
        asyncio.run(connected_memframe.adelete_table(data_id=data_id))
        assert asyncio.run(connected_memframe.alist_operations(data_id)) == []


class TestCacheClearLifecycle:
    def test_clear_cache_keeps_upload_and_clears_ops(self, connected_memframe, uploaded_ctx):
        data_id = uploaded_ctx._data_id
        asyncio.run(uploaded_ctx.amul("a", "b"))
        assert asyncio.run(connected_memframe.alist_operations(data_id))

        asyncio.run(connected_memframe._aclear_cache(data_id))
        assert asyncio.run(connected_memframe.alist_operations(data_id)) == []

        tables = asyncio.run(connected_memframe.alist_tables())
        assert any(t["data_id"] == data_id for t in tables)
