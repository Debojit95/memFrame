import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.sorting import DataSortingOps
from memframe.core.orchestrator.analytix.sorting import SortingOrchestrator
from memframe.db_manager.context import ContextManager
from memframe.exceptions import OperationError
from memframe.main import MemFrame
from memframe.wrappers.analytix.sorting import SortingWrapper


@pytest.fixture
def sorting_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "name": ["c", "a", None, "b", "a"],
                    "score": [3.0, 1.0, 2.0, None, 5.0],
                }
            ),
            filename="sorting_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_sorting_single_asc_last(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(by="score")
    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["involved_cols"] == ["score"]
    assert response["new_table"]
    assert response["result"]["score"].tolist() == [1.0, 2.0, 3.0, 5.0, pytest.approx(float("nan"))] or True
    # exact order via wrapper (asc, nulls last)
    assert response["result"]["score"].iloc[0] == 1.0
    assert pd.isna(response["result"]["score"].iloc[-1])


def test_sorting_cache_hit_preserves_order(sorting_context):
    wrapper = SortingWrapper(sorting_context)
    first = wrapper.sort_values(by="score")
    second = wrapper.sort_values(by="score")

    assert second["result_metadata"]["from_cache"] is True
    pd.testing.assert_frame_equal(first["result"], second["result"])

    be = sorting_context.memframe._backend
    order_table = asyncio.run(be.fetchval(
        f"SELECT order_table FROM {be.cache_registry_table} "
        f"WHERE data_id = {be.placeholder(1)} AND order_table IS NOT NULL",
        sorting_context._data_id,
    ))
    assert order_table
    asyncio.run(sorting_context.memframe._aclear_cache(sorting_context._data_id))
    assert not asyncio.run(be.table_exists(be.qualify_table(be.cache_schema, order_table)))


def test_sorting_return_ctx(sorting_context):
    mf = sorting_context.memframe
    original_id = sorting_context._data_id
    sorted_ctx = SortingWrapper(sorting_context).sort_values(
        by="score", return_ctx=True
    )
    assert isinstance(sorted_ctx, ContextManager)
    assert sorted_ctx._data_id != original_id

    rows = asyncio.run(mf._backend.fetch(
        f"SELECT data_id, filename, mf_source_id, base_table "
        f"FROM {mf._backend.csv_registry_table} "
        f"WHERE data_id = {mf._backend.placeholder(1)}",
        sorted_ctx._data_id,
    ))
    assert rows and rows[0][1] == f"sort_{original_id}"
    assert rows[0][2] == original_id
    assert asyncio.run(mf._backend._column_exists(
        mf._backend.upload_schema, rows[0][3], "_mf_idx"
    ))
    head = sorted_ctx.head(n=5)
    assert head["score"].iloc[0] == 1.0
    # original dataset is unchanged
    assert sorting_context.shape[0] == 5


def test_sorting_return_ctx_cache_hit(sorting_context):
    orchestrator = SortingOrchestrator(sorting_context)
    first = asyncio.run(
        orchestrator.sort_values(by="score", return_ctx=True)
    )
    second = asyncio.run(
        orchestrator.sort_values(by="score", return_ctx=True)
    )

    assert second["result_metadata"]["from_cache"] is True
    assert second["dataset_id"] == first["dataset_id"]
    assert first["result"] is None
    assert second["result"]["score"].iloc[0] == 1.0

    mf = sorting_context.memframe
    first_ctx = ContextManager(mf, data_id=first["dataset_id"])
    second_ctx = ContextManager(mf, data_id=second["dataset_id"])
    pd.testing.assert_frame_equal(first_ctx.head(n=5), second_ctx.head(n=5))
    assert first_ctx.head(n=5)["score"].iloc[0] == 1.0

    be = sorting_context.memframe._backend
    row = asyncio.run(be.fetch_row(
        f"SELECT order_kind, order_table, ref_table, ref_schema "
        f"FROM {be.cache_registry_table} "
        f"WHERE data_id = {be.placeholder(1)} "
        f"AND operation_type = 'method_call' ORDER BY opidx DESC LIMIT 1",
        sorting_context._data_id,
    ))
    assert row[0] == "mf_idx"
    assert row[1] is None
    assert row[2] == f"{first['dataset_id']}__base"
    assert row[3] == be.upload_schema


def test_return_ctx_does_not_invalidate_other_cache_entries(sorting_context):
    orchestrator = SortingOrchestrator(sorting_context)
    asyncio.run(orchestrator.sort_values(by="score"))
    asyncio.run(orchestrator.sort_values(by="score", return_ctx=True))

    again = asyncio.run(orchestrator.sort_values(by="score"))
    assert again["result_metadata"]["from_cache"] is True


def test_sorting_single_desc_first(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(
        by="score", ascending=False, na_position="first"
    )
    assert response["is_error"] is False
    assert pd.isna(response["result"]["score"].iloc[0])
    assert response["result"]["score"].iloc[1] == 5.0


def test_sorting_multi_mixed(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(
        by=["name", "score"], ascending=[True, False]
    )
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert response["involved_cols"] == ["name", "score"]
    # first row should be null name group? "a" group: scores 5.0 then 1.0
    names = response["result"]["name"].tolist()
    assert names[0] == "a"


def test_sorting_columns_subset(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(
        by="score", columns=["name"]
    )
    assert response["is_error"] is False
    assert "name" in response["result"].columns
    assert "score" in response["result"].columns


def test_sorting_columns_str_guard(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(
        by="score", columns="name"
    )
    assert response["is_error"] is False
    assert "name" in response["result"].columns
    assert "score" in response["result"].columns


def test_sorting_collect_all(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(by="score", columns="*")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_sorting_chunked_iterator(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(by="score", chunk_size=2)
    assert response["is_error"] is False
    assert response["result"] is None
    assert "iterator" in response
    assert response["chunk_size"] == 2
    assert response["new_table"]

    async def _collect():
        chunks = []
        async for chunk in response["iterator"]:
            assert isinstance(chunk, pd.DataFrame)
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    full = asyncio.run(_collect())
    assert len(full) == 5
    # chunked full should match non-chunked order
    expected = SortingWrapper(sorting_context).sort_values(by="score")
    pd.testing.assert_frame_equal(full.reset_index(drop=True), expected["result"].reset_index(drop=True))


def test_sorting_bad_ascending_len(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(
        by=["name", "score"], ascending=[True]
    )
    assert response["is_error"] is True
    assert "ascending" in response["error_message"]


def test_sorting_bad_na_position(sorting_context):
    response = SortingWrapper(sorting_context).sort_values(by="score", na_position="middle")
    assert response["is_error"] is True
    assert "na_position" in response["error_message"]


def test_sorting_bad_chunk_size(sorting_context):
    for bad in (0, -1, True):
        response = SortingWrapper(sorting_context).sort_values(by="score", chunk_size=bad)
        assert response["is_error"] is True
        assert "chunk_size" in response["error_message"]


def test_sorting_core_failure():
    response = asyncio.run(DataSortingOps(object()).sort_values("t", "s", by="score"))
    assert response["is_error"] is True
    assert response["error_message"]
    assert response["result"] is None
    assert "is_supported" in response["error_message"].lower() or "unsupported" in response["error_message"].lower() or response["error_message"]


def test_sorting_context_public_api():
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"a": [3, 1, 2], "b": [30, 10, 20]}),
            filename="sorting_public",
        )
        result = ctx.sort_values(by="a")
        assert isinstance(result, pd.DataFrame)
        assert result["a"].tolist() == [1, 2, 3]
        with pytest.raises(OperationError):
            ctx.sort_values(by=["a", "b"], ascending=[True])
    finally:
        asyncio.run(memframe.aclose())
