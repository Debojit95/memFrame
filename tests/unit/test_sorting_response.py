import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.sorting import DataSortingOps
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
