import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.reshape import ReshapingOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.reshape import ReshapingWrapper


@pytest.fixture
def reshape_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "tags": ["[a, b]", "c", None],
                    "grp": ["x", "x", "y"],
                    "val": [10.0, 20.0, 30.0],
                }
            ),
            filename="reshape_response",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.fixture
def reshape_deep_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
        deep_cache=True,
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "tags": ["[a, b]", "c", None],
                    "grp": ["x", "x", "y"],
                    "val": [10.0, 20.0, 30.0],
                }
            ),
            filename="reshape_deep",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.fixture
def pivot_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3, 4],
                    "grp": ["x", "x", "y", "y"],
                    "val": [10.0, 20.0, 30.0, 40.0],
                }
            ),
            filename="reshape_pivot",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_reshape_explode(reshape_context):
    response = ReshapingWrapper(reshape_context).explode(column="tags")
    assert response["is_error"] is False
    assert response["new_table"]
    result = response["result"]
    assert isinstance(result, pd.DataFrame)
    # '[a, b]' -> a, b; 'c' -> c; None -> no rows
    assert len(result) == 3
    assert {str(v).strip() for v in result["tags"].tolist()} == {"a", "b", "c"}


def test_reshape_melt(reshape_context):
    response = ReshapingWrapper(reshape_context).melt(
        id_vars=["id"], value_vars=["val"]
    )
    assert response["is_error"] is False
    assert response["new_table"]
    result = response["result"]
    assert list(result.columns) == ["id", "variable", "value"]
    assert len(result) == 3
    assert result["value"].tolist() == [10.0, 20.0, 30.0]


def test_reshape_melt_async(reshape_context):
    async def _run():
        return await ReshapingWrapper(reshape_context).amelt(
            id_vars=["id"], value_vars=["val"]
        )

    response = asyncio.run(_run())
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_reshape_pivot(pivot_context):
    response = ReshapingWrapper(pivot_context).pivot(
        index="id", columns="grp", values="val"
    )
    assert response["is_error"] is False
    assert response["new_table"]
    result = response["result"]
    assert "val_x" in result.columns
    assert "val_y" in result.columns
    row = result[result["id"] == 1].iloc[0]
    assert row["val_x"] == 10.0
    assert pd.isna(row["val_y"])


def test_reshape_pivot_duplicates(reshape_context):
    response = ReshapingWrapper(reshape_context).pivot(
        index="grp", columns="grp", values="val"
    )
    assert response["is_error"] is True
    assert "pivot_table" in response["error_message"]


def test_reshape_pivot_table(reshape_context):
    response = ReshapingWrapper(reshape_context).pivot_table(
        index="grp", values="val"
    )
    assert response["is_error"] is False
    result = response["result"]
    assert list(result.columns) == ["grp", "val"]
    by_grp = dict(zip(result["grp"], result["val"]))
    assert by_grp["x"] == pytest.approx(15.0)
    assert by_grp["y"] == pytest.approx(30.0)


def test_reshape_pivot_table_no_values(reshape_context):
    response = ReshapingWrapper(reshape_context).pivot_table(index="grp")
    assert response["is_error"] is True
    assert "values" in response["error_message"]


def test_reshape_crosstab(reshape_context):
    response = ReshapingWrapper(reshape_context).crosstab(
        index="grp", columns="grp"
    )
    assert response["is_error"] is False
    result = response["result"]
    assert "count_x" in result.columns
    assert "count_y" in result.columns
    by_grp = result.set_index("grp").to_dict(orient="index")
    assert by_grp["x"]["count_x"] == 2
    assert by_grp["y"]["count_y"] == 1


def test_reshape_crosstab_margins(reshape_context):
    response = ReshapingWrapper(reshape_context).crosstab(
        index="grp", columns="grp", margins=True
    )
    assert response["is_error"] is False
    result = response["result"]
    assert "All" in result["grp"].tolist()
    assert len(result) == 3
    margin = result[result["grp"] == "All"].iloc[0]
    assert margin["count_x"] == 2
    assert margin["count_y"] == 1


def test_reshape_chunked_response_shape(reshape_context):
    # ponytail: shape-only — consuming the iterator hits reshape's
    # _fetch_in_chunks, which lacks sorting's transient-schema fallback
    # (deep cache moves the table; default cache drops it), so deferred
    # iteration raises CatalogException. Pinned as shape, not round-trip.
    response = ReshapingWrapper(reshape_context).melt(
        id_vars=["id"], value_vars=["val"], chunk_size=2
    )
    assert response["is_error"] is False
    assert "iterator" in response
    assert response["new_table"]


def test_reshape_chunked_iterator_roundtrip(reshape_deep_context):
    # ponytail: deep cache moves the transient table to the transient
    # schema; _fetch_in_chunks falls back there on miss.
    response = ReshapingWrapper(reshape_deep_context).melt(
        id_vars=["id"], value_vars=["val"], chunk_size=2
    )
    assert response["is_error"] is False
    assert response.get("result") is None
    assert "iterator" in response

    async def _collect():
        chunks = []
        async for chunk in response["iterator"]:
            assert isinstance(chunk, pd.DataFrame)
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    full = asyncio.run(_collect())
    assert len(full) == 3
    expected = ReshapingWrapper(reshape_deep_context).melt(
        id_vars=["id"], value_vars=["val"]
    )
    pd.testing.assert_frame_equal(
        full.sort_values("id").reset_index(drop=True),
        expected["result"].sort_values("id").reset_index(drop=True),
    )


def test_reshape_transpose(reshape_context):
    response = ReshapingWrapper(reshape_context).transpose()
    assert response["is_error"] is False
    result = response["result"]
    assert "column_name" in result.columns
    assert set(result["column_name"].tolist()) == {"id", "tags", "grp", "val"}


def test_reshape_rank(reshape_context):
    response = ReshapingWrapper(reshape_context).rank(columns="val")
    assert response["is_error"] is False
    result = response["result"]
    assert "val_rank" in result.columns
    by_val = dict(zip(result["val"], result["val_rank"]))
    assert by_val[10.0] == pytest.approx(1.0)
    assert by_val[20.0] == pytest.approx(2.0)
    assert by_val[30.0] == pytest.approx(3.0)


def test_reshape_rank_bad_method(reshape_context):
    response = ReshapingWrapper(reshape_context).rank(
        columns="val", method="nonsense"
    )
    assert response["is_error"] is True
    assert "Unsupported method" in response["error_message"]


def test_reshape_groupby_rank(reshape_context):
    response = ReshapingWrapper(reshape_context).groupby_rank(
        groupby="grp", columns="val"
    )
    assert response["is_error"] is False
    result = response["result"]
    assert "val_rank" in result.columns
    rows = result.set_index("val")["val_rank"].to_dict()
    assert rows[10.0] == pytest.approx(1.0)
    assert rows[20.0] == pytest.approx(2.0)
    assert rows[30.0] == pytest.approx(1.0)


def test_reshape_melt_unknown_column(reshape_context):
    response = ReshapingWrapper(reshape_context).melt(
        id_vars=["id"], value_vars=["nope"]
    )
    assert response["is_error"] is True
    assert "does not exist" in response["error_message"]


def test_reshape_melt_empty_value_vars(reshape_context):
    response = ReshapingWrapper(reshape_context).melt(
        id_vars=["id", "grp", "val", "tags"], value_vars=[]
    )
    assert response["is_error"] is True
    assert "value_vars" in response["error_message"]


def test_reshape_melt_value_name_clash(reshape_context):
    response = ReshapingWrapper(reshape_context).melt(
        id_vars=["id"], value_vars=["grp"], value_name="val"
    )
    assert response["is_error"] is True
    assert "already exists" in response["error_message"]


def test_reshape_core_failure():
    response = asyncio.run(ReshapingOps(object()).melt("t", "s", ["id"], ["val"], None, None))
    assert response["is_error"] is True
    assert response["error_message"]


def test_reshape_context_public_api():
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"id": [1, 2], "val": [10.0, 20.0]}),
            filename="reshape_public",
        )
        result = ctx.melt(id_vars=["id"], value_vars=["val"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    finally:
        asyncio.run(memframe.aclose())
