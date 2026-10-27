import asyncio

import pandas as pd
import pytest

from memframe.db_manager.context import ContextManager
from memframe.exceptions import ConfigurationError
from memframe.main import MemFrame


@pytest.fixture
def mf():
    m = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(m.aconnect())
    try:
        yield m
    finally:
        asyncio.run(m.aclose())


def test_dataframe_from_dict(mf):
    ctx = mf.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    assert isinstance(ctx, ContextManager)
    assert ctx.head(n=10).shape == (2, 2)


def test_dataframe_from_lists_with_columns(mf):
    ctx = mf.DataFrame([[1, "x"], [2, "y"]], columns=["a", "b"])

    assert list(ctx.columns) == ["a", "b"]
    assert ctx.head(n=10)["a"].tolist() == [1, 2]


def test_dataframe_from_series(mf):
    ctx = mf.DataFrame(pd.Series([1, 2, 3], name="s"))

    assert list(ctx.columns) == ["s"]
    assert len(ctx.head(n=10)) == 3


def test_dataframe_from_dataframe_with_dtype(mf):
    ctx = mf.DataFrame(pd.DataFrame({"a": [1, 2]}), dtype="float64")

    assert ctx.head(n=10)["a"].tolist() == [1.0, 2.0]


def test_dataframe_result_has_full_api(mf):
    ctx = mf.DataFrame({"a": [3, 1, 2]})

    assert ctx.shape == (3, 1)
    assert set(ctx.dtypes) == {"a"}
    assert ctx.head(n=1)["a"].tolist() == [3]


def test_dataframe_none_raises(mf):
    with pytest.raises(ConfigurationError):
        mf.DataFrame(None)


def test_dataframe_empty_raises(mf):
    with pytest.raises(ConfigurationError):
        mf.DataFrame(pd.DataFrame())
    with pytest.raises(ConfigurationError):
        mf.DataFrame({})


def test_dataframe_columns_select_and_order(mf):
    ctx = mf.DataFrame(
        {"a": [1], "b": [2], "c": [3]}, columns=["c", "a"]
    )

    assert list(ctx.columns) == ["c", "a"]
    assert ctx.head(n=10).to_dict("list") == {"c": [3], "a": [1]}


def test_dataframe_index_alignment(mf):
    ctx = mf.DataFrame(
        {"a": pd.Series([2, 3], index=[2, 3])},
        index=[0, 1, 2, 3],
    )

    values = ctx.head(n=10)["a"].tolist()
    assert sum(pd.isna(value) for value in values) == 2
    assert sorted(value for value in values if not pd.isna(value)) == [2, 3]


def test_dataframe_copy_argument(mf):
    frame = pd.DataFrame({"a": [1, 2]})
    ctx = mf.DataFrame(frame, copy=True)

    assert ctx.head(n=10)["a"].tolist() == [1, 2]


def test_dataframe_filename_and_dtypes(mf):
    ctx = mf.DataFrame(
        {"id": [1, 2]},
        filename="constructed.csv",
        dtypes={"id": "BIGINT"},
    )

    assert any(
        row["filename"] == "constructed.csv" for row in mf.list_tables()
    )
    assert ctx.dtypes["id"] == "BIGINT"


def test_dataframe_from_context_copies(mf):
    source = mf.upload_df(pd.DataFrame({"a": [1, 2]}), filename="source")
    copied = mf.DataFrame(source)

    assert copied._data_id != source._data_id
    assert copied.head(n=10)["a"].tolist() == [1, 2]
    assert source.head(n=10)["a"].tolist() == [1, 2]


def test_adataframe():
    async def run():
        instance = MemFrame(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
        )
        await instance.aconnect()
        try:
            ctx = await instance.aDataFrame({"a": [1, 2]})
            assert ctx.head(n=10)["a"].tolist() == [1, 2]
        finally:
            await instance.aclose()

    asyncio.run(run())
