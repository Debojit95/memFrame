import asyncio

import pandas as pd
import pytest

from memframe.exceptions import OperationError
from memframe.main import MemFrame
from memframe.wrappers.analytix.index import IndexWrapper


@pytest.fixture
def ctx():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "month": [1, 4, 7, 10],
                    "sale": [55, 40, 84, 31],
                }
            ),
            filename="index_wrapper",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_ctx_set_index_dispatches_to_metadata_engine(ctx):
    # ponytail: IndexWrapper precedes TableOpsWrapper, so ctx.set_index (both
    # `keys` and legacy `columns=` spellings) hits the metadata path.
    assert ctx.set_index("month")["index_cols"] == ["month"]
    assert ctx.set_index(columns=["month"])["index_cols"] == ["month"]
    assert ctx.index == [1, 4, 7, 10]


def test_ctx_reindex_returns_dataframe(ctx):
    ctx.set_index("month")
    result = ctx.reindex([1, 2, 4], fill_value=0)
    assert isinstance(result, pd.DataFrame)
    assert result["sale"].tolist() == [55, 0, 40]


def test_ctx_reset_index_and_property(ctx):
    ctx.set_index("month")
    assert ctx.reset_index()["index_cols"] == []
    assert ctx.index == [0, 1, 2, 3]  # synthetic RangeIndex


def test_ctx_async_twins(ctx):
    async def _run():
        await ctx.aset_index("month")
        assert await ctx.aget_index() == {
            "index_cols": ["month"],
            "synthetic": False,
            "values": [1, 4, 7, 10],
        }
        result = await ctx.areindex([4, 7], method="ffill")
        assert result["sale"].tolist() == [40, 84]
        other = ctx.memframe.upload_df(
            pd.DataFrame({"month": [7], "sale": [700]}),
            filename="index_wrapper_other",
        )
        await IndexWrapper(other).aset_index("month")
        result = await ctx.areindex_like(other._data_id)
        assert result["sale"].tolist() == [84]

    asyncio.run(_run())


def test_ctx_index_errors_raise(ctx):
    with pytest.raises(OperationError, match="set_index"):
        ctx.reindex([1, 2])
    ctx.set_index("month")
    with pytest.raises(OperationError, match="monotonic"):
        ctx.reindex([4, 1, 7], method="ffill")


def test_ctx_dir_includes_index(ctx):
    assert "index" in ctx.__dir__()
    assert "reindex" in ctx.__dir__()
    assert "reset_index" in ctx.__dir__()
