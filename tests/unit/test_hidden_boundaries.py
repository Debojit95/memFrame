"""Public results must never expose the hidden _mf_idx column."""

import asyncio

import pandas as pd

from memframe.main import MemFrame


def _open(df, filename):
    mf = MemFrame(
        connection_type="local", connection_params={"db_path": ":memory:"}
    )
    asyncio.run(mf.aconnect())
    return mf, asyncio.run(mf.aupload_df(df, filename=filename))


def test_tail_returns_last_rows_in_original_order():
    mf, ctx = _open(pd.DataFrame({"id": [1, 2, 3, 4, 5, 6]}), "tail_order")
    try:
        result = ctx.tail(n=2)
        assert result["id"].tolist() == [5, 6]
        assert "_mf_idx" not in result.columns
    finally:
        asyncio.run(mf.aclose())


def test_itertuples_hides_mf_idx():
    mf, ctx = _open(
        pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}), "itertuples_hidden"
    )
    try:
        async def first_row():
            iterator = ctx.itertuples(index=False)
            async for row in iterator:
                return row

        row = asyncio.run(first_row())
        assert row._fields == ("id", "name")
        assert "_mf_idx" not in row._fields
        assert tuple(row) == (1, "a")
    finally:
        asyncio.run(mf.aclose())


def test_explode_result_hides_mf_idx():
    mf, ctx = _open(
        pd.DataFrame({"id": [1, 2], "tags": ["[a,b]", "[c]"]}), "explode_hidden"
    )
    try:
        response = ctx.explode("tags")
        result = response if isinstance(response, pd.DataFrame) else response["result"]
        assert "_mf_idx" not in result.columns
        assert set(result["tags"]) == {"a", "b", "c"}
    finally:
        asyncio.run(mf.aclose())
