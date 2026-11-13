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


def test_preview_rows_limits_feature_results_and_cache():
    from memframe.wrappers.analytix.window import WindowWrapper

    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
        deep_cache=True,
        preview_rows=5,
    )
    asyncio.run(mf.aconnect())
    try:
        ctx = asyncio.run(mf.aupload_df(
            pd.DataFrame({
                "amount": list(range(20)),
                "created_at": pd.date_range("2024-01-01", periods=20, freq="D"),
            }),
            filename="preview_rows",
        ))
        wrapper = WindowWrapper(ctx)
        first = wrapper.rolling(
            column="amount", window=3, func="mean", order_by="created_at"
        )
        assert len(first["result"]) == 5
        assert "amount_rolling_mean_w3" in ctx.head(n=1).columns

        second = wrapper.rolling(
            column="amount", window=3, func="mean", order_by="created_at"
        )
        assert second["result_metadata"]["from_cache"] is True
        assert len(second["result"]) == 5
    finally:
        asyncio.run(mf.aclose())


def test_preview_rows_must_be_positive():
    from memframe.exceptions import ConfigurationError

    try:
        MemFrame(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
            preview_rows=0,
        )
    except ConfigurationError:
        return
    raise AssertionError("preview_rows=0 should raise ConfigurationError")


def test_preview_rows_limits_sort_and_filter():
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
        deep_cache=True,
        preview_rows=5,
    )
    asyncio.run(mf.aconnect())
    try:
        ctx = asyncio.run(mf.aupload_df(
            pd.DataFrame({"x": list(range(100))}), filename="preview_rows_ops"
        ))

        first = ctx.sort_values(by="x", ascending=False)
        assert len(first) == 5
        assert first["x"].iloc[0] == 99

        cached = ctx.sort_values(by="x", ascending=False)
        assert len(cached) == 5
        assert cached["x"].iloc[0] == 99

        filtered = ctx.filter("x >= 50")
        assert len(filtered) == 5

        sliced = ctx.iloc(row_indexer=slice(0, 50))
        assert len(sliced) == 5

        # Full data is still in DuckDB.
        assert ctx.shape[0] == 100
    finally:
        asyncio.run(mf.aclose())
