import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper


@pytest.fixture
def grouped_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe
    finally:
        asyncio.run(memframe.aclose())


def _upload(ctx, frame, name):
    return ctx.upload_df(frame, filename=name)


def _result(response):
    assert response["is_error"] is False, response.get("error_message")
    return response["result"]


def _pandas_expected(frame, func, window=None, min_periods=1, order_by="o"):
    if order_by:
        ordered = frame.sort_values(
            ["g", order_by], kind="mergesort"
        ).reset_index(drop=True)
    else:
        ordered = frame.reset_index(drop=True)
    grouped = ordered.groupby("g", sort=False)["x"]
    roll = (
        grouped.rolling(window, min_periods=min_periods)
        if window is not None
        else grouped.expanding(min_periods=min_periods)
    )
    values = getattr(roll, func)().reset_index(level=0, drop=True)
    return ordered, np.asarray(values)


@pytest.mark.parametrize(
    "func", ["sum", "mean", "min", "max", "count", "std", "var"]
)
@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_rolling_matches_pandas(grouped_context, func, order_by):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 6 + ["b"] * 5,
            "o": [1, 2, 2, 4, 5, 6, 1, 2, 3, 4, 5],
            "x": [10.0, 20.0, 30.0, None, 50.0, 60.0, 5.0, 15.0, 25.0, 35.0, 45.0],
        }
    )
    ctx = _upload(grouped_context, frame, f"groll_{func}_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(2, order_by=order_by, min_periods=1)
        .__getattribute__(func)("x")
    )
    result = _result(response)
    ordered, expected = _pandas_expected(frame, func, window=2, order_by=order_by)
    # Result is in _mf_idx order; align it to the expected frame order.
    result = result.reset_index(drop=True)
    tile = result.copy()
    tile["_ord"] = np.arange(len(tile))
    keys = ["g"] + ([order_by] if order_by else []) + ["_ord"]
    tile = tile.sort_values(keys, kind="mergesort")
    col = f"x_rolling_{func}_w2_by_g"
    np.testing.assert_allclose(
        tile[col].to_numpy(dtype=float),
        expected,
        equal_nan=True,
        rtol=1e-10,
        atol=1e-10,
    )


@pytest.mark.parametrize("func", ["sum", "mean", "min", "max", "count", "std", "var"])
@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_expanding_matches_pandas(grouped_context, func, order_by):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 5 + ["b"] * 4,
            "o": [1, 2, 3, 4, 5, 1, 2, 3, 4],
            "x": [10.0, 20.0, 30.0, 40.0, 50.0, 5.0, 15.0, None, 35.0],
        }
    )
    ctx = _upload(grouped_context, frame, f"gexp_{func}_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .expanding(order_by=order_by, min_periods=1)
        .__getattribute__(func)("x")
    )
    result = _result(response)
    ordered, expected = _pandas_expected(frame, func, min_periods=1, order_by=order_by)
    result = result.reset_index(drop=True)
    tile = result.copy()
    tile["_ord"] = np.arange(len(tile))
    keys = ["g"] + ([order_by] if order_by else []) + ["_ord"]
    tile = tile.sort_values(keys, kind="mergesort")
    col = f"x_expanding_{func}_by_g"
    np.testing.assert_allclose(
        tile[col].to_numpy(dtype=float),
        expected,
        equal_nan=True,
        rtol=1e-10,
        atol=1e-10,
    )


def test_grouped_rolling_first_last(grouped_context):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 4,
            "o": [1, 2, 3, 4],
            "x": [10.0, 20.0, 30.0, 40.0],
        }
    )
    ctx = _upload(grouped_context, frame, "gfirstlast")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(2, order_by="o", min_periods=1)
        .agg("x", ["first", "last"])
    )
    result = _result(response).sort_values("o")
    np.testing.assert_allclose(
        result["x_rolling_first_w2_by_g"], [10.0, 10.0, 20.0, 30.0]
    )
    np.testing.assert_allclose(
        result["x_rolling_last_w2_by_g"], [10.0, 20.0, 30.0, 40.0]
    )


def test_grouped_rolling_multi_column_groups(grouped_context):
    frame = pd.DataFrame(
        {
            "g1": ["a"] * 4 + ["b"] * 4,
            "g2": ["x", "x", "y", "y"] * 2,
            "o": [1, 2, 1, 2] * 2,
            "x": [10.0, 20.0, 30.0, 40.0, 5.0, 15.0, 25.0, 35.0],
        }
    )
    ctx = _upload(grouped_context, frame, "gmulti")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g1", "g2")
        .rolling(2, order_by="o", min_periods=1)
        .sum("x")
    )
    result = _result(response).sort_values(["g1", "g2", "o"])
    expected = (
        frame.sort_values(["g1", "g2", "o"], kind="mergesort")
        .groupby(["g1", "g2"], sort=False)["x"]
        .rolling(2, min_periods=1)
        .sum()
        .reset_index(level=[0, 1], drop=True)
    )
    np.testing.assert_allclose(
        result["x_rolling_sum_w2_by_g1_g2"], np.asarray(expected)
    )


def test_grouped_rolling_is_idempotent(grouped_context):
    frame = pd.DataFrame(
        {"g": ["a"] * 3, "o": [1, 2, 3], "x": [10.0, 20.0, 30.0]}
    )
    ctx = _upload(grouped_context, frame, "grepeat")
    wrapper = GroupByWindowStatsWrapper(ctx)
    first = wrapper.groupby("g").rolling(2, order_by="o", min_periods=1).mean("x")
    second = wrapper.groupby("g").rolling(2, order_by="o", min_periods=1).mean("x")
    assert first["is_error"] is False
    assert second["is_error"] is False
    assert list(ctx.head(n=10).columns).count("x_rolling_mean_w2_by_g") == 1
