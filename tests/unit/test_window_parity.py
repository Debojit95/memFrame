import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper
from memframe.wrappers.analytix.window import WindowWrapper


@pytest.fixture
def ctx():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe
    finally:
        asyncio.run(memframe.aclose())


def _frame():
    return pd.DataFrame(
        {
            "x": [10.0, None, 30.0, 40.0, None, 60.0],
            "o": [1, 2, 3, 4, 5, 6],
        }
    )


def _rolling(wrapper, column, window, func, **kwargs):
    response = wrapper.rolling(
        column=column, window=window, func=func, order_by="o", **kwargs
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    return response["result"]


def _pandas_rolling(frame, func, window, min_periods):
    roll = frame["x"].rolling(window, min_periods=min_periods)
    if func == "quantile":
        return roll.quantile(0.5).to_numpy()
    return getattr(roll, func)().to_numpy()


@pytest.mark.parametrize("func", ["sum", "mean", "min", "max", "count"])
@pytest.mark.parametrize("min_periods", [None, 1, 2, 0])
def test_rolling_min_periods_matches_pandas(ctx, func, min_periods):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"p_{func}_{min_periods}"))

    pandas_min = 3 if min_periods is None else min_periods
    expected = _pandas_rolling(frame, func, 3, pandas_min)

    result = _rolling(
        wrapper, "x", 3, func,
        **({} if min_periods is None else {"min_periods": min_periods}),
    )
    column = [c for c in result.columns if c.startswith("x_rolling")][0]
    np.testing.assert_allclose(
        result[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-12,
    )


def test_rolling_specials_min_periods_match_pandas(ctx):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="p_specials"))
    values = frame["x"]

    got = _rolling(wrapper, "x", 3, "quantile", min_periods=2)
    expected = values.rolling(3, min_periods=2).quantile(0.5).to_numpy()
    np.testing.assert_allclose(
        got["x_rolling_q0.5_w3"].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-12,
    )

    got = _rolling(wrapper, "x", 3, "nunique", min_periods=2)
    expected = values.rolling(3, min_periods=2).apply(
        lambda v: v.nunique(), raw=False
    ).to_numpy()
    np.testing.assert_allclose(
        got["x_rolling_nunique_w3"].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-12,
    )


def test_rolling_min_periods_above_window_is_rejected(ctx):
    wrapper = WindowWrapper(ctx.upload_df(_frame(), filename="p_err"))
    response = wrapper.rolling(
        column="x", window=2, func="mean", order_by="o", min_periods=3
    )
    assert response["is_error"] is True
    assert "min_periods" in response["failed_funcs"]["mean"]


def test_grouped_rolling_min_periods_matches_pandas(ctx):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 4 + ["b"] * 4,
            "x": [10.0, None, 30.0, 40.0, 5.0, 15.0, 25.0, 35.0],
            "o": [1, 2, 3, 4, 1, 2, 3, 4],
        }
    )
    context = ctx.upload_df(frame, filename="p_grouped")
    wrapper = GroupByWindowStatsWrapper(context)

    async def _run(min_periods):
        return await wrapper.arolling(
            column="x", window=3, func="mean", group_cols="g",
            order_by="o",
            **({} if min_periods is None else {"min_periods": min_periods}),
        )

    for min_periods in (None, 1, 2):
        response = asyncio.run(_run(min_periods))
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "o"]).reset_index(drop=True)

        pandas_min = 3 if min_periods is None else min_periods
        expected = (
            frame.sort_values("o", kind="mergesort")
            .groupby("g", sort=False)["x"]
            .rolling(3, min_periods=pandas_min)
            .mean()
            .reset_index(level=0, drop=True)
            .to_numpy()
        )
        np.testing.assert_allclose(
            got["x_rolling_mean_w3_by_g"].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-12,
        )
