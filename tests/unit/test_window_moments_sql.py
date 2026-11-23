import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.client import MemFrame
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
            "x": [1.0, 2.0, None, 4.0, 5.0, 6.0, 7.0, 8.0],
            "o": list(range(8)),
            "t": pd.to_datetime(
                [f"2024-01-{day:02d}" for day in range(1, 9)]
            ),
        }
    )


@pytest.mark.parametrize("func", ["skew", "kurt"])
@pytest.mark.parametrize("window", [3, 4, 5])
def test_rolling_moments_match_pandas(ctx, func, window):
    frame = _frame()
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"m_{func}_{window}")
    )
    response = wrapper.rolling(
        column="x", window=window, func=func, order_by="o", min_periods=1
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = getattr(
        frame["x"].rolling(window, min_periods=1), func
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("func", ["skew", "kurt"])
def test_expanding_moments_match_pandas(ctx, func):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"me_{func}"))
    response = wrapper.expanding(
        column="x", func=func, order_by="o", min_periods=1
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_expanding")][0]
    expected = getattr(
        frame["x"].expanding(min_periods=1), func
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("func", ["skew", "kurt"])
def test_time_window_moments_match_pandas(ctx, func):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"mt_{func}"))
    response = wrapper.rolling(
        column="x", window="3D", func=func, order_by="t", min_periods=1
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = getattr(
        frame.set_index("t", drop=False)["x"].rolling("3D", min_periods=1),
        func,
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


def test_moments_use_center_and_closed(ctx):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="mc"))
    response = wrapper.rolling(
        column="x", window=4, func="skew", order_by="o",
        min_periods=1, center=True, closed="both",
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = frame["x"].rolling(
        4, min_periods=1, center=True, closed="both"
    ).skew().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("func", ["skew", "kurt"])
def test_grouped_moments_match_pandas(ctx, func):
    frame = _frame().assign(g=["a"] * 4 + ["b"] * 4)
    context = ctx.upload_df(frame, filename=f"mg_{func}")
    wrapper = GroupByWindowStatsWrapper(context)

    for method, kwargs in (
        ("arolling", {"window": 4}),
        ("aexpanding", {}),
    ):
        response = asyncio.run(
            getattr(wrapper, method)(
                column="x", func=func, group_cols="g", order_by="o",
                min_periods=1, **kwargs,
            )
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "o"])
        column = [c for c in got.columns if func in c][0]

        ordered = frame.sort_values(["g", "o"], kind="mergesort")
        expected = pd.concat(
            [
                getattr(
                    (
                        grp["x"].rolling(4, min_periods=1)
                        if method == "arolling"
                        else grp["x"].expanding(min_periods=1)
                    ),
                    func,
                )()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        ).to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8, atol=1e-10,
        )


def test_moment_edge_cases(ctx):
    frame = pd.DataFrame(
        {"x": [5.0, 5.0, 5.0, 5.0, 1.0, 1.0, 1.0, 1.0], "o": list(range(8))}
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="medge"))

    response = wrapper.rolling(
        column="x", window=4, func="skew", order_by="o", min_periods=1
    )
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if "skew" in c][0]
    values = got[column].to_numpy(dtype=float)
    assert np.isnan(values[1])
    assert values[2] == 0.0  # three identical values
    assert values[3] == 0.0

    response = wrapper.rolling(
        column="x", window=4, func="kurt", order_by="o", min_periods=1
    )
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if "kurt" in c][0]
    values = got[column].to_numpy(dtype=float)
    assert np.isnan(values[2])
    assert values[3] == -3.0


def test_moments_rejected_for_categorical(ctx):
    frame = pd.DataFrame({"cat": ["a", "b", "c", "d"], "o": [0, 1, 2, 3]})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="mcat"))
    response = wrapper.rolling(
        column="cat", window=3, func="skew", order_by="o"
    )
    assert response["is_error"] is True
