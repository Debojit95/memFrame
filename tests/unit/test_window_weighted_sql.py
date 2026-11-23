import asyncio

import numpy as np
import pandas as pd
import pytest

# scipy is a test-only oracle: pandas' win_type needs it as well, so it is
# listed in the tox/CI dependencies (not a runtime dependency).
from scipy.signal import get_window

from memframe.core.analytix.window.base import WindowOps
from memframe.client import MemFrame
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper
from memframe.wrappers.analytix.window import WindowWrapper

WIN_TYPES = [
    "boxcar", "triang", "bartlett", "parzen", "bohman", "blackman",
    "blackmanharris", "barthann", "nuttall", "hann", "hamming", "cosine",
]


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


@pytest.mark.parametrize("win_type", WIN_TYPES)
@pytest.mark.parametrize("window", [1, 2, 3, 5])
def test_window_weights_match_scipy(win_type, window):
    got = np.asarray(WindowOps._window_weights(win_type, window))
    expected = get_window(win_type, window, fftbins=False)
    np.testing.assert_allclose(got, expected, atol=1e-10, rtol=0)


@pytest.mark.parametrize("win_type", WIN_TYPES)
@pytest.mark.parametrize("func", ["sum", "mean"])
def test_weighted_rolling_matches_pandas(ctx, win_type, func):
    frame = pd.DataFrame(
        {
            "x": [1.0, None, 3.0, 4.0, 5.0, 6.0, 7.0],
            "o": list(range(7)),
        }
    )
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"w_{win_type}_{func}")
    )
    response = wrapper.rolling(
        column="x", window=4, func=func, order_by="o", min_periods=1,
        win_type=win_type,
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]

    expected = getattr(
        frame["x"].rolling(4, min_periods=1, win_type=win_type), func
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-8,
    )


def test_grouped_weighted_rolling_matches_pandas(ctx):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 4 + ["b"] * 4,
            "x": [1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0],
            "o": [1, 2, 3, 4] * 2,
        }
    )
    context = ctx.upload_df(frame, filename="wg")
    wrapper = GroupByWindowStatsWrapper(context)

    async def _run(func):
        return await wrapper.arolling(
            column="x", window=3, func=func, group_cols="g", order_by="o",
            min_periods=1, win_type="triang",
        )

    ordered = frame.sort_values(["g", "o"], kind="mergesort")

    for func in ("sum", "mean"):
        response = asyncio.run(_run(func))
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "o"])
        # GroupBy.rolling drops win_type in pandas, so weight each group's
        # Series directly.
        expected = pd.concat(
            [
                getattr(
                    grp["x"].rolling(3, min_periods=1, win_type="triang"),
                    func,
                )()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        ).to_numpy()
        np.testing.assert_allclose(
            got[f"x_rolling_{func}_w3_by_g"].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8, atol=1e-8,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"win_type": "gaussian"},
        {"win_type": "hann", "center": True},
        {"win_type": "hann", "closed": "left"},
    ],
)
def test_weighted_rolling_rejects_unsupported_options(ctx, kwargs):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0], "o": [0, 1, 2]})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"werr_{len(kwargs)}"))
    response = wrapper.rolling(
        column="x", window=2, func="mean", order_by="o", **kwargs
    )
    assert response["is_error"] is True


def test_weighted_std_is_not_implemented(ctx):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0], "o": [0, 1, 2]})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="wstd"))
    response = wrapper.rolling(
        column="x", window=2, func="std", order_by="o", win_type="hann"
    )
    assert response["is_error"] is True
    assert "win_type" in response["failed_funcs"]["std"]
