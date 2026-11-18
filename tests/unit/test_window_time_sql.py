import asyncio
import datetime

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
            "x": [1.0, 2.0, None, 4.0, 5.0, 6.0, 7.0],
            "t": pd.to_datetime(
                [
                    "2024-01-01", "2024-01-01", "2024-01-03", "2024-01-04",
                    "2024-01-04", "2024-01-06", "2024-01-10",
                ]
            ),
        }
    )


@pytest.mark.parametrize(
    "window, pandas_window",
    [("3D", "3D"), (datetime.timedelta(days=2), "2D"), ("36h", "36h")],
)
@pytest.mark.parametrize(
    "func", ["sum", "mean", "min", "max", "count", "std", "var"]
)
def test_time_window_matches_pandas(ctx, window, pandas_window, func):
    frame = _frame()
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"t_{pandas_window}_{func}")
    )
    response = wrapper.rolling(
        column="x", window=window, func=func, order_by="t"
    )
    assert response["is_error"] is False, response.get("failed_funcs")

    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = getattr(
        frame.rolling(pandas_window, on="t")["x"], func
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-9,
    )


def test_time_window_min_periods(ctx):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="t_mp"))
    response = wrapper.rolling(
        column="x", window="3D", func="count", order_by="t", min_periods=2
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = (
        frame.rolling("3D", on="t", min_periods=2)["x"].count().to_numpy()
    )
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-9,
    )


def test_grouped_time_window_matches_pandas(ctx):
    frame = _frame().assign(g=["a"] * 4 + ["b"] * 3)
    context = ctx.upload_df(frame, filename="t_grouped")
    wrapper = GroupByWindowStatsWrapper(context)

    for func in ("sum", "mean"):
        response = asyncio.run(
            wrapper.arolling(
                column="x", window="3D", func=func, group_cols="g",
                order_by="t",
            )
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "t"])
        ordered = frame.sort_values(["g", "t"], kind="mergesort")

        expected = pd.concat(
            [
                getattr(grp.rolling("3D", on="t")["x"], func)()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        ).to_numpy()
        np.testing.assert_allclose(
            got[f"x_rolling_{func}_w259200s_by_g"].to_numpy(dtype=float),
            expected, equal_nan=True, rtol=1e-9,
        )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"order_by": None}, "order_by"),
        ({"order_by": ["t"]}, "single datetime order_by"),
        ({"order_by": "x"}, "datetime"),
        ({"func": "quantile"}, "time-based windows"),
        ({"win_type": "hann"}, "time-based windows"),
    ],
)
def test_time_window_validation(ctx, kwargs, message):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"t_bad_{message[:4]}"))
    call = {"column": "x", "window": "3D", "func": "mean", "order_by": "t"}
    call.update(kwargs)
    response = wrapper.rolling(**call)
    assert response["is_error"] is True
    assert message in str(response.get("failed_funcs") or response.get("error_message"))
