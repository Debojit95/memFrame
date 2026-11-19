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


@pytest.mark.parametrize("func", ["quantile", "median", "sem", "rank", "nunique"])
def test_time_window_specials_match_pandas(ctx, func):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"ts_{func}"))
    response = wrapper.rolling(
        column="x", window="3D", func=func, order_by="t", min_periods=1
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]

    roll = frame.set_index("t", drop=False).rolling("3D", min_periods=1)["x"]
    if func == "quantile":
        expected = roll.quantile(0.5).to_numpy()
    elif func == "median":
        expected = roll.median().to_numpy()
    elif func == "sem":
        expected = roll.sem().to_numpy()
    elif func == "rank":
        expected = roll.rank().to_numpy()
    else:
        expected = roll.apply(
            lambda v: pd.Series(v).nunique(), raw=True
        ).to_numpy()

    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-9,
    )


def test_time_window_datetime_stats(ctx):
    stamps = pd.to_datetime(
        [
            "2024-01-01", "2024-01-01", "2024-01-03", "2024-01-04",
            "2024-01-04", "2024-01-06", "2024-01-10",
        ]
    )
    frame = pd.DataFrame({"d": stamps, "t": stamps})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="ts_dt"))

    ts = frame["t"].to_numpy()
    ns = frame["d"].to_numpy().astype("int64")
    window = np.timedelta64(3, "D")
    exp_mean, exp_median = [], []
    for i in range(len(ts)):
        mask = (ts > ts[i] - window) & (np.arange(len(ts)) <= i)
        arr = np.sort(ns[mask])
        exp_mean.append(arr.mean())
        exp_median.append(np.median(arr))

    for func, expected, atol in (
        ("mean", exp_mean, 1e9),
        ("median", exp_median, 1e6),
    ):
        response = wrapper.rolling(
            column="d", window="3D", func=func, order_by="t", min_periods=1
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("t")
        column = [c for c in got.columns if c.startswith("d_rolling")][0]
        got_ns = (
            pd.to_datetime(got[column]).astype("int64").to_numpy()
        )
        np.testing.assert_allclose(
            got_ns, np.asarray(expected), rtol=0, atol=atol,
        )

    response = wrapper.rolling(
        column="d", window="3D", func="mode", order_by="t", min_periods=1
    )
    assert response["is_error"] is False, response.get("failed_funcs")


@pytest.mark.parametrize("closed", ["right", "left", "both", "neither"])
def test_time_window_closed_matches_pandas(ctx, closed):
    frame = pd.DataFrame(
        {
            "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "t": pd.to_datetime(
                [
                    "2024-01-01", "2024-01-01", "2024-01-02",
                    "2024-01-03", "2024-01-04", "2024-01-05",
                ]
            ),
        }
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"ts_c_{closed}"))
    for func in ("sum", "mean"):
        response = wrapper.rolling(
            column="x", window="2D", func=func, order_by="t",
            closed=closed, min_periods=1,
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("t")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        roll = frame.set_index("t", drop=False).rolling(
            "2D", closed=closed, min_periods=1
        )["x"]
        expected = getattr(roll, func)().to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-9,
        )


def test_grouped_time_window_specials(ctx):
    frame = _frame().assign(g=["a"] * 3 + ["b"] * 4)
    context = ctx.upload_df(frame, filename="ts_grouped")
    wrapper = GroupByWindowStatsWrapper(context)
    ordered = frame.sort_values(["g", "t"], kind="mergesort")

    for func in ("quantile", "median"):
        response = asyncio.run(
            wrapper.arolling(
                column="x", window="3D", func=func, group_cols="g",
                order_by="t", min_periods=1,
            )
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "t"])

        expected = pd.concat(
            [
                (
                    grp.set_index("t", drop=False).rolling(
                        "3D", min_periods=1
                    )["x"].quantile(0.5)
                    if func == "quantile"
                    else getattr(
                        grp.set_index("t", drop=False).rolling(
                            "3D", min_periods=1
                        )["x"],
                        func,
                    )()
                )
                for _, grp in ordered.groupby("g", sort=False)
            ]
        ).to_numpy()
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-9,
        )


@pytest.mark.parametrize("window", ["2D", "36h"])
@pytest.mark.parametrize("closed", [None, "right", "left", "both", "neither"])
@pytest.mark.parametrize("func", ["sum", "mean", "count"])
def test_centered_time_window_matches_pandas(ctx, window, closed, func):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03",
         "2024-01-03", "2024-01-04"]
    )
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "t": ts})
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"tc_{window}_{closed}_{func}")
    )
    kwargs = {"center": True}
    if closed:
        kwargs["closed"] = closed
    response = wrapper.rolling(
        column="x", window=window, func=func, order_by="t", **kwargs
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = getattr(
        frame.rolling(window, on="t", **kwargs)["x"], func
    )().to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-9,
    )


@pytest.mark.parametrize("func", ["quantile", "skew"])
def test_centered_time_window_specials(ctx, func):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    )
    frame = pd.DataFrame({"x": [1.0, 2.0, 4.0, 8.0, 16.0], "t": ts})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"tcs_{func}"))
    response = wrapper.rolling(
        column="x", window="2D", func=func, order_by="t",
        center=True, min_periods=1,
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    roll = frame.set_index("t", drop=False).rolling(
        "2D", center=True, min_periods=1
    )["x"]
    expected = (
        roll.quantile(0.5) if func == "quantile" else roll.skew()
    ).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("center", [False, True])
@pytest.mark.parametrize("func", ["first", "last"])
def test_time_window_first_last(ctx, center, func):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03",
         "2024-01-03", "2024-01-04"]
    )
    values = [None, 2.0, 3.0, 4.0, 5.0, 6.0]
    frame = pd.DataFrame({"x": values, "t": ts})
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"tfl_{center}_{func}")
    )
    response = wrapper.rolling(
        column="x", window="2D", func=func, order_by="t",
        center=center, min_periods=1,
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]

    ts_v = ts.to_numpy()
    expected = []
    for i in range(len(ts_v)):
        if center:
            mask = (ts_v > ts_v[i] - np.timedelta64(1, "D")) & (
                ts_v <= ts_v[i] + np.timedelta64(1, "D")
            )
        else:
            mask = (
                (ts_v > ts_v[i] - np.timedelta64(2, "D"))
                & (np.arange(len(ts_v)) <= i)
            )
        picked = np.asarray(values, dtype=object)[np.flatnonzero(mask)]
        expected.append(
            picked[0] if func == "first" else picked[-1]
        )
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), np.asarray(expected, dtype=float),
        equal_nan=True,
    )


def test_time_window_first_last_preserve_types(ctx):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-03"]
    )
    frame = pd.DataFrame(
        {
            "d": ts,
            "s": ["a", "b", "c"],
            "x": [1.0, 2.0, 3.0],
            "t": ts,
        }
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="tfl_types"))
    for func in ("first", "last"):
        response = wrapper.rolling(
            column="d", window="2D", func=func, order_by="t", min_periods=1
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("t")
        column = [c for c in got.columns if c.startswith("d_rolling")][0]
        assert str(got[column].dtype).startswith("datetime64")
        assert got[column].tolist()[2] == (ts[1] if func == "first" else ts[2])

        response = wrapper.rolling(
            column="s", window="2D", func=func, order_by="t", min_periods=1
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("t")
        column = [c for c in got.columns if c.startswith("s_rolling")][0]
        assert got[column].tolist()[2] == ("b" if func == "first" else "c")


def test_grouped_centered_and_first_last(ctx):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-04",
         "2024-01-01", "2024-01-03", "2024-01-05"]
    )
    frame = pd.DataFrame(
        {
            "g": ["a"] * 3 + ["b"] * 3,
            "x": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
            "t": ts,
        }
    )
    context = ctx.upload_df(frame, filename="tcg")
    wrapper = GroupByWindowStatsWrapper(context)
    ordered = frame.sort_values(["g", "t"], kind="mergesort")

    response = asyncio.run(
        wrapper.arolling(
            column="x", window="2D", func="sum", group_cols="g",
            order_by="t", center=True, min_periods=1,
        )
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "t"])
    expected = pd.concat(
        [
            grp.rolling("2D", on="t", center=True, min_periods=1)["x"].sum()
            for _, grp in ordered.groupby("g", sort=False)
        ]
    ).to_numpy()
    np.testing.assert_allclose(
        got["x_rolling_sum_w172800s_by_g"].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-9,
    )

    response = asyncio.run(
        wrapper.arolling(
            column="x", window="2D", func="last", group_cols="g",
            order_by="t", min_periods=1,
        )
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "t"])
    expected_last = (
        ordered.groupby("g", sort=False)["x"].last().to_numpy()
    )
    assert got["x_rolling_last_w172800s_by_g"].to_numpy(dtype=float)[-1] == (
        expected_last[1]
    )


def test_centered_time_window_min_periods(ctx):
    ts = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    )
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0], "t": ts})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="tcmp"))
    response = wrapper.rolling(
        column="x", window="2D", func="sum", order_by="t",
        center=True, min_periods=3,
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    np.testing.assert_allclose(
        got["x_rolling_sum_w172800s"].to_numpy(dtype=float),
        frame.rolling("2D", on="t", center=True, min_periods=3)["x"].sum(),
        equal_nan=True, rtol=1e-9,
    )
