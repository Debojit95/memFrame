import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.client import MemFrame
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


def _frame():
    return pd.DataFrame(
        {
            "g": ["a"] * 5 + ["b"] * 5,
            "o": [1, 2, 3, 4, 5] * 2,
            "x": [10.0, 20.0, 30.0, 40.0, 50.0, 5.0, 15.0, 25.0, 35.0, 45.0],
        }
    )


def _upload(ctx, frame, name):
    return ctx.upload_df(frame, filename=name)


def _aligned(response):
    assert response["is_error"] is False, response.get("error_message")
    return response["result"]


def _sorted_tile(df, order_by="o"):
    tile = df.copy()
    tile["_ord"] = np.arange(len(tile))
    keys = ["g"] + ([order_by] if order_by else []) + ["_ord"]
    return tile.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _pandas_frame(frame, order_by="o"):
    if order_by:
        return frame.sort_values(["g", order_by], kind="mergesort").reset_index(drop=True)
    return frame.reset_index(drop=True)


def _sem_expected(frame, window=None):
    out = []
    for _, group in frame.groupby("g", sort=False):
        values = group["x"].to_numpy(dtype=float)
        for i in range(len(values)):
            lo = 0 if window is None else max(0, i - window + 1)
            vals = values[lo:i + 1]
            if len(vals) < 2:
                out.append(np.nan)
            else:
                out.append(np.std(vals, ddof=1) / np.sqrt(len(vals) - 1))
    return np.asarray(out)


@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_rolling_quantile_matches_pandas(grouped_context, order_by):
    frame = _frame()
    ctx = _upload(grouped_context, frame, f"gsq_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(3, order_by=order_by, min_periods=1)
        .quantile("x", q=0.5)
    )
    got = _sorted_tile(_aligned(response), order_by)
    ordered = _pandas_frame(frame, order_by)
    expected = (
        ordered.groupby("g", sort=False)["x"]
        .rolling(3, min_periods=1)
        .quantile(0.5)
        .reset_index(level=0, drop=True)
    )
    np.testing.assert_allclose(
        got["x_rolling_q0.5_w3_by_g"], np.asarray(expected), rtol=1e-10
    )


@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_rolling_sem_matches_pandas(grouped_context, order_by):
    frame = _frame()
    ctx = _upload(grouped_context, frame, f"gss_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(3, order_by=order_by, min_periods=1)
        .sem("x")
    )
    got = _sorted_tile(_aligned(response), order_by)
    ordered = _pandas_frame(frame, order_by)
    expected = _sem_expected(ordered, window=3)
    np.testing.assert_allclose(
        got["x_rolling_sem_w3_by_g"], expected,
        equal_nan=True, rtol=1e-10,
    )


@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_rolling_nunique_matches_pandas(grouped_context, order_by):
    frame = _frame()
    ctx = _upload(grouped_context, frame, f"gsn_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(3, order_by=order_by, min_periods=1)
        .nunique("x")
    )
    got = _sorted_tile(_aligned(response), order_by)
    ordered = _pandas_frame(frame, order_by)
    expected = (
        ordered.groupby("g", sort=False)["x"]
        .rolling(3, min_periods=1)
        .apply(lambda values: values.nunique(), raw=False)
        .reset_index(level=0, drop=True)
    )
    np.testing.assert_allclose(
        got["x_rolling_nunique_w3_by_g"], np.asarray(expected), rtol=1e-10
    )


@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_rolling_rank_matches_manual(grouped_context, order_by):
    frame = _frame()
    ctx = _upload(grouped_context, frame, f"gsr_{order_by}")
    response = (
        GroupByWindowStatsWrapper(ctx)
        .groupby("g")
        .rolling(3, order_by=order_by, min_periods=1)
        .rank("x", method="max")
    )
    got = _sorted_tile(_aligned(response), order_by)
    ordered = _pandas_frame(frame, order_by)
    expected = []
    for _, group in ordered.groupby("g", sort=False):
        values = group["x"].to_numpy()
        for i in range(len(values)):
            lo = max(0, i - 2)
            expected.append(int((values[lo:i + 1] <= values[i]).sum()))
    np.testing.assert_allclose(
        got["x_rolling_rank_w3_by_g"], expected, rtol=1e-12
    )


@pytest.mark.parametrize("order_by", ["o", None])
def test_grouped_expanding_specials_match_pandas(grouped_context, order_by):
    frame = _frame()
    ctx = _upload(grouped_context, frame, f"gexp_{order_by}")
    wrapper = GroupByWindowStatsWrapper(ctx)
    ordered = _pandas_frame(frame, order_by)

    q = wrapper.groupby("g").expanding(
        order_by=order_by, min_periods=1
    ).quantile("x", q=0.5)
    got_q = _sorted_tile(_aligned(q), order_by)
    exp_q = (
        ordered.groupby("g", sort=False)["x"]
        .expanding(min_periods=1).quantile(0.5)
        .reset_index(level=0, drop=True)
    )
    np.testing.assert_allclose(
        got_q["x_expanding_q0.5_by_g"], np.asarray(exp_q), rtol=1e-10
    )

    sem = wrapper.groupby("g").expanding(
        order_by=order_by, min_periods=1
    ).sem("x")
    got_sem = _sorted_tile(_aligned(sem), order_by)
    exp_sem = _sem_expected(ordered, window=None)
    np.testing.assert_allclose(
        got_sem["x_expanding_sem_by_g"], exp_sem,
        equal_nan=True, rtol=1e-10,
    )

    nun = wrapper.groupby("g").expanding(
        order_by=order_by, min_periods=1
    ).nunique("x")
    got_nun = _sorted_tile(_aligned(nun), order_by)
    exp_nun = (
        ordered.groupby("g", sort=False)["x"]
        .expanding(min_periods=1)
        .apply(lambda values: values.nunique(), raw=False)
        .reset_index(level=0, drop=True)
    )
    np.testing.assert_allclose(
        got_nun["x_expanding_nunique_by_g"], np.asarray(exp_nun), rtol=1e-10
    )


def test_grouped_datetime_specials(grouped_context):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 4,
            "o": [1, 2, 3, 4],
            "d": pd.date_range("2024-01-01", periods=4),
        }
    )
    ctx = _upload(grouped_context, frame, "gsdt")
    wrapper = GroupByWindowStatsWrapper(ctx)

    mean = wrapper.groupby("g").rolling(2, order_by="o", min_periods=1).agg("d", ["mean"])
    got_mean = _aligned(mean).sort_values("o").reset_index(drop=True)
    assert list(got_mean["d_rolling_mean_w2_by_g"]) == [
        pd.Timestamp("2024-01-01 00:00"),
        pd.Timestamp("2024-01-01 12:00"),
        pd.Timestamp("2024-01-02 12:00"),
        pd.Timestamp("2024-01-03 12:00"),
    ]

    median = wrapper.groupby("g").rolling(2, order_by="o", min_periods=1).agg("d", ["median"])
    got_median = _aligned(median).sort_values("o").reset_index(drop=True)
    assert got_median["d_rolling_median_w2_by_g"].iloc[0] == pd.Timestamp("2024-01-01")

    mode = wrapper.groupby("g").expanding(order_by="o").agg("d", ["mode"])
    got_mode = _aligned(mode).sort_values("o").reset_index(drop=True)
    assert got_mode["d_expanding_mode_by_g"].iloc[-1] == pd.Timestamp("2024-01-01")
