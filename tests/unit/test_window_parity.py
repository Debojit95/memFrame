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


@pytest.mark.parametrize("window", [2, 3, 4, 5])
@pytest.mark.parametrize("center", [False, True])
@pytest.mark.parametrize("closed", [None, "right", "left", "both", "neither"])
def test_rolling_center_closed_matches_pandas(ctx, window, center, closed):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "o": list(range(6))})
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"pf_{window}_{center}_{closed}")
    )
    for func in ("sum", "mean", "min", "max", "count"):
        response = wrapper.rolling(
            column="x", window=window, func=func, order_by="o",
            min_periods=1, center=center, closed=closed,
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]

        roll = frame["x"].rolling(
            window, min_periods=1, center=center, closed=closed
        )
        expected = getattr(roll, func)().to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-12,
        )


@pytest.mark.parametrize("closed", ["right", "left", "both", "neither"])
@pytest.mark.parametrize("center", [False, True])
def test_rolling_center_closed_specials(ctx, center, closed):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "o": list(range(6))})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"ps_{center}_{closed}"))
    values = frame["x"]
    roll = values.rolling(3, min_periods=1, center=center, closed=closed)

    got = _rolling(
        wrapper, "x", 3, "quantile",
        min_periods=1, center=center, closed=closed,
    ).sort_values("o")
    np.testing.assert_allclose(
        got["x_rolling_q0.5_w3"].to_numpy(dtype=float),
        roll.quantile(0.5).to_numpy(), equal_nan=True, rtol=1e-12,
    )

    got = _rolling(
        wrapper, "x", 3, "nunique",
        min_periods=1, center=center, closed=closed,
    ).sort_values("o")
    np.testing.assert_allclose(
        got["x_rolling_nunique_w3"].to_numpy(dtype=float),
        roll.apply(lambda v: v.nunique(), raw=False).to_numpy(),
        equal_nan=True, rtol=1e-12,
    )

    got = _rolling(
        wrapper, "x", 3, "sem",
        min_periods=1, center=center, closed=closed,
    ).sort_values("o")
    expected_sem = roll.sem().to_numpy()
    np.testing.assert_allclose(
        got["x_rolling_sem_w3"].to_numpy(dtype=float), expected_sem,
        equal_nan=True, rtol=1e-12,
    )

    # Rank is only directly comparable when the current row is the frame end
    # (pandas ranks the frame's last row): right/both, uncentered.
    if not center and closed in ("right", "both"):
        got = _rolling(
            wrapper, "x", 3, "rank",
            min_periods=1, center=center, closed=closed,
        ).sort_values("o")
        np.testing.assert_allclose(
            got["x_rolling_rank_w3"].to_numpy(dtype=float),
            roll.rank().to_numpy(), equal_nan=True, rtol=1e-12,
        )


@pytest.mark.parametrize("closed", [None, "right", "left", "both", "neither"])
@pytest.mark.parametrize("center", [False, True])
def test_grouped_rolling_center_closed_matches_pandas(ctx, center, closed):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 5 + ["b"] * 5,
            "x": [1.0, 2.0, None, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0],
            "o": list(range(5)) * 2,
        }
    )
    context = ctx.upload_df(frame, filename=f"pg_{center}_{closed}")
    wrapper = GroupByWindowStatsWrapper(context)

    response = asyncio.run(
        wrapper.arolling(
            column="x", window=3, func="mean", group_cols="g",
            order_by="o", min_periods=1, center=center, closed=closed,
        )
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "o"]).reset_index(drop=True)

    expected = (
        frame.sort_values("o", kind="mergesort")
        .groupby("g", sort=False)["x"]
        .rolling(3, min_periods=1, center=center, closed=closed)
        .mean()
        .reset_index(level=0, drop=True)
        .to_numpy()
    )
    np.testing.assert_allclose(
        got["x_rolling_mean_w3_by_g"].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-12,
    )


def test_rolling_closed_validation(ctx):
    wrapper = WindowWrapper(ctx.upload_df(_frame(), filename="p_closed_bad"))
    response = wrapper.rolling(
        column="x", window=3, func="mean", order_by="o", closed="middle"
    )
    assert response["is_error"] is True
    assert "closed" in response["failed_funcs"]["mean"]

    response = wrapper.rolling(
        column="x", window=1, func="mean", order_by="o", closed="neither"
    )
    assert response["is_error"] is True
    assert "neither" in response["failed_funcs"]["mean"]


@pytest.mark.parametrize("func", ["std", "var", "sem"])
@pytest.mark.parametrize("ddof", [0, 1, 2])
def test_rolling_ddof_matches_pandas(ctx, func, ddof):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "o": list(range(6))})
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"pdd_{func}_{ddof}")
    )
    response = wrapper.rolling(
        column="x", window=4, func=func, order_by="o", min_periods=1,
        func_kwargs={func: {"ddof": ddof}},
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]

    expected = getattr(
        frame["x"].rolling(4, min_periods=1), func
    )(ddof=ddof).to_numpy()
    # pandas returns inf where n <= ddof; MemFrame returns NULL there.
    expected = np.where(np.isinf(expected), np.nan, expected)
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-10,
    )


@pytest.mark.parametrize("func", ["std", "var", "sem"])
def test_expanding_ddof_matches_pandas(ctx, func):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0], "o": list(range(5))})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename=f"ped_{func}"))
    for ddof in (0, 1, 2):
        response = wrapper.expanding(
            column="x", func=func, order_by="o", min_periods=1,
            func_kwargs={func: {"ddof": ddof}},
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_expanding")][0]
        expected = getattr(frame["x"].expanding(min_periods=1), func)(
            ddof=ddof
        ).to_numpy()
        expected = np.where(np.isinf(expected), np.nan, expected)
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-10,
        )


@pytest.mark.parametrize("method", ["average", "min", "max"])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("pct", [False, True])
def test_rolling_rank_params_match_pandas(ctx, method, ascending, pct):
    frame = pd.DataFrame(
        {"x": [1.0, 2.0, 2.0, 4.0, 5.0, 6.0], "o": list(range(6))}
    )
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"prk_{method}_{ascending}_{pct}")
    )
    response = wrapper.rolling(
        column="x", window=3, func="rank", order_by="o", min_periods=1,
        func_kwargs={
            "rank": {
                "method": method, "ascending": ascending, "pct": pct,
            }
        },
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if "rank" in c][0]
    expected = frame["x"].rolling(3, min_periods=1).rank(
        method=method, ascending=ascending, pct=pct
    ).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-12,
    )


def test_rolling_rank_first_and_dense(ctx):
    frame = pd.DataFrame(
        {"x": [1.0, 2.0, 2.0, 4.0, 5.0, 6.0], "o": list(range(6))}
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="prk_extra"))
    for method, expected in (
        ("first", [1, 2, 3, 3, 3, 3]),
        ("dense", [1, 2, 2, 2, 3, 3]),
    ):
        response = wrapper.rolling(
            column="x", window=3, func="rank", order_by="o", min_periods=1,
            func_kwargs={"rank": {"method": method}},
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if "rank" in c][0]
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected, rtol=1e-12,
        )


def test_rolling_and_expanding_median_match_pandas(ctx):
    frame = pd.DataFrame(
        {"x": [1.0, None, 3.0, 4.0, 5.0, 6.0], "o": list(range(6))}
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="pmed"))
    values = frame["x"]

    got = _rolling(wrapper, "x", 3, "median")
    np.testing.assert_allclose(
        got["x_rolling_median_w3"].to_numpy(dtype=float),
        values.rolling(3).median().to_numpy(),
        equal_nan=True, rtol=1e-12,
    )

    response = wrapper.expanding(column="x", func="median", order_by="o")
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    np.testing.assert_allclose(
        got["x_expanding_median"].to_numpy(dtype=float),
        values.expanding().median().to_numpy(),
        equal_nan=True, rtol=1e-12,
    )


def test_grouped_median_ddof_and_rank(ctx):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 5 + ["b"] * 5,
            "x": [1.0, 2.0, 2.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0],
            "o": list(range(5)) * 2,
        }
    )
    context = ctx.upload_df(frame, filename="pg_extra")
    wrapper = GroupByWindowStatsWrapper(context)

    async def _run(func, **func_kwargs):
        return await wrapper.arolling(
            column="x", window=3, func=func, group_cols="g", order_by="o",
            min_periods=1, func_kwargs={func: func_kwargs} if func_kwargs else None,
        )

    ordered = frame.sort_values(["g", "o"], kind="mergesort")
    grouped = ordered.groupby("g", sort=False)["x"]

    response = asyncio.run(_run("median"))
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "o"])
    np.testing.assert_allclose(
        got["x_rolling_median_w3_by_g"].to_numpy(dtype=float),
        grouped.rolling(3, min_periods=1).median().to_numpy(),
        equal_nan=True, rtol=1e-12,
    )

    response = asyncio.run(_run("std", ddof=0))
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "o"])
    np.testing.assert_allclose(
        got["x_rolling_std_w3_by_g"].to_numpy(dtype=float),
        grouped.rolling(3, min_periods=1).std(ddof=0).to_numpy(),
        equal_nan=True, rtol=1e-10,
    )

    response = asyncio.run(_run("rank", method="average"))
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values(["g", "o"])
    np.testing.assert_allclose(
        got["x_rolling_rank_w3_by_g"].to_numpy(dtype=float),
        grouped.rolling(3, min_periods=1).rank().to_numpy(),
        equal_nan=True, rtol=1e-12,
    )


def test_quantile_interpolation_default_and_unsupported(ctx):
    frame = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0], "o": list(range(4))})
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="pqi"))

    response = wrapper.rolling(
        column="x", window=3, func="quantile", order_by="o",
        q=0.25, func_kwargs={"quantile": {"interpolation": "linear"}},
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    np.testing.assert_allclose(
        got["x_rolling_q0.25_w3"].to_numpy(dtype=float),
        frame["x"].rolling(3).quantile(0.25).to_numpy(),
        equal_nan=True, rtol=1e-12,
    )

    response = wrapper.rolling(
        column="x", window=3, func="quantile", order_by="o",
        func_kwargs={"quantile": {"interpolation": "lower"}},
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    np.testing.assert_allclose(
        got["x_rolling_q0.25_w3"].to_numpy(dtype=float)
        if "x_rolling_q0.25_w3" in got.columns
        else got["x_rolling_q0.5_w3"].to_numpy(dtype=float),
        frame["x"].rolling(3).quantile(
            0.25 if "x_rolling_q0.25_w3" in got.columns else 0.5,
            interpolation="lower",
        ).to_numpy(),
        equal_nan=True, rtol=1e-9,
    )

    response = wrapper.rolling(
        column="x", window=3, func="quantile", order_by="o",
        func_kwargs={"quantile": {"interpolation": "bogus"}},
    )
    assert response["is_error"] is True
    assert "interpolation" in response["failed_funcs"]["quantile"]
