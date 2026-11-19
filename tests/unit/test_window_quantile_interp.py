import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper
from memframe.wrappers.analytix.window import WindowWrapper

INTERPOLATIONS = ["lower", "higher", "nearest", "midpoint"]


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
            "o": list(range(7)),
            "t": pd.to_datetime(
                [f"2024-01-{day:02d}" for day in range(1, 8)]
            ),
        }
    )


def _fkwargs(interp):
    return {"quantile": {"interpolation": interp}}


@pytest.mark.parametrize("interpolation", INTERPOLATIONS)
@pytest.mark.parametrize("q", [0.1, 0.25, 0.5, 0.75, 0.9])
@pytest.mark.parametrize("window", [1, 2, 3, 4, 5])
def test_rolling_interpolation_matches_pandas(ctx, interpolation, q, window):
    frame = _frame()
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"i_{interpolation}_{q}_{window}")
    )
    response = wrapper.rolling(
        column="x", window=window, func="quantile", q=q, order_by="o",
        min_periods=1, func_kwargs=_fkwargs(interpolation),
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = frame["x"].rolling(window, min_periods=1).quantile(
        q, interpolation=interpolation
    ).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("interpolation", INTERPOLATIONS)
def test_expanding_interpolation_matches_pandas(ctx, interpolation):
    frame = _frame()
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"ie_{interpolation}")
    )
    response = wrapper.expanding(
        column="x", func="quantile", q=0.25, order_by="o",
        min_periods=1, func_kwargs=_fkwargs(interpolation),
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_expanding")][0]
    expected = frame["x"].expanding(min_periods=1).quantile(
        0.25, interpolation=interpolation
    ).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


def test_interpolation_edge_cases(ctx):
    frame = pd.DataFrame(
        {"x": [1.0, 2.0, 3.0, 4.0, 5.0], "o": list(range(5))}
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="iedge"))

    # Integer position (n=3, q=0.5 -> pos 1): every mode equals the linear one.
    values = {}
    for interpolation in ["linear", *INTERPOLATIONS]:
        response = wrapper.rolling(
            column="x", window=3, func="quantile", q=0.5, order_by="o",
            min_periods=3, func_kwargs=_fkwargs(interpolation),
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        values[interpolation] = got[column].to_numpy(dtype=float)
    for interpolation in INTERPOLATIONS:
        np.testing.assert_allclose(
            values[interpolation], values["linear"],
            equal_nan=True, rtol=1e-8,
        )

    # nearest half-to-even ties: n=3 with q=0.25 (pos 0.5, idx 0) and q=0.75
    # (pos 1.5, idx 1) must match pandas exactly.
    for q in (0.25, 0.75):
        response = wrapper.rolling(
            column="x", window=3, func="quantile", q=q, order_by="o",
            min_periods=3, func_kwargs=_fkwargs("nearest"),
        )
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        expected = frame["x"].rolling(3, min_periods=3).quantile(
            q, interpolation="nearest"
        ).to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8,
        )

    # q=0 / q=1 -> min / max of the frame.
    for q in (0.0, 1.0):
        response = wrapper.rolling(
            column="x", window=4, func="quantile", q=q, order_by="o",
            min_periods=1, func_kwargs=_fkwargs("lower"),
        )
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        expected = frame["x"].rolling(4, min_periods=1).quantile(
            q, interpolation="lower"
        ).to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8,
        )


@pytest.mark.parametrize("interpolation", ["lower", "nearest"])
def test_interpolation_center_closed_and_time(ctx, interpolation):
    frame = _frame()
    wrapper = WindowWrapper(
        ctx.upload_df(frame, filename=f"ic_{interpolation}")
    )

    response = wrapper.rolling(
        column="x", window=4, func="quantile", q=0.25, order_by="o",
        min_periods=1, center=True, closed="both",
        func_kwargs=_fkwargs(interpolation),
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = frame["x"].rolling(
        4, min_periods=1, center=True, closed="both"
    ).quantile(0.25, interpolation=interpolation).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )

    response = wrapper.rolling(
        column="x", window="3D", func="quantile", q=0.5, order_by="t",
        min_periods=1, func_kwargs=_fkwargs(interpolation),
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("t")
    column = [c for c in got.columns if c.startswith("x_rolling")][0]
    expected = frame.set_index("t", drop=False)["x"].rolling(
        "3D", min_periods=1
    ).quantile(0.5, interpolation=interpolation).to_numpy()
    np.testing.assert_allclose(
        got[column].to_numpy(dtype=float), expected,
        equal_nan=True, rtol=1e-8, atol=1e-10,
    )


@pytest.mark.parametrize("interpolation", INTERPOLATIONS)
def test_grouped_interpolation_matches_pandas(ctx, interpolation):
    frame = _frame().assign(g=["a"] * 4 + ["b"] * 3)
    context = ctx.upload_df(frame, filename=f"ig_{interpolation}")
    wrapper = GroupByWindowStatsWrapper(context)
    ordered = frame.sort_values(["g", "o"], kind="mergesort")

    for method, kwargs, win in (
        ("arolling", {"window": 4}, 4),
        ("aexpanding", {}, None),
    ):
        response = asyncio.run(
            getattr(wrapper, method)(
                column="x", func="quantile", q=0.5, group_cols="g",
                order_by="o", min_periods=1,
                func_kwargs=_fkwargs(interpolation), **kwargs,
            )
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values(["g", "o"])
        column = [c for c in got.columns if "q0.5" in c][0]

        expected = pd.concat(
            [
                (
                    grp["x"].rolling(win, min_periods=1)
                    if win else grp["x"].expanding(min_periods=1)
                ).quantile(0.5, interpolation=interpolation)
                for _, grp in ordered.groupby("g", sort=False)
            ]
        ).to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8, atol=1e-10,
        )


def test_interpolation_min_periods_gate(ctx):
    frame = pd.DataFrame(
        {"x": [1.0, None, None, 4.0], "o": list(range(4))}
    )
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="imp"))
    for interpolation in INTERPOLATIONS:
        response = wrapper.rolling(
            column="x", window=3, func="quantile", q=0.5, order_by="o",
            func_kwargs=_fkwargs(interpolation),
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        expected = frame["x"].rolling(3).quantile(
            0.5, interpolation=interpolation
        ).to_numpy()
        np.testing.assert_allclose(
            got[column].to_numpy(dtype=float), expected,
            equal_nan=True, rtol=1e-8,
        )

    # Not enough observations -> every row NULL.
    sparse = pd.DataFrame({"x": [1.0, None, None], "o": [0, 1, 2]})
    wrapper = WindowWrapper(ctx.upload_df(sparse, filename="impnull"))
    for interpolation in INTERPOLATIONS:
        response = wrapper.rolling(
            column="x", window=2, func="quantile", q=0.5, order_by="o",
            func_kwargs=_fkwargs(interpolation),
        )
        assert response["is_error"] is False, response.get("failed_funcs")
        got = response["result"].sort_values("o")
        column = [c for c in got.columns if c.startswith("x_rolling")][0]
        assert got[column].isna().all()


def test_median_stays_linear(ctx):
    frame = _frame()
    wrapper = WindowWrapper(ctx.upload_df(frame, filename="imlin"))
    response = wrapper.rolling(
        column="x", window=3, func="median", order_by="o"
    )
    assert response["is_error"] is False, response.get("failed_funcs")
    got = response["result"].sort_values("o")
    np.testing.assert_allclose(
        got["x_rolling_median_w3"].to_numpy(dtype=float),
        frame["x"].rolling(3).median().to_numpy(),
        equal_nan=True, rtol=1e-8,
    )
