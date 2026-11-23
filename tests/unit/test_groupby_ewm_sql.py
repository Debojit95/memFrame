import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.core.analytix.window.base import WindowOps
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


def _upload(ctx, frame, name):
    return ctx.upload_df(frame, filename=name)


def _reference(values, alpha, adjust, ignore_na, min_periods, aggs):
    """The unified/un-grouped EWM contract, used per group."""
    arr = np.array(
        [np.nan if v is None else v for v in values], dtype=np.float64
    )
    out = {f: np.full(len(arr), np.nan) for f in aggs}

    def compute(z, res):
        n = len(z)
        if adjust:
            s_num = s_den = s_w2 = s_x2w = 0.0
            valid = 0
            for t in range(n):
                s_num *= 1 - alpha
                s_den *= 1 - alpha
                s_w2 *= (1 - alpha) ** 2
                s_x2w *= 1 - alpha
                if np.isnan(z[t]):
                    continue
                valid += 1
                s_num += z[t]
                s_den += 1
                s_w2 += 1
                s_x2w += z[t] * z[t]
                if valid < min_periods or s_den <= 0:
                    continue
                mean = s_num / s_den
                if "mean" in res:
                    res["mean"][t] = mean
                if "sum" in res:
                    res["sum"][t] = s_num
                den = s_den - s_w2 / s_den
                var = np.nan
                if valid >= 2 and den > 0:
                    var = max(
                        (s_x2w / s_den - mean * mean) * (s_den / den),
                        0.0,
                    )
                if "var" in res:
                    res["var"][t] = var
                if "std" in res:
                    res["std"][t] = np.sqrt(var) if not np.isnan(var) else np.nan
        else:
            state = s_w2 = s_x2w = 0.0
            valid = 0
            has_state = False
            for t in range(n):
                if np.isnan(z[t]):
                    continue
                valid += 1
                if not has_state:
                    state = z[t]
                    s_w2 = 1.0
                    s_x2w = z[t] * z[t]
                    has_state = True
                else:
                    state = (1 - alpha) * state + alpha * z[t]
                    s_w2 = (1 - alpha) ** 2 * s_w2 + alpha ** 2
                    s_x2w = (1 - alpha) * s_x2w + alpha * z[t] * z[t]
                if valid < min_periods:
                    continue
                if "mean" in res:
                    res["mean"][t] = state
                if "sum" in res:
                    res["sum"][t] = state
                den = 1 - s_w2
                var = np.nan
                if valid >= 2 and den > 0:
                    var = max((s_x2w - state * state) / den, 0.0)
                if "var" in res:
                    res["var"][t] = var
                if "std" in res:
                    res["std"][t] = np.sqrt(var) if not np.isnan(var) else np.nan

    if ignore_na:
        mask = ~np.isnan(arr)
        valid_values = arr[mask]
        temp = {f: np.full(len(valid_values), np.nan) for f in aggs}
        compute(valid_values, temp)
        for f in aggs:
            out[f][mask] = temp[f]
    else:
        compute(arr, out)
    return out


def _check(ctx, frame, group_cols, order_by, **params):
    aggs = params.pop("func", ["mean", "sum", "std", "var"])
    if isinstance(aggs, str):
        aggs = [aggs]
    group_cols = list(group_cols) if isinstance(group_cols, (list, tuple)) else [group_cols]
    wrapper = GroupByWindowStatsWrapper(ctx)
    response = asyncio.run(
        wrapper.aewm(
            column="x", group_cols=group_cols, order_by=order_by,
            func=aggs, **params,
        )
    )
    assert response["is_error"] is False, response.get("error_message")
    result = response["result"]
    alpha = WindowOps._alpha_from_params(
        params.get("com"), params.get("span"),
        params.get("halflife"), params.get("alpha"),
    )
    keys = group_cols + ([order_by] if isinstance(order_by, str) else (order_by or []))
    # Stable natural order inside each group when no explicit order is set.
    for group_key, group_df in frame.groupby(group_cols, sort=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        mask = np.ones(len(result), dtype=bool)
        for col, value in zip(group_cols, group_key):
            mask &= result[col].to_numpy() == value
        got = result.loc[mask].copy()
        if order_by:
            got = got.sort_values(list(order_by) if isinstance(order_by, (list, tuple)) else [order_by])
        expected = _reference(
            group_df["x"].tolist(), alpha,
            params.get("adjust", True),
            params.get("ignore_na", False),
            params.get("min_periods", 0),
            aggs,
        )
        for agg in aggs:
            col = f"x_ewm_{agg}_by_{'_'.join(group_cols)}"
            np.testing.assert_allclose(
                got[col].to_numpy(dtype=float),
                expected[agg],
                equal_nan=True,
                rtol=1e-9,
                atol=1e-9,
            )


def test_grouped_ewm_matches_unified_per_group(grouped_context):
    frame = pd.DataFrame(
        {
            "g": ["a"] * 8 + ["b"] * 6 + ["c"] * 4,
            "o": list(range(18)),
            "x": [
                10.0, None, 30.0, 40.0, 50.0, None, 70.0, 80.0,
                5.0, 15.0, None, 25.0, 35.0, 45.0,
                None, 12.0, 22.0, 32.0,
            ],
        }
    )
    ctx = _upload(grouped_context, frame, "gewm_matrix")
    for adjust in (True, False):
        for ignore_na in (False, True):
            for min_periods in (0, 2):
                _check(
                    ctx, frame, ["g"], "o", span=2, adjust=adjust,
                    ignore_na=ignore_na, min_periods=min_periods,
                )


def test_grouped_ewm_order_none_uses_natural_group_order(grouped_context):
    frame = pd.DataFrame(
        {
            "g": ["a", "b", "a", "b", "a", "b"],
            "o": [10, 10, 20, 20, 30, 30],
            "x": [10.0, 100.0, 20.0, 200.0, 30.0, 300.0],
        }
    )
    ctx = _upload(grouped_context, frame, "gewm_natural")
    response = asyncio.run(
        GroupByWindowStatsWrapper(ctx).aewm(
            column="x", group_cols="g", order_by=None,
            span=2, func="mean",
        )
    )
    assert response["is_error"] is False, response.get("error_message")
    frame = frame.sort_values(["g", "o"]).reset_index(drop=True)
    got = response["result"]
    for group in ("a", "b"):
        gmask = frame["g"] == group
        ggot = got.loc[got["g"] == group, "x_ewm_mean_by_g"].to_numpy()
        gexp = _reference(
            frame.loc[gmask, "x"].tolist(), 2 / 3, True, False, 0, ["mean"]
        )["mean"]
        np.testing.assert_allclose(ggot, gexp, equal_nan=True, rtol=1e-12)


def test_grouped_ewm_adjust_false_semantics(grouped_context):
    frame = pd.DataFrame(
        {"g": ["a"] * 5, "o": list(range(5)), "x": [10.0, 20.0, 30.0, 40.0, 50.0]}
    )
    ctx = _upload(grouped_context, frame, "gewm_false")
    response = asyncio.run(
        GroupByWindowStatsWrapper(ctx).aewm(
            column="x", group_cols="g", order_by="o",
            span=2, adjust=False, func=["mean", "sum", "var", "std"],
        )
    )
    assert response["is_error"] is False, response.get("error_message")
    result = response["result"].sort_values("o")
    np.testing.assert_allclose(
        result["x_ewm_sum_by_g"], result["x_ewm_mean_by_g"], rtol=1e-12
    )
    assert np.isnan(result["x_ewm_var_by_g"].iloc[0])
    assert np.isnan(result["x_ewm_std_by_g"].iloc[0])
    pandas_var = (
        pd.Series(frame["x"].to_numpy())
        .ewm(span=2, adjust=False)
        .var()
        .to_numpy()
    )
    np.testing.assert_allclose(
        result["x_ewm_var_by_g"], pandas_var, equal_nan=True, rtol=1e-12
    )


def test_grouped_ewm_null_decay_adjust_true(grouped_context):
    frame = pd.DataFrame(
        {"g": ["a"] * 3, "o": [1, 2, 3], "x": [10.0, None, 30.0]}
    )
    ctx = _upload(grouped_context, frame, "gewm_nulldecay")
    response = asyncio.run(
        GroupByWindowStatsWrapper(ctx).aewm(
            column="x", group_cols="g", order_by="o",
            span=2, adjust=True, ignore_na=False, func="mean",
        )
    )
    assert response["is_error"] is False, response.get("error_message")
    result = response["result"].sort_values("o")
    assert np.isnan(result["x_ewm_mean_by_g"].iloc[1])
    np.testing.assert_allclose(
        result["x_ewm_mean_by_g"].iloc[2], 28.0, rtol=1e-12
    )


def test_grouped_ewm_multicolumn_and_block_boundary(grouped_context, monkeypatch):
    monkeypatch.setattr(WindowOps, "EWM_BLOCK_SIZE", 4)
    rows = []
    for g1 in ("a", "b"):
        for g2 in ("x", "y"):
            for i in range(9):
                rows.append(
                    {
                        "g1": g1, "g2": g2, "o": i,
                        "x": None if i == 3 else float(10 * i + ord(g1) + ord(g2)),
                    }
                )
    frame = pd.DataFrame(rows)
    ctx = _upload(grouped_context, frame, "gewm_multi")
    _check(
        ctx, frame, ["g1", "g2"], "o", alpha=0.3,
        adjust=True, ignore_na=False, min_periods=0,
    )


def test_grouped_ewm_is_idempotent(grouped_context):
    frame = pd.DataFrame(
        {"g": ["a"] * 4 + ["b"] * 4, "o": [1, 2, 3, 4] * 2,
         "x": [10.0, 20.0, 30.0, 40.0, 5.0, 15.0, 25.0, 35.0]}
    )
    ctx = _upload(grouped_context, frame, "gewm_repeat")
    wrapper = GroupByWindowStatsWrapper(ctx)
    first = asyncio.run(
        wrapper.aewm(column="x", group_cols="g", order_by="o", span=2, func="mean")
    )
    second = asyncio.run(
        wrapper.aewm(column="x", group_cols="g", order_by="o", span=2, func="mean")
    )
    assert first["is_error"] is False
    assert second["is_error"] is False
    assert list(ctx.head(n=10).columns).count("x_ewm_mean_by_g") == 1
