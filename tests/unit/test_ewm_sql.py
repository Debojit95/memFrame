import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.core.analytix.window.base import WindowOps
from memframe.client import MemFrame
from memframe.wrappers.analytix.window import WindowWrapper


@pytest.fixture
def ewm_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe
    finally:
        asyncio.run(memframe.aclose())


def _upload(memframe, values, name):
    frame = pd.DataFrame(
        {"x": values, "o": list(range(len(values)))}
    )
    return memframe.upload_df(frame, filename=name)


def _run(ctx, wrapper, **kwargs):
    response = wrapper.ewm(column="x", order_by="o", **kwargs)
    assert response["is_error"] is False, response.get("error_message")
    return response["result"].sort_values("o").reset_index(drop=True)


def test_ewm_adjust_true_matches_pandas(ewm_context):
    ctx = _upload(ewm_context, [10.0, 20.0, 30.0, 40.0, 50.0], "ewm_true")
    result = _run(ctx, WindowWrapper(ctx), span=2, adjust=True, func="mean")
    expected = (
        pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        .ewm(span=2, adjust=True)
        .mean()
        .to_numpy()
    )
    np.testing.assert_allclose(result["x_ewm_mean"], expected, rtol=1e-12)


def test_ewm_adjust_false_matches_pandas(ewm_context):
    ctx = _upload(ewm_context, [10.0, 20.0, 30.0, 40.0, 50.0], "ewm_false")
    result = _run(ctx, WindowWrapper(ctx), span=2, adjust=False, func="mean")
    expected = (
        pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        .ewm(span=2, adjust=False)
        .mean()
        .to_numpy()
    )
    np.testing.assert_allclose(result["x_ewm_mean"], expected, rtol=1e-12)


def test_ewm_sum_adjust_true_is_unnormalised_numerator(ewm_context):
    # a=2/3, q=1/3: S_num = [10, 70/3, 340/9]
    ctx = _upload(ewm_context, [10.0, 20.0, 30.0], "ewm_sum")
    result = _run(ctx, WindowWrapper(ctx), span=2, adjust=True, func="sum")
    np.testing.assert_allclose(
        result["x_ewm_sum"], [10.0, 70.0 / 3.0, 340.0 / 9.0], rtol=1e-12
    )


def test_ewm_null_rows_and_ignore_na(ewm_context):
    values = [None, 20.0, None, 40.0, 50.0, None, 70.0, 80.0]
    ctx = _upload(ewm_context, values, "ewm_null")
    wrapper = WindowWrapper(ctx)
    for ignore_na, expected in [
        (
            False,
            [None, 20.0, None, 38.0, 46.75675675675676, None, 66.92857142857143, 76.3726461807618],
        ),
        (
            True,
            [None, 20.0, None, 35.0, 45.38461538461539, None, 62.0, 74.04958677685951],
        ),
    ]:
        result = _run(
            ctx, wrapper, span=2, adjust=True,
            ignore_na=ignore_na, func="mean",
        )
        got = result["x_ewm_mean"].to_numpy(dtype=float)
        mask = np.array([v is not None for v in values])
        np.testing.assert_allclose(got[mask], np.array(expected, dtype=float)[mask], rtol=1e-9)
        assert np.isnan(got[~mask]).all()


def test_ewm_block_boundary(ewm_context, monkeypatch):
    monkeypatch.setattr(WindowOps, "EWM_BLOCK_SIZE", 4)
    values = [None, 20.0, None, 40.0, 50.0, None, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0]
    ctx = _upload(ewm_context, values, "ewm_blocks")
    result = _run(
        ctx, WindowWrapper(ctx), alpha=0.3, adjust=True,
        ignore_na=False, func=["mean", "sum", "std", "var"],
    )
    # Independent recurrence reference.
    a = 0.3
    s_num = s_den = s_w2 = s_x2w = 0.0
    valid = 0
    means = []
    sums = []
    for value in values:
        s_num *= 1 - a
        s_den *= 1 - a
        s_w2 *= (1 - a) ** 2
        s_x2w *= 1 - a
        if value is None:
            means.append(np.nan)
            sums.append(np.nan)
            continue
        valid += 1
        s_num += value
        s_den += 1
        s_w2 += 1
        s_x2w += value * value
        means.append(s_num / s_den)
        sums.append(s_num)
    np.testing.assert_allclose(result["x_ewm_mean"], means, equal_nan=True, rtol=1e-10)
    np.testing.assert_allclose(result["x_ewm_sum"], sums, equal_nan=True, rtol=1e-10)


def test_ewm_alpha_one(ewm_context):
    ctx = _upload(ewm_context, [10.0, None, 30.0], "ewm_one")
    result = _run(
        ctx, WindowWrapper(ctx), alpha=1, adjust=True,
        func=["mean", "sum", "var", "std"],
    )
    np.testing.assert_allclose(
        result["x_ewm_mean"], [10.0, np.nan, 30.0], equal_nan=True
    )
    np.testing.assert_allclose(
        result["x_ewm_sum"], [10.0, np.nan, 30.0], equal_nan=True
    )
    assert result["x_ewm_var"].isna().all()
    assert result["x_ewm_std"].isna().all()


def test_ewm_order_none_uses_upload_order(ewm_context):
    ctx = _upload(ewm_context, [10.0, 20.0, 30.0, 40.0], "ewm_natural")
    wrapper = WindowWrapper(ctx)
    natural = wrapper.ewm(column="x", span=2, func="mean")
    ordered = wrapper.ewm(column="x", span=2, func="mean", order_by="o")
    assert natural["is_error"] is False
    assert ordered["is_error"] is False
    np.testing.assert_allclose(
        natural["result"]["x_ewm_mean"].to_numpy(),
        ordered["result"].sort_values("o")["x_ewm_mean"].to_numpy(),
        rtol=1e-12,
    )


def test_ewm_repeat_is_idempotent_and_keeps_source(ewm_context):
    ctx = _upload(ewm_context, [10.0, 20.0, 30.0, 40.0], "ewm_repeat")
    wrapper = WindowWrapper(ctx)
    first = wrapper.ewm(column="x", span=2, func="mean", order_by="o")
    second = wrapper.ewm(column="x", span=2, func="mean", order_by="o")
    assert first["is_error"] is False
    assert second["is_error"] is False
    source = ctx.head(n=10)
    assert list(source.columns).count("x_ewm_mean") == 1
