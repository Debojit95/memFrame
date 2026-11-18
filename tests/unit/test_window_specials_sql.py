import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.window import WindowWrapper


@pytest.fixture
def window_context():
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


def _run(wrapper, method, column, **kwargs):
    response = getattr(wrapper, method)(column=column, **kwargs)
    assert response["is_error"] is False, response.get("error_message")
    return response["result"]


def _frame():
    return pd.DataFrame(
        {
            "x": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "o": [1, 2, 3, 4, 5, 6],
            "d": pd.date_range("2024-01-01", periods=6),
        }
    )


def test_rolling_specials_match_pandas(window_context):
    frame = _frame()
    ctx = _upload(window_context, frame, "uspec")
    wrapper = WindowWrapper(ctx)
    x = frame["x"]

    q = _run(wrapper, "rolling", "x", window=3, func="quantile", q=0.5, order_by="o")
    expected_q = x.rolling(3).quantile(0.5).to_numpy()
    np.testing.assert_allclose(
        q["x_rolling_q0.5_w3"], expected_q, equal_nan=True, rtol=1e-12
    )

    sem = _run(
        wrapper, "rolling", "x", window=3, func="sem", order_by="o",
        min_periods=1,
    )
    expected_sem = (
        x.rolling(3, min_periods=1).apply(
            lambda v: np.std(v, ddof=1) / np.sqrt(len(v)) if len(v) > 1 else np.nan,
            raw=True,
        ).to_numpy()
    )
    np.testing.assert_allclose(
        sem["x_rolling_sem_w3"], expected_sem, equal_nan=True, rtol=1e-12
    )

    nun = _run(
        wrapper, "rolling", "x", window=3, func="nunique", order_by="o",
        min_periods=1,
    )
    expected_nun = x.rolling(3, min_periods=1).apply(
        lambda v: pd.Series(v).nunique(), raw=True
    ).to_numpy()
    np.testing.assert_allclose(
        nun["x_rolling_nunique_w3"], expected_nun, rtol=1e-12
    )

    rank = _run(
        wrapper, "rolling", "x", window=3, func="rank", order_by="o",
        min_periods=1,
    )
    values = x.to_numpy()
    expected_rank = []
    for i in range(len(values)):
        lo = max(0, i - 2)
        expected_rank.append(int((values[lo:i + 1] <= values[i]).sum()))
    np.testing.assert_allclose(
        rank["x_rolling_rank_w3"], expected_rank, rtol=1e-12
    )


def test_expanding_specials_match_pandas(window_context):
    frame = _frame()
    ctx = _upload(window_context, frame, "uspexp")
    wrapper = WindowWrapper(ctx)
    x = frame["x"]

    q = _run(
        wrapper, "expanding", "x", func="quantile", q=0.5,
        min_periods=2, order_by="o",
    )
    expected_q = x.expanding(min_periods=2).quantile(0.5).to_numpy()
    np.testing.assert_allclose(
        q["x_expanding_q0.5"], expected_q, equal_nan=True, rtol=1e-12
    )

    sem = _run(
        wrapper, "expanding", "x", func="sem", min_periods=1, order_by="o"
    )
    expected_sem = x.expanding(min_periods=1).apply(
        lambda v: np.std(v, ddof=1) / np.sqrt(len(v)) if len(v) > 1 else np.nan,
        raw=True,
    ).to_numpy()
    np.testing.assert_allclose(
        sem["x_expanding_sem"], expected_sem, equal_nan=True, rtol=1e-12
    )

    nun = _run(
        wrapper, "expanding", "x", func="nunique", min_periods=1, order_by="o"
    )
    expected_nun = x.expanding(min_periods=1).apply(
        lambda v: pd.Series(v).nunique(), raw=True
    ).to_numpy()
    np.testing.assert_allclose(
        nun["x_expanding_nunique"], expected_nun, rtol=1e-12
    )

    rank = _run(
        wrapper, "expanding", "x", func="rank", min_periods=1, order_by="o"
    )
    values = x.to_numpy()
    expected_rank = [
        int((values[:i + 1] <= values[i]).sum()) for i in range(len(values))
    ]
    np.testing.assert_allclose(
        rank["x_expanding_rank"], expected_rank, rtol=1e-12
    )


def test_ungrouped_datetime_specials(window_context):
    frame = _frame()
    ctx = _upload(window_context, frame, "uspecdt")
    wrapper = WindowWrapper(ctx)

    mean = _run(
        wrapper, "rolling", "d", window=2, func="mean", order_by="o",
        min_periods=1,
    ).sort_values("o").reset_index(drop=True)
    assert list(mean["d_rolling_mean_w2"]) == [
        pd.Timestamp("2024-01-01 00:00"),
        pd.Timestamp("2024-01-01 12:00"),
        pd.Timestamp("2024-01-02 12:00"),
        pd.Timestamp("2024-01-03 12:00"),
        pd.Timestamp("2024-01-04 12:00"),
        pd.Timestamp("2024-01-05 12:00"),
    ]

    mode = _run(
        wrapper, "expanding", "d", func="mode", min_periods=1, order_by="o"
    ).sort_values("o").reset_index(drop=True)
    assert mode["d_expanding_mode"].iloc[-1] == pd.Timestamp("2024-01-01")
