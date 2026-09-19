import asyncio

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
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "sales": [10.0, 20.0, 30.0, 40.0, 50.0],
                    "day": [1, 2, 3, 4, 5],
                    "ts": pd.date_range("2024-01-01", periods=5, freq="D"),
                    "cat": ["a", "b", "c", "d", "e"],
                }
            ),
            filename="window_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_rolling_sum_values(window_context):
    response = WindowWrapper(window_context).rolling(
        column="sales", window=3, func="sum", order_by="day"
    )

    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_column"] == "sales_rolling_sum_w3"
    assert list(response["result"]["sales_rolling_sum_w3"]) == [
        10.0,
        30.0,
        60.0,
        90.0,
        120.0,
    ]


def test_rolling_mean_values(window_context):
    response = WindowWrapper(window_context).rolling(
        column="sales", window=2, func="mean", order_by="day"
    )

    assert response["is_error"] is False
    assert list(response["result"]["sales_rolling_mean_w2"]) == [
        10.0,
        15.0,
        25.0,
        35.0,
        45.0,
    ]


def test_rolling_multi_func_chains_single_table(window_context):
    response = WindowWrapper(window_context).rolling(
        column="sales", window=3, func=["sum", "mean"], order_by="day"
    )

    assert response["is_error"] is False
    assert response["new_columns"] == [
        "sales_rolling_sum_w3",
        "sales_rolling_mean_w3",
    ]
    assert response["successful_funcs"] == ["sum", "mean"]
    assert response["new_table"]
    assert list(response["result"]["sales_rolling_sum_w3"]) == [
        10.0,
        30.0,
        60.0,
        90.0,
        120.0,
    ]


def test_rolling_unknown_func_is_canonical_error(window_context):
    response = WindowWrapper(window_context).rolling(
        column="sales", window=3, func="frobnicate", order_by="day"
    )

    assert response["is_error"] is True
    assert response["error_message"]
    assert response.get("result") is None


def test_rolling_sum_on_string_col_unsupported_for_dtype(window_context):
    response = WindowWrapper(window_context).rolling(
        column="cat", window=3, func="sum", order_by="day"
    )

    assert response["is_error"] is True
    assert "categorical" in response["error_message"]


def test_rolling_min_on_datetime_col(window_context):
    response = WindowWrapper(window_context).rolling(
        column="ts", window=2, func="min", order_by="day"
    )

    assert response["is_error"] is False
    dates = list(pd.date_range("2024-01-01", periods=5, freq="D"))
    assert list(response["result"]["ts_rolling_min_w2"]) == [
        dates[0],
        dates[0],
        dates[1],
        dates[2],
        dates[3],
    ]


def test_expanding_sum_values(window_context):
    response = WindowWrapper(window_context).expanding(
        column="sales", func="sum", order_by="day"
    )

    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert list(response["result"]["sales_expanding_sum"]) == [
        10.0,
        30.0,
        60.0,
        100.0,
        150.0,
    ]


def test_ewm_mean_on_numeric_col(window_context):
    response = WindowWrapper(window_context).ewm(
        column="sales", span=2, func="mean", order_by="day"
    )

    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert len(response["result"]) == 5


def test_ewm_on_string_col_rejected(window_context):
    response = WindowWrapper(window_context).ewm(
        column="cat", span=2, func="mean", order_by="day"
    )

    assert response["is_error"] is True
    assert "numeric" in response["error_message"]


def test_fluent_builder_rolling_mean(window_context):
    response = (
        WindowWrapper(window_context).on("sales").rolling(2).mean(order_by="day")
    )

    assert response["is_error"] is False
    assert response["new_column"] == "sales_rolling_mean_w2"
    assert list(response["result"]["sales_rolling_mean_w2"]) == [
        10.0,
        15.0,
        25.0,
        35.0,
        45.0,
    ]
