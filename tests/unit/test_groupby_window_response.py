import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper


@pytest.fixture
def groupby_window_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "g": ["a", "a", "a", "b", "b", "b"],
                    "o": [1, 2, 3, 1, 2, 3],
                    "x": [10.0, 20.0, 30.0, 5.0, 15.0, 25.0],
                }
            ),
            filename="groupby_window_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def _sorted_by(result, keys=("g", "o")):
    return result.sort_values(list(keys)).reset_index(drop=True)


def test_rolling_mean_values(groupby_window_context):
    response = (
        GroupByWindowStatsWrapper(groupby_window_context)
        .groupby("g")
        .rolling(2, order_by="o")
        .mean("x")
    )

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_column"] == "x_rolling_mean_w2_by_g"
    assert response["new_table"]
    frame = _sorted_by(response["result"])
    assert list(frame["x_rolling_mean_w2_by_g"]) == [10.0, 15.0, 25.0, 5.0, 10.0, 20.0]


def test_rolling_sum_values(groupby_window_context):
    response = (
        GroupByWindowStatsWrapper(groupby_window_context)
        .groupby("g")
        .rolling(2, order_by="o")
        .sum("x")
    )

    assert response["is_error"] is False
    frame = _sorted_by(response["result"])
    assert list(frame["x_rolling_sum_w2_by_g"]) == [10.0, 30.0, 50.0, 5.0, 20.0, 40.0]


def test_rolling_multi_func_chains_single_table(groupby_window_context):
    response = (
        GroupByWindowStatsWrapper(groupby_window_context)
        .groupby("g")
        .rolling(2, order_by="o")
        .agg("x", ["sum", "mean"])
    )

    assert response["is_error"] is False
    assert response["new_columns"] == [
        "x_rolling_sum_w2_by_g",
        "x_rolling_mean_w2_by_g",
    ]
    assert response["new_table"]
    frame = _sorted_by(response["result"])
    assert list(frame["x_rolling_sum_w2_by_g"]) == [10.0, 30.0, 50.0, 5.0, 20.0, 40.0]


def test_rolling_without_order_col(groupby_window_context):
    response = (
        GroupByWindowStatsWrapper(groupby_window_context)
        .groupby("g")
        .rolling(2)
        .mean("x")
    )

    assert response["is_error"] is False
    assert response["new_column"] == "x_rolling_mean_w2_by_g"


def test_expanding_sum_values(groupby_window_context):
    response = (
        GroupByWindowStatsWrapper(groupby_window_context)
        .groupby("g")
        .expanding(order_by="o")
        .sum("x")
    )

    assert response["is_error"] is False
    assert response["new_columns"] == ["x_expanding_sum_by_g"]
    assert response["new_table"]
    frame = _sorted_by(response["result"])
    assert list(frame["x_expanding_sum_by_g"]) == [10.0, 30.0, 60.0, 5.0, 20.0, 45.0]


def test_direct_rolling_async(groupby_window_context):
    async def _run():
        return await GroupByWindowStatsWrapper(groupby_window_context).arolling(
            column="x", window=2, func="max", group_cols="g", order_by="o"
        )

    response = asyncio.run(_run())

    assert response["is_error"] is False
    frame = _sorted_by(response["result"])
    assert list(frame["x_rolling_max_w2_by_g"]) == [10.0, 20.0, 30.0, 5.0, 15.0, 25.0]


def test_unified_groupby_routes_window(groupby_window_context):
    response = groupby_window_context.groupby("g").rolling(2, order_by="o").mean("x")

    assert response["is_error"] is False
    assert response["new_column"] == "x_rolling_mean_w2_by_g"


def test_groupby_without_columns_raises(groupby_window_context):
    with pytest.raises(ValueError, match="at least one group-by column"):
        GroupByWindowStatsWrapper(groupby_window_context).groupby()


def test_rolling_missing_column_raises(groupby_window_context):
    # ponytail: dtype detection runs before the core op, so an unknown
    # column raises instead of returning an error envelope.
    with pytest.raises(Exception, match="missing_col"):
        (
            GroupByWindowStatsWrapper(groupby_window_context)
            .groupby("g")
            .rolling(2, order_by="o")
            .mean("missing_col")
        )
