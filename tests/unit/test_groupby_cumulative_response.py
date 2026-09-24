import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_cumulative import GroupByCumulativeWrapper


@pytest.fixture
def groupby_cumulative_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "g": ["a", "a", "b", "b"],
                    "x": [1.0, 2.0, 3.0, 4.0],
                }
            ),
            filename="groupby_cumulative_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def _by_x(result, col):
    # physical row order is not guaranteed; map each x to its running value
    return dict(zip(result["x"], result[col]))


def test_cumsum_values(groupby_cumulative_context):
    response = GroupByCumulativeWrapper(groupby_cumulative_context).groupby("g").cumsum("x")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_columns"] == ["cum_x_sum_by_g"]
    assert response["new_table"]
    assert _by_x(response["result"], "cum_x_sum_by_g") == {
        1.0: 1.0,
        2.0: 3.0,
        3.0: 3.0,
        4.0: 7.0,
    }


def test_cumprod_values(groupby_cumulative_context):
    response = GroupByCumulativeWrapper(groupby_cumulative_context).groupby("g").cumprod("x")

    assert response["is_error"] is False
    prod = _by_x(response["result"], "cum_x_prod_by_g")
    for x, value in {1.0: 1.0, 2.0: 2.0, 3.0: 3.0, 4.0: 12.0}.items():
        assert prod[x] == pytest.approx(value)


def test_cummax_cummin_values(groupby_cumulative_context):
    wrapper = GroupByCumulativeWrapper(groupby_cumulative_context)

    assert _by_x(wrapper.groupby("g").cummax("x")["result"], "cum_x_max_by_g") == {
        1.0: 1.0,
        2.0: 2.0,
        3.0: 3.0,
        4.0: 4.0,
    }
    assert _by_x(wrapper.groupby("g").cummin("x")["result"], "cum_x_min_by_g") == {
        1.0: 1.0,
        2.0: 1.0,
        3.0: 3.0,
        4.0: 3.0,
    }


def test_cummean_cumcount_values(groupby_cumulative_context):
    wrapper = GroupByCumulativeWrapper(groupby_cumulative_context)

    mean = _by_x(wrapper.groupby("g").cummean("x")["result"], "cum_x_mean_by_g")
    for x, value in {1.0: 1.0, 2.0: 1.5, 3.0: 3.0, 4.0: 3.5}.items():
        assert mean[x] == pytest.approx(value)
    assert _by_x(wrapper.groupby("g").cumcount("x")["result"], "cum_x_count_by_g") == {
        1.0: 1,
        2.0: 2,
        3.0: 1,
        4.0: 2,
    }


def test_cumstd_cumvar_values(groupby_cumulative_context):
    wrapper = GroupByCumulativeWrapper(groupby_cumulative_context)

    std = _by_x(wrapper.groupby("g").cumstd("x")["result"], "cum_x_std_by_g")
    for x, value in {1.0: 0.0, 2.0: 0.5, 3.0: 0.0, 4.0: 0.5}.items():
        assert std[x] == pytest.approx(value)
    var = _by_x(wrapper.groupby("g").cumvar("x")["result"], "cum_x_var_by_g")
    for x, value in {1.0: 0.0, 2.0: 0.25, 3.0: 0.0, 4.0: 0.25}.items():
        assert var[x] == pytest.approx(value)


@pytest.mark.parametrize(
    "op",
    [
        "cumsum",
        "cumprod",
        "cummax",
        "cummin",
        "cummean",
        "cumcount",
        "cumstd",
        "cumvar",
    ],
)
def test_all_groupby_cumulative_ops_succeed(groupby_cumulative_context, op):
    response = getattr(
        GroupByCumulativeWrapper(groupby_cumulative_context).groupby("g"), op
    )("x")

    assert response["is_error"] is False
    assert len(response["new_columns"]) == 1
    assert response["new_table"]
    assert response["group_cols"] == ["g"]


def test_unified_groupby_routes_cumulative(groupby_cumulative_context):
    response = groupby_cumulative_context.groupby("g").cumsum("x")

    assert response["is_error"] is False
    assert response["new_columns"] == ["cum_x_sum_by_g"]


def test_groupby_without_columns_raises(groupby_cumulative_context):
    with pytest.raises(ValueError, match="at least one group-by column"):
        GroupByCumulativeWrapper(groupby_cumulative_context).groupby()


def test_cumsum_missing_column_returns_canonical_error_shape(
    groupby_cumulative_context,
):
    response = (
        GroupByCumulativeWrapper(groupby_cumulative_context)
        .groupby("g")
        .cumsum("missing_col")
    )

    assert response["is_error"] is True
    assert response["message"] == ""
    assert response["error_message"]
    assert response.get("result") is None
