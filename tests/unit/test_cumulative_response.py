import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.cumulative import CumulativeWrapper


@pytest.fixture
def cumulative_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"x": [1.0, 2.0, 3.0], "g": [3, 1, 2]}),
            filename="cumulative_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_cumsum_returns_envelope_with_values(cumulative_context):
    response = CumulativeWrapper(cumulative_context).cumsum("x")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["generated_cols"] == ["cum_x_sum"]
    assert response["new_table"]
    assert list(response["result"]["cum_x_sum"]) == [1.0, 3.0, 6.0]


def test_cumsum_with_order_col_and_target_col(cumulative_context):
    response = CumulativeWrapper(cumulative_context).cumsum(
        "x", order_col="g", target_col="run"
    )

    assert response["is_error"] is False
    assert response["generated_cols"] == ["run"]
    assert dict(zip(response["result"]["g"], response["result"]["run"])) == {
        3: 6.0,
        1: 2.0,
        2: 5.0,
    }


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
def test_all_cumulative_ops_succeed(cumulative_context, op):
    response = getattr(CumulativeWrapper(cumulative_context), op)("x")

    assert response["is_error"] is False
    assert len(response["generated_cols"]) == 1
    assert response["new_table"]


def test_cumulative_failure_returns_canonical_error_shape(cumulative_context):
    response = CumulativeWrapper(cumulative_context).cumsum("missing_col")

    assert response["is_error"] is True
    assert response["message"] == ""
    assert response["error_message"]
    assert response.get("result") is None
    assert response["involved_cols"] == []
    assert response["generated_cols"] == []
