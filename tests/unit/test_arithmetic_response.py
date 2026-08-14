import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.arithmetic import ArithmeticOps
from memframe.main import MemFrame


@pytest.fixture
def arithmetic_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"value": [1.0, 2.0, 3.0]}),
            filename="arithmetic_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_arithmetic_success_returns_dataframe_result(arithmetic_context):
    response = arithmetic_context.add("value", 2, target_col="adjusted")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["generated_cols"] == ["adjusted"]
    assert response["new_table"]
    assert response["expression"]


def test_arithmetic_failure_returns_common_error_shape():
    response = asyncio.run(
        ArithmeticOps(object()).add("table", "schema", "value", 2)
    )

    assert response["is_error"] is True
    assert response["message"] == ""
    assert response["error_message"]
    assert response["result"] is None
    assert response["involved_cols"] == []
    assert response["generated_cols"] == []
