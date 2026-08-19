import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.inspection import GeneralTableOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.inspection import TableOpsWrapper


@pytest.fixture
def inspection_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"value": [1, 2, 3]}),
            filename="inspection_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_inspection_dataframe_result_uses_common_envelope(inspection_context):
    response = TableOpsWrapper(inspection_context).head(n=2)

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["result"].shape == (2, 1)
    assert "current_state" not in response


def test_inspection_current_state_is_moved_to_result(inspection_context):
    response = TableOpsWrapper(inspection_context).insert("added", [4, 5, 6])

    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert "added" in response["result"].columns
    assert "current_state" not in response


def test_inspection_streaming_response_keeps_iterator(inspection_context):
    response = TableOpsWrapper(inspection_context).full_table(chunk_size=2)

    assert response["is_error"] is False
    assert response["result"] is None
    assert response["iterator"]
    assert response["chunk_size"] == 2


def test_inspection_failure_has_result_key():
    response = asyncio.run(GeneralTableOps(object()).dataframe_shape("table", "schema"))

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["result"] is None
