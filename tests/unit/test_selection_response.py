import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.selection import DataSelectionOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.selection import SelectionWrapper


@pytest.fixture
def selection_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "value": [10, 20, 30],
                }
            ),
            filename="selection_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_selection_scalar_result_uses_result(selection_context):
    response = SelectionWrapper(selection_context).at(
        2, "value", index_column="id"
    )

    assert response["is_error"] is False
    assert response["result"] == 20
    assert "value" not in response


def test_selection_streaming_response_keeps_iterator(selection_context):
    response = SelectionWrapper(selection_context).where("value > 10", chunk_size=2)

    assert response["is_error"] is False
    assert response["result"] is None
    assert response["iterator"]
    assert response["chunk_size"] == 2


def test_selection_failure_has_result_key():
    response = asyncio.run(
        DataSelectionOps(object()).at("row", "schema", 1, "value")
    )

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["result"] is None
