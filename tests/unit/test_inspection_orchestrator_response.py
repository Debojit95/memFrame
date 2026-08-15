import asyncio

import pandas as pd
import pytest

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
            filename="inspection_orchestrator_response",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda wrapper: wrapper.null_analysis(columns="value"), "columns must be"),
        (lambda wrapper: wrapper.astype(), "Provide either dtype_map"),
        (lambda wrapper: wrapper.insert("added", 1), "Value must be a list"),
        (lambda wrapper: wrapper.map("x", datetime_action="bad"), "Invalid datetime_action"),
    ],
)
def test_inspection_validation_errors_are_canonical(inspection_context, call, message):
    response = call(TableOpsWrapper(inspection_context))

    assert response["is_error"] is True
    assert message in response["error_message"]
    assert response["result"] is None
