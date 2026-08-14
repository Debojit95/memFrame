import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame


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
        (lambda ctx: ctx.null_analysis(columns="value"), "columns must be"),
        (lambda ctx: ctx.astype(), "Provide either dtype_map"),
        (lambda ctx: ctx.insert("added", 1), "Value must be a list"),
        (lambda ctx: ctx.map("x", datetime_action="bad"), "Invalid datetime_action"),
    ],
)
def test_inspection_validation_errors_are_canonical(inspection_context, call, message):
    response = call(inspection_context)

    assert response["is_error"] is True
    assert message in response["error_message"]
    assert response["result"] is None
