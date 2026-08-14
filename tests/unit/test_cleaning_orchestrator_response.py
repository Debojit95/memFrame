import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame


@pytest.fixture
def cleaning_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"category": ["a", "b"]}),
            filename="cleaning_orchestrator_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_cleaning_validation_error_is_canonical(cleaning_context):
    response = cleaning_context.fillna("category", method="mean")

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["involved_cols"] == ["category"]
    assert response["result"] is None


def test_groupby_validation_error_is_canonical(cleaning_context):
    response = cleaning_context.groupby_fillna("category", group_cols=[])

    assert response["is_error"] is True
    assert response["error_message"] == "group_cols must be provided for groupby_fillna"
    assert response["result"] is None
