import asyncio

import pandas as pd
import pytest

from memframe.core.analytix._response import fail, ok
from memframe.main import MemFrame
from memframe.wrappers.analytix.cleaning import CleaningWrapper


@pytest.fixture
def cleaning_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"value": [1.0, None, 3.0]}),
            filename="cleaning_response",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.mark.parametrize(
    "result",
    [pd.DataFrame({"value": [1]}), {"value": 1}, 1, 1.5, "ok"],
)
def test_ok_preserves_supported_result_values(result):
    response = ok(
        "completed",
        ["value"],
        ["cleaned_value"],
        result,
        result_metadata={"source": "test"},
    )

    assert response["is_error"] is False
    assert response["message"] == "completed"
    assert response["error_message"] is None
    assert response["involved_cols"] == ["value"]
    assert response["generated_cols"] == ["cleaned_value"]
    if isinstance(result, pd.DataFrame):
        assert response["result"].equals(result)
    else:
        assert response["result"] == result
    assert response["result_metadata"] == {"source": "test"}


def test_fail_always_has_the_common_shape():
    response = fail("bad input", ["value"], [])

    assert response == {
        "is_error": True,
        "message": "",
        "error_message": "bad input",
        "involved_cols": ["value"],
        "generated_cols": [],
        "result": None,
    }


def test_cleaning_success_uses_result_and_preserves_metadata(cleaning_context):
    response = CleaningWrapper(cleaning_context).fillna("value", method="mean")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["involved_cols"] == ["value"]
    assert response["generated_cols"]
    assert response["fill_mode"] == "MEAN"
    assert response["new_table"]


def test_cleaning_error_has_result_key(cleaning_context):
    response = CleaningWrapper(cleaning_context).fillna("value", method="unsupported")

    assert response["is_error"] is True
    assert response["error_message"] == "Unsupported mode: UNSUPPORTED"
    assert response["result"] is None


def test_fillna_mean_after_to_numeric_with_leading_null_rows():
    # First 10 rows all NULL, numeric values after: content sampling would
    # mis-detect the column as categorical, but the schema says numeric.
    df = pd.DataFrame(
        {
            "total_cases": [None] * 12 + ["123", "456", "789", "1011"],
            "x": list(range(16)),
        }
    )
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(df, filename="fillna_leading_nulls")
        wrapper = CleaningWrapper(ctx)
        wrapper.to_numeric(column="total_cases")
        response = wrapper.fillna("total_cases", method="mean")

        assert response["is_error"] is False
        assert response["error_message"] is None
        assert response["fill_mode"] == "MEAN"
        assert response["generated_cols"]
    finally:
        asyncio.run(memframe.aclose())
