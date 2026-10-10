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


@pytest.fixture
def interpolate_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "a": [0.0, None, 2.0, None],
                    "b": [None, 2.0, 3.0, 4.0],
                }
            ),
            filename="interpolate_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def _interp_values(ctx, column, **kwargs):
    response = CleaningWrapper(ctx).interpolate(column, **kwargs)
    assert response["is_error"] is False, response["error_message"]
    assert response["fill_mode"] == "LINEAR"
    assert response["generated_cols"] == [f"cleaned_{column}_interpolated"]
    return response["result"][f"cleaned_{column}_interpolated"].tolist()


def test_interpolate_linear_forward(interpolate_context):
    assert _interp_values(interpolate_context, "a") == [0.0, 1.0, 2.0, 2.0]
    result = _interp_values(interpolate_context, "b")
    assert result[1:] == [2.0, 3.0, 4.0]
    assert pd.isna(result[0])


def test_interpolate_directions(interpolate_context):
    backward = _interp_values(interpolate_context, "a", limit_direction="backward")
    assert backward[:3] == [0.0, 1.0, 2.0]
    assert pd.isna(backward[3])
    assert _interp_values(interpolate_context, "a", limit_direction="both") == [0.0, 1.0, 2.0, 2.0]


def test_interpolate_limit_area(interpolate_context):
    inside = _interp_values(interpolate_context, "a", limit_area="inside")
    assert inside[:3] == [0.0, 1.0, 2.0]
    assert pd.isna(inside[3])
    outside = _interp_values(
        interpolate_context, "a", limit_direction="both", limit_area="outside"
    )
    assert outside[0] == 0.0
    assert pd.isna(outside[1])
    assert outside[2:] == [2.0, 2.0]


def test_interpolate_limit():
    # 3-long NaN run: limit=1 fills from the fill edge only (pandas vectors).
    df = pd.DataFrame({"v": [None, None, 5.0, None, None, None, 13.0, None, None]})
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(df, filename="interpolate_limit")
        wrapper = CleaningWrapper(ctx)

        def _vals(**kwargs):
            response = wrapper.interpolate("v", **kwargs)
            assert response["is_error"] is False, response["error_message"]
            return [
                None if v is None or (isinstance(v, float) and pd.isna(v)) else v
                for v in response["result"]["cleaned_v_interpolated"].tolist()
            ]

        assert _vals() == [None, None, 5.0, 7.0, 9.0, 11.0, 13.0, 13.0, 13.0]
        assert _vals(limit=1) == [None, None, 5.0, 7.0, None, None, 13.0, 13.0, None]
        backward = _vals(limit=1, limit_direction="backward")
        assert backward == [None, 5.0, 5.0, None, None, 11.0, 13.0, None, None]
        both = _vals(limit=1, limit_direction="both")
        assert both == [None, 5.0, 5.0, 7.0, None, 11.0, 13.0, 13.0, None]
    finally:
        asyncio.run(memframe.aclose())


def test_interpolate_guards(interpolate_context):
    wrapper = CleaningWrapper(interpolate_context)
    for kwargs, fragment in [
        ({"method": "polynomial"}, "only 'linear'"),
        ({"limit_direction": "sideways"}, "limit_direction"),
        ({"limit_area": "middle"}, "limit_area"),
        ({"limit": 0}, "positive integer"),
        ({"limit": True}, "positive integer"),
    ]:
        response = wrapper.interpolate("a", **kwargs)
        assert response["is_error"] is True
        assert fragment in response["error_message"]
        assert response["result"] is None
