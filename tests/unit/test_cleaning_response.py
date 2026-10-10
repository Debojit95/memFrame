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


def test_interpolate_limit():    # 3-long NaN run: limit=1 fills from the fill edge only (pandas vectors).
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


@pytest.fixture
def replace_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "A": [0, 1, 2, 3, 4],
                    "B": [5, 6, 7, 8, 9],
                    "C": ["a", "b", "c", "d", "e"],
                }
            ),
            filename="replace_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def _replace_values(ctx, col, **kwargs):
    response = CleaningWrapper(ctx).replace(column=col, **kwargs)
    assert response["is_error"] is False, response["error_message"]
    frame = response["result"]
    cols = [col] if isinstance(col, str) else response["involved_cols"]
    return {
        c: [None if v is None or (isinstance(v, float) and pd.isna(v)) else v
            for v in frame[f"cleaned_{c}_replaced"].tolist()]
        for c in cols
    }


def test_replace_scalar_and_lists(replace_context):
    assert _replace_values(replace_context, "A", to_replace=0, value=5) == {"A": [5, 1, 2, 3, 4]}
    assert _replace_values(replace_context, "A", to_replace=[0, 1, 2, 3], value=4) == {
        "A": [4, 4, 4, 4, 4]}
    assert _replace_values(replace_context, "A", to_replace=[0, 1, 2, 3], value=[4, 3, 2, 1]) == {
        "A": [4, 3, 2, 1, 4]}


def test_replace_dict_forms(replace_context):
    assert _replace_values(replace_context, "A", to_replace={0: 10, 1: 100}) == {
        "A": [10, 100, 2, 3, 4]}
    both = _replace_values(replace_context, None, to_replace={"A": 0, "B": 5}, value=100)
    assert both == {"A": [100, 1, 2, 3, 4], "B": [100, 6, 7, 8, 9]}
    assert _replace_values(replace_context, None, to_replace={"A": {0: 100, 4: 400}}) == {
        "A": [100, 1, 2, 3, 400]}


def test_replace_null_keys(replace_context):
    assert _replace_values(replace_context, "C", to_replace={"b": None}) == {
        "C": ["a", None, "c", "d", "e"]}
    assert _replace_values(replace_context, "C", to_replace="b", value=None) == {
        "C": ["a", None, "c", "d", "e"]}


def test_replace_regex(replace_context):
    assert _replace_values(replace_context, "C", to_replace=r"^[ab]$", value="z", regex=True) == {
        "C": ["z", "z", "c", "d", "e"]}
    # ponytail: frame-wide regex skips non-text columns (pandas parity).
    assert _replace_values(replace_context, None, regex=[r"^a$", r"^b$"], value="z") == {
        "C": ["z", "z", "c", "d", "e"]}


def test_replace_guards(replace_context):
    wrapper = CleaningWrapper(replace_context)
    for kwargs, fragment in [
        ({"column": "A", "to_replace": [0, 1], "value": [1]}, "differ in length"),
        ({"column": "A", "to_replace": 1, "value": [2]}, "scalar"),
        ({"to_replace": {"X": 1}, "value": 2}, "not among"),
        ({"column": "A", "regex": True}, "both None"),
        ({"column": "nope", "to_replace": 1, "value": 2}, "not found"),
        ({"column": "A", "to_replace": 1, "value": "x"}, "Conversion Error"),
        ({"column": "A", "to_replace": "^0$", "value": "z", "regex": True}, "cannot match"),
    ]:
        response = wrapper.replace(**kwargs)
        assert response["is_error"] is True, kwargs
        assert fragment in response["error_message"], (kwargs, response["error_message"])
        assert response["result"] is None


def test_interpolate_order_by():
    # physical order and month order disagree: values must follow the
    # declared sequence while rows keep their positions.
    df = pd.DataFrame({"m": [10, 1, 4, 7], "v": [None, 0.0, None, 6.0]})
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(df, filename="interpolate_order_by")
        wrapper = CleaningWrapper(ctx)

        physical = wrapper.interpolate("v")["result"]["cleaned_v_interpolated"].tolist()
        assert physical[1:] == [0.0, 3.0, 6.0]
        assert pd.isna(physical[0])

        for kwargs in ({"order_by": "m"}, {"order_by": ["m"]}):
            response = wrapper.interpolate("v", **kwargs)
            assert response["is_error"] is False, response["error_message"]
            assert response["order_by"] == ["m"]
            assert response["result"]["cleaned_v_interpolated"].tolist() == [6.0, 0.0, 3.0, 6.0]

        # pandas parity: sort, interpolate, map back to original positions
        expected = df.sort_values("m")["v"].interpolate()
        lut = dict(zip(df.sort_values("m")["m"], expected))
        assert [lut[m] for m in df["m"]] == [6.0, 0.0, 3.0, 6.0]

        response = wrapper.interpolate("v", order_by="nope")
        assert response["is_error"] is True
        assert "Order-by" in response["error_message"]
        for bad in ([], [1], ""):
            response = wrapper.interpolate("v", order_by=bad)
            assert response["is_error"] is True
            assert "order_by" in response["error_message"]
    finally:
        asyncio.run(memframe.aclose())
