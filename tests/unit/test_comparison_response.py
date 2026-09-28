import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.comparison import ComparisonOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.comparison import ComparisonWrapper


@pytest.fixture
def comparison_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "A": [1, 5, 3],
                    "B": [2, 4, 3],
                    "C": ["x", "y", "x"],
                    "D": ["x", "z", "x"],
                    "E": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                    "F": pd.to_datetime(["2024-01-02", "2024-01-15", "2024-03-01"]),
                }
            ),
            filename="comparison_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_compare_expression_form_success(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("A >= B")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert list(response["result"].columns) == ["A", "B", "cmp_A_ge_B"]
    assert response["result"]["cmp_A_ge_B"].tolist() == [False, True, True]
    assert response["new_column"] == "cmp_A_ge_B"
    assert response["new_table"]


def test_compare_three_arg_form_success(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("A", "B", ">=")

    assert response["is_error"] is False
    assert response["result"]["cmp_A_ge_B"].tolist() == [False, True, True]


def test_compare_categorical_success(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("C == D")

    assert response["is_error"] is False
    assert response["result"]["cmp_C_eq_D"].tolist() == [True, False, True]


def test_compare_datetime_success(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("E < F")

    assert response["is_error"] is False
    assert response["result"]["cmp_E_lt_F"].tolist() == [True, False, False]


def test_compare_invalid_operator(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("A", "B", "===")

    assert response["is_error"] is True
    assert "Invalid operator" in response["error_message"]


def test_compare_dtype_mismatch(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("A", "C", "==")

    assert response["is_error"] is True
    assert "Datatype mismatch" in response["error_message"]


def test_compare_parse_error(comparison_context):
    response = ComparisonWrapper(comparison_context).compare("not a comparison")

    assert response["is_error"] is True


def test_compare_core_unsupported_backend_raises():
    with pytest.raises(NotImplementedError, match="Unsupported database backend"):
        asyncio.run(
            ComparisonOps(object()).compare_numeric("t", "s", "a", "b", "==")
        )


def test_compare_sql_operator_and_column_naming():
    ops = ComparisonOps.__new__(ComparisonOps)

    assert ops._sql_operator("==") == "="
    assert ops._sql_operator(">=") == ">="
    assert ops._generate_cleaned_column_name("A", "==", "B") == "cmp_A_eq_B"
    assert ops._generate_cleaned_column_name("A", ">=", "B") == "cmp_A_ge_B"
