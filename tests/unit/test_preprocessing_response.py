import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.preprocessing import PreprocessingWrapper


@pytest.fixture
def preprocessing_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "age": [20.0, 30.0, 40.0, None],
                    "city": ["a", "b", "a", "c"],
                }
            ),
            filename="preprocessing_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_scale_success_returns_canonical_envelope(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).scale("age")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert response["involved_cols"] == ["age"]
    assert response["generated_cols"] == ["transformed_age_standardized"]
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_table"]


def test_onehot_success_returns_generated_cols(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).onehot("city", max_categories=2)

    assert response["is_error"] is False
    assert response["involved_cols"] == ["city"]
    assert len(response["generated_cols"]) == 2
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_table"]


def test_unknown_strategy_error_has_result_key(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).bin("age", bins=2, strategy="bogus")

    assert response["is_error"] is True
    assert response["error_message"] == "Unknown binning strategy: bogus"
    assert response["involved_cols"] == ["age"]
    assert response["result"] is None


def test_robust_scale_returns_canonical(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).robust_scale("age")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_robust"]
    assert isinstance(response["result"], pd.DataFrame)


def test_maxabs_scale_range(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).maxabs_scale("age")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_maxabs"]
    assert isinstance(response["result"], pd.DataFrame)


def test_normalize_sign(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).normalize("age")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_normalized"]
    assert isinstance(response["result"], pd.DataFrame)


def test_log_transform_null_on_nonpositive(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).log_transform("age", base="e", epsilon=0)
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_log"]
    assert isinstance(response["result"], pd.DataFrame)


def test_quantile_transform_returns_canonical(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).quantile_transform("age")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_quantile"]
    assert isinstance(response["result"], pd.DataFrame)


def test_power_transform_returns_canonical(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).power_transform("age")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_age_power"]
    assert isinstance(response["result"], pd.DataFrame)


def test_ordinal_encode_returns_canonical(preprocessing_context):
    response = PreprocessingWrapper(preprocessing_context).ordinal_encode("city")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["transformed_city_ordinal"]
    assert isinstance(response["result"], pd.DataFrame)


def test_context_exposes_preprocessing_methods(preprocessing_context):
    for name in ("scale", "minmax", "bin", "onehot", "binarize", "robust_scale", "maxabs_scale", "normalize", "log_transform", "quantile_transform", "power_transform", "ordinal_encode"):
        assert hasattr(preprocessing_context, name), name
