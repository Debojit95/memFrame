import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.stats import DuckDBDataStatsOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.stats import StatsWrapper


@pytest.fixture
def stats_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "value": [1.0, 2.0, 3.0],
                    "category": ["a", "a", "b"],
                }
            ),
            filename="stats_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_stats_scalar_result_uses_common_envelope(stats_context):
    response = StatsWrapper(stats_context).mean("value")

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert response["result"] == pytest.approx(2.0)


def test_stats_dict_result_uses_common_envelope(stats_context):
    response = StatsWrapper(stats_context).proportions("category")

    assert response["is_error"] is False
    assert isinstance(response["result"], dict)
    assert response["result"] == {"a": pytest.approx(2 / 3), "b": pytest.approx(1 / 3)}


def test_stats_failure_has_result_key():
    response = asyncio.run(
        DuckDBDataStatsOps(object()).categorical_count("table", "schema", "value")
    )

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["result"] is None
    assert response["involved_cols"] == ["value"]
    assert response["generated_cols"] == []


def test_value_counts_single_column(stats_context):
    response = StatsWrapper(stats_context).value_counts("category")

    assert response["is_error"] is False
    assert response["result"] == {"a": 2, "b": 1}


def test_value_counts_no_column_counts_all(stats_context):
    response = StatsWrapper(stats_context).value_counts()

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert set(response["result"]) == {"value", "category"}
    assert response["result"]["category"] == {"a": 2, "b": 1}
    assert set(response["involved_cols"]) == {"value", "category"}


def test_value_counts_no_column_top_n_caps_each(stats_context):
    response = StatsWrapper(stats_context).value_counts(top_n=1)

    assert response["is_error"] is False
    assert response["result"]["category"] == {"a": 2}
    assert len(response["result"]["value"]) == 1
