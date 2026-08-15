import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.stats import DataStatsOps
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
        DataStatsOps(object()).categorical_count("table", "schema", "value")
    )

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["result"] is None
    assert response["involved_cols"] == ["value"]
    assert response["generated_cols"] == []
