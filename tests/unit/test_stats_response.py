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


@pytest.fixture
def stats_mixed_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "sale": [10.0, 20.0, 30.0, None],
                    "qty": [1, 2, 3, 4],
                    "region": ["E", "W", "E", "W"],
                    "ts": pd.to_datetime(
                        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
                    ),
                }
            ),
            filename="stats_mixed_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_mean_no_column_skips_text(stats_mixed_context):
    response = StatsWrapper(stats_mixed_context).mean()

    assert response["is_error"] is False
    assert set(response["result"]) == {"sale", "qty", "ts"}
    assert response["result"]["sale"] == pytest.approx(20.0)
    assert response["result"]["qty"] == pytest.approx(2.5)
    assert set(response["involved_cols"]) == {"sale", "qty", "ts"}


def test_mode_no_column_skips_datetime(stats_mixed_context):
    response = StatsWrapper(stats_mixed_context).mode()

    assert response["is_error"] is False
    assert set(response["result"]) == {"sale", "qty", "region"}


def test_sum_no_column_numeric_only(stats_mixed_context):
    response = StatsWrapper(stats_mixed_context).sum()

    assert response["is_error"] is False
    assert set(response["result"]) == {"sale", "qty"}
    assert response["result"]["sale"] == pytest.approx(60.0)


def test_count_no_column_covers_all(stats_mixed_context):
    response = StatsWrapper(stats_mixed_context).count()

    assert response["is_error"] is False
    assert set(response["result"]) == {"sale", "qty", "region", "ts"}


def test_proportions_no_column_categorical_only(stats_mixed_context):
    response = StatsWrapper(stats_mixed_context).proportions()

    assert response["is_error"] is False
    assert set(response["result"]) == {"region"}


def test_whole_table_no_compatible_column_fails():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"t": ["a", "b"]}),
            filename="stats_text_only",
        )
        response = StatsWrapper(ctx).sum()
        assert response["is_error"] is True
        assert "no compatible columns" in response["error_message"]
        assert response["result"] is None
    finally:
        asyncio.run(memframe.aclose())


def test_single_column_paths_unchanged(stats_mixed_context):
    assert StatsWrapper(stats_mixed_context).mean("sale")["result"] == pytest.approx(20.0)
    assert StatsWrapper(stats_mixed_context).sum("qty")["result"] == 10
