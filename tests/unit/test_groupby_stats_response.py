import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.groupby_stats import GroupByStatsWrapper


@pytest.fixture
def groupby_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "region": ["a", "a", "b", "b"],
                    "sales": [10, 20, 30, 40],
                    "ts": pd.to_datetime(
                        ["2024-01-01", "2024-01-03", "2024-01-01", "2024-01-02"]
                    ),
                }
            ),
            filename="groupby_stats_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_agg_multi_stat_returns_envelope_with_values(groupby_context):
    response = GroupByStatsWrapper(groupby_context).agg(
        group_cols=["region"], agg_dict={"sales": ["sum", "mean"]}
    )

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["new_columns"] == ["sales_sum", "sales_mean"]
    assert response["new_table"]
    assert dict(
        zip(response["result"]["region"], response["result"]["sales_sum"])
    ) == {"a": 30, "b": 70}
    assert dict(
        zip(response["result"]["region"], response["result"]["sales_mean"])
    ) == {"a": 15.0, "b": 35.0}


def test_aagg_with_string_group_col(groupby_context):
    response = asyncio.run(
        GroupByStatsWrapper(groupby_context).aagg(
            group_cols="region", agg_dict={"sales": ["max"]}
        )
    )

    assert response["is_error"] is False
    assert response["new_columns"] == ["sales_max"]
    assert list(response["result"]["sales_max"]) == [20, 40]


@pytest.mark.parametrize(
    ("stat", "expected"),
    [
        ("sum", {"a": 30, "b": 70}),
        ("min", {"a": 10, "b": 30}),
        ("max", {"a": 20, "b": 40}),
        ("count", {"a": 2, "b": 2}),
        ("median", {"a": 15.0, "b": 35.0}),
        ("mode", {"a": 10, "b": 30}),
        ("nunique", {"a": 2, "b": 2}),
        ("range", {"a": 10, "b": 10}),
    ],
)
def test_builder_single_stats_exact_values(groupby_context, stat, expected):
    response = GroupByStatsWrapper(groupby_context).groupby("region").agg(
        {"sales": [stat]}
    )

    assert response["is_error"] is False
    assert response["new_columns"] == [f"sales_{stat}"]
    assert response["new_table"]
    assert (
        dict(zip(response["result"]["region"], response["result"][f"sales_{stat}"]))
        == expected
    )


@pytest.mark.parametrize(
    ("stat", "expected"),
    [
        ("mean", {"a": 15.0, "b": 35.0}),
        ("std", {"a": 5.0, "b": 5.0}),
        ("var", {"a": 25.0, "b": 25.0}),
        ("sem", {"a": 3.535534, "b": 3.535534}),
        ("product", {"a": 200.0, "b": 1200.0}),
    ],
)
def test_builder_float_stats_approx_values(groupby_context, stat, expected):
    response = GroupByStatsWrapper(groupby_context).groupby("region").agg(
        {"sales": [stat]}
    )

    assert response["is_error"] is False
    actual = dict(
        zip(response["result"]["region"], response["result"][f"sales_{stat}"])
    )
    for group, value in expected.items():
        assert actual[group] == pytest.approx(value)


def test_builder_event_rate_shape(groupby_context):
    response = (
        GroupByStatsWrapper(groupby_context).groupby("region").event_rate("ts")
    )

    assert response["is_error"] is False
    assert response["new_columns"] == ["cnt", "event_rate"]
    assert response["new_table"]
    assert dict(zip(response["result"]["region"], response["result"]["cnt"])) == {
        "a": 2,
        "b": 2,
    }


def test_groupby_without_columns_raises(groupby_context):
    with pytest.raises(ValueError, match="at least one group-by column"):
        GroupByStatsWrapper(groupby_context).groupby()


def test_agg_missing_column_returns_canonical_error_shape(groupby_context):
    response = GroupByStatsWrapper(groupby_context).agg(
        group_cols=["region"], agg_dict={"missing_col": ["sum"]}
    )

    assert response["is_error"] is True
    assert response["message"] == ""
    assert response["error_message"]
    assert response.get("result") is None


def test_map_feature_adds_columns_to_original(groupby_context):
    wrapper = GroupByStatsWrapper(groupby_context)
    response = wrapper.groupby("region").agg({"sales": ["sum"]}, map_feature=True)

    assert response["is_error"] is False
    assert response["mapped_columns"] == ["sales_sum"]
    assert response["mapped_table"]
    assert dict(
        zip(response["result"]["region"], response["result"]["sales_sum"])
    ) == {"a": 30, "b": 70}

    original = groupby_context.head(n=10)
    assert "sales_sum" in list(original.columns)
    assert dict(zip(original["region"], original["sales_sum"])) == {
        "a": 30,
        "b": 70,
    }


def test_map_feature_identical_rerun_succeeds(groupby_context):
    wrapper = GroupByStatsWrapper(groupby_context)
    first = wrapper.groupby("region").agg({"sales": ["sum"]}, map_feature=True)
    assert first["is_error"] is False

    second = wrapper.groupby("region").agg({"sales": ["sum"]}, map_feature=True)
    assert second["is_error"] is False

    original = groupby_context.head(n=10)
    assert list(original.columns).count("sales_sum") == 1


def test_map_feature_foreign_collision_errors(groupby_context):
    async def _add_foreign_column():
        table, schema = await groupby_context._get_active_context()
        await groupby_context.memframe._backend.execute(
            f'ALTER TABLE {schema}."{table}" ADD COLUMN sales_sum INTEGER'
        )

    asyncio.run(_add_foreign_column())
    response = (
        GroupByStatsWrapper(groupby_context)
        .groupby("region")
        .agg({"sales": ["sum"]}, map_feature=True)
    )

    assert response["is_error"] is True
    assert response["message"] == ""
    assert "already exist" in response["error_message"]
    assert response.get("result") is None


def test_map_feature_direct_agg(groupby_context):
    response = GroupByStatsWrapper(groupby_context).agg(
        group_cols=["region"], agg_dict={"sales": ["mean"]}, map_feature=True
    )

    assert response["is_error"] is False
    assert response["mapped_columns"] == ["sales_mean"]

    original = groupby_context.head(n=10)
    assert dict(zip(original["region"], original["sales_mean"])) == {
        "a": 15.0,
        "b": 35.0,
    }


def test_map_feature_multicolumn_keys(groupby_context):
    response = (
        GroupByStatsWrapper(groupby_context)
        .groupby("region", "ts")
        .agg({"sales": ["sum"]}, map_feature=True)
    )

    assert response["is_error"] is False

    original = groupby_context.head(n=10)
    assert "sales_sum" in list(original.columns)
    # each (region, ts) group holds a single row, so the mapped sum
    # equals the row's own sales regardless of physical row order
    assert sorted(original["sales_sum"]) == [10, 20, 30, 40]
    assert (original["sales_sum"] == original["sales"]).all()


def test_map_feature_second_spec_stacks(groupby_context):
    wrapper = GroupByStatsWrapper(groupby_context)
    assert (
        wrapper.groupby("region").agg({"sales": ["sum"]}, map_feature=True)[
            "is_error"
        ]
        is False
    )
    response = wrapper.groupby("region").agg(
        {"sales": ["mean"]}, map_feature=True
    )
    assert response["is_error"] is False

    original = groupby_context.head(n=10)
    assert dict(zip(original["region"], original["sales_sum"])) == {
        "a": 30,
        "b": 70,
    }
    assert dict(zip(original["region"], original["sales_mean"])) == {
        "a": 15.0,
        "b": 35.0,
    }
