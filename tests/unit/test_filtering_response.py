import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.filter_I import F, SQLContext
from memframe.core.analytix.filter_II import DataFilteringOps
from memframe.main import MemFrame
from memframe.utils.str_filter_parser import ParseError
from memframe.wrappers.analytix.filter import FilteringWrapper


@pytest.fixture
def filtering_context_deep():
    # ponytail: chunk streaming reads the transient table lazily, so it
    # needs deep_cache=True — L1 mode drops the table when the call returns.
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
        deep_cache=True,
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"val_a": [10, 20, 30, 40, 50]}),
            filename="filtering_response_deep",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.fixture
def filtering_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "val_a": [10, 20, 30, 40, 50],
                    "val_b": [10, 25, 25, 40, 55],
                    "cat": ["X", "Y", "Z", "W", None],
                    "date": pd.to_datetime(
                        [
                            "2023-01-01",
                            "2023-02-01",
                            "2023-03-01",
                            "2023-04-01",
                            None,
                        ]
                    ),
                }
            ),
            filename="filtering_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_filter_predicate_object_numeric(filtering_context):
    response = FilteringWrapper(filtering_context).filter(F.num.gte("val_a", 25))

    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["result"]["val_a"].tolist() == [30, 40, 50]
    assert response["new_table"]
    assert response["where_clause"]
    assert response["params"] == [25]


def test_filter_string_expression_column_vs_column(filtering_context):
    response = FilteringWrapper(filtering_context).filter("val_a >= val_b")

    assert response["is_error"] is False
    assert response["result"]["val_a"].tolist() == [10, 30, 40]


def test_filter_categorical_eq_and_contains(filtering_context):
    response = FilteringWrapper(filtering_context).filter(F.cat.eq("cat", "X"))

    assert response["is_error"] is False
    assert response["result"]["cat"].tolist() == ["X"]

    response = FilteringWrapper(filtering_context).filter(
        F.cat.contains("cat", "Y")
    )

    assert response["is_error"] is False
    assert response["result"]["cat"].tolist() == ["Y"]


def test_filter_datetime_after_and_between(filtering_context):
    response = FilteringWrapper(filtering_context).filter(
        F.time.after("date", "2023-02-15")
    )

    assert response["is_error"] is False
    assert response["result"]["val_a"].tolist() == [30, 40]

    response = FilteringWrapper(filtering_context).filter(
        F.time.between("date", "2023-01-15", "2023-03-15")
    )

    assert response["is_error"] is False
    assert response["result"]["val_a"].tolist() == [20, 30]


def test_filter_logical_combinations(filtering_context):
    response = FilteringWrapper(filtering_context).filter(
        F.num.gte("val_a", 20) & F.cat.ne("cat", "Y")
    )

    assert response["is_error"] is False
    # ponytail: row 5 (cat None) is excluded — NULL != 'Y' is NULL, not true.
    assert response["result"]["val_a"].tolist() == [30, 40]

    response = FilteringWrapper(filtering_context).filter(
        F.num.lt("val_a", 20) | F.num.gt("val_a", 40)
    )

    assert response["is_error"] is False
    assert response["result"]["val_a"].tolist() == [10, 50]

    response = FilteringWrapper(filtering_context).filter(
        ~F.num.gte("val_a", 50)
    )

    assert response["is_error"] is False
    assert response["result"]["val_a"].tolist() == [10, 20, 30, 40]


def test_filter_column_subset(filtering_context):
    response = FilteringWrapper(filtering_context).filter(
        F.num.gte("val_a", 40), columns=["val_a", "cat"]
    )

    assert response["is_error"] is False
    assert list(response["result"].columns) == ["val_a", "cat"]
    assert response["result"]["val_a"].tolist() == [40, 50]


def test_filter_chunk_iterator(filtering_context_deep):
    response = FilteringWrapper(filtering_context_deep).filter(
        F.num.gte("val_a", 20), chunk_size=2
    )

    assert response["is_error"] is False
    assert "result" not in response
    assert response["new_table"]

    chunks = asyncio.run(_collect(response["iterator"]))
    combined = pd.concat(chunks, ignore_index=True)
    assert combined["val_a"].tolist() == [20, 30, 40, 50]


async def _collect(iterator):
    return [chunk async for chunk in iterator]


def test_filter_parse_error_raises(filtering_context):
    with pytest.raises(ParseError):
        FilteringWrapper(filtering_context).filter("val_a ??? val_b")


def test_filter_core_unsupported_backend_returns_error():
    response = asyncio.run(
        DataFilteringOps(object(), "duckdb").filter_table(
            "t",
            "s",
            F.num.gt("val_a", 1),
            backend=None,
            data_id=None,
        )
    )

    assert response["is_error"] is True
    assert response["error_message"]


def test_filter_predicate_compile_sql():
    ctx = SQLContext()
    sql = F.num.gt("val_a", 25).compile(ctx)

    assert sql == '("val_a" > $1)'
    assert ctx.params == [25]

    ctx = SQLContext()
    sql = (F.cat.eq("cat", "X") & F.num.lt("val_a", 100)).compile(ctx)

    assert sql == '(("cat" = $1) AND ("val_a" < $2))'
    assert ctx.params == ["X", 100]


def test_filter_categorical_in_clause(filtering_context):
    response = FilteringWrapper(filtering_context).filter(
        F.cat.in_("cat", ["X", "Z"])
    )

    assert response["is_error"] is False
    assert sorted(response["result"]["cat"].tolist()) == ["X", "Z"]
