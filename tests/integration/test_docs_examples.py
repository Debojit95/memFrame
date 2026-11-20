"""Keep the documented example dataset and signatures executable."""

import pandas as pd
import pytest

from memframe import MemFrame

EXPECTED_DTYPES = {
    "id": "BIGINT",
    "date": "TIMESTAMP",
    "event_time": "TIMESTAMP",
    "month": "VARCHAR",
    "region": "VARCHAR",
    "category": "VARCHAR",
    "sales": "DOUBLE",
    "salary": "BIGINT",
    "bonus": "BIGINT",
    "score": "DOUBLE",
    "age": "BIGINT",
    "notes": "VARCHAR",
}


def _example_frame() -> pd.DataFrame:
    rows = [
        [1, "2024-01-01", "2024-01-01 09:15", "2024-01", "North", "A", 10.0, 50000, 5000, 88.5, 25, "ok"],
        [2, "2024-01-02", "2024-01-02 10:30", "2024-01", "South", "B", 20.0, 60000, 6000, 92.0, 32, None],
        [3, "2024-01-03", "2024-01-03 11:45", "2024-01", "North", "A", 30.0, 55000, 5500, None, 41, "ok"],
        [4, "2024-02-01", "2024-02-01 14:00", "2024-02", "South", "C", 40.0, 65000, 6500, 60.0, 29, "ok"],
        [5, "2024-02-02", "2024-02-02 15:15", "2024-02", "North", "B", 50.0, 52000, 5200, 95.0, 36, None],
    ]
    frame = pd.DataFrame(
        rows,
        columns=[
            "id", "date", "event_time", "month", "region", "category",
            "sales", "salary", "bonus", "score", "age", "notes",
        ],
    )
    frame["date"] = pd.to_datetime(frame["date"])
    frame["event_time"] = pd.to_datetime(frame["event_time"])
    return frame


@pytest.fixture
def docs_ctx():
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    mf.connect()
    try:
        yield mf.DataFrame(
            _example_frame(),
            filename="example",
            dtypes={"month": "VARCHAR", "notes": "VARCHAR"},
        )
    finally:
        mf.close()


def _result(value):
    return value if isinstance(value, pd.DataFrame) else value["result"]


def test_example_schema(docs_ctx):
    assert docs_ctx.dtypes == EXPECTED_DTYPES
    assert list(_result(docs_ctx.head()).columns) == list(EXPECTED_DTYPES)


def test_example_operations(docs_ctx):
    docs_ctx.add("salary", "bonus", "total_income")
    docs_ctx.fillna("score", method="mean")
    assert docs_ctx.mean("sales") == pytest.approx(30.0)
    assert list(_result(docs_ctx.groupby("region").sum("sales")).columns) == [
        "region", "sales_sum",
    ]

    cumulative = _result(docs_ctx.groupby("region").cumsum("sales", order_col="date"))
    assert "cum_sales_sum_by_region_order_by_date" in cumulative.columns

    rolling = _result(
        docs_ctx.groupby("region").rolling(3, order_by="date").mean("sales")
    )
    assert "sales_rolling_mean_w3_by_region" in rolling.columns

    assert "dt_event_time_year" in list(_result(docs_ctx.dt.year("event_time")).columns)
    assert list(_result(docs_ctx.sort_values("sales", columns=["id", "sales"])).columns) == [
        "id", "sales",
    ]
    assert "mapped_sales" in list(
        _result(docs_ctx.map(func="x * 2", columns=["sales"])).columns
    )


def test_window_ordered_vs_upload_order(docs_ctx):
    ordered = _result(
        docs_ctx.rolling("sales", window=3, func="mean", order_by="date")
    )
    ordered_values = ordered["sales_rolling_mean_w3"].tolist()
    assert pd.isna(ordered_values[0]) and pd.isna(ordered_values[1])
    assert ordered_values[2:] == [20.0, 30.0, 40.0]

    shuffled = _example_frame().iloc[[2, 0, 4, 1, 3]].reset_index(drop=True)
    shuffled_ctx = docs_ctx.memframe.DataFrame(shuffled, filename="shuffled")
    upload_order = _result(
        shuffled_ctx.rolling("sales", window=3, func="mean")
    )
    upload_values = upload_order["sales_rolling_mean_w3"].tolist()
    assert pd.isna(upload_values[0]) and pd.isna(upload_values[1])
    assert upload_values[2:] == pytest.approx([30.0, 26.67, 36.67], rel=1e-3)

def test_cleaning_filter_selection_examples(docs_ctx):
    docs_ctx.replace(column="region", to_replace={"North": "N", "South": "S"})
    docs_ctx.filter_valid(column="region", valid_values=["N", "S"])
    docs_ctx.filter("salary >= 55000 && region == 'N'")
    docs_ctx.select_dtypes(include="numeric")


def test_datetime_transform_examples(docs_ctx):
    docs_ctx.dt.quarter("date")
    docs_ctx.dt.diff("date", "event_time", unit="hour")
    docs_ctx.interact(column1="age", column2="salary")
    docs_ctx.binarize(column="score")


def test_reshape_merge_examples(docs_ctx):
    docs_ctx.melt(id_vars=["id"], value_vars=["sales", "score"])
    left = docs_ctx.memframe.DataFrame(
        pd.DataFrame({"id": [1, 2, 3], "region": ["North", "South", "North"]}),
        filename="left",
    )
    right = docs_ctx.memframe.DataFrame(
        pd.DataFrame({"id": [1, 2, 4], "category": ["A", "B", "C"]}),
        filename="right",
    )
    left.merge(right, on="id")
