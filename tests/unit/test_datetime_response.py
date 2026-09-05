import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.datetime import DatetimeOps
from memframe.main import MemFrame
from memframe.wrappers.analytix.datetime import DateTimeWrapper


@pytest.fixture
def datetime_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "ts": pd.to_datetime(
                        ["2023-01-01 10:30:00", "2023-02-15 14:45:00", "2023-12-31 23:59:59"]
                    ),
                    "s": ["2023-01-01", "2023-02-15", "2023-12-31"],
                    "val": [1, 2, 3],
                }
            ),
            filename="datetime_response",
            dtypes={"s": "TEXT"},
        )
    finally:
        asyncio.run(memframe.aclose())


def test_datetime_wrapper_extract_returns_dataframe(datetime_context):
    # DateTimeWrapper is exposed only via ctx.dt, but core wrapper can be tested directly
    response = DateTimeWrapper(datetime_context).year("ts")
    assert response["is_error"] is False
    assert response["error_message"] is None
    assert isinstance(response["result"], pd.DataFrame)
    assert response["involved_cols"] == ["ts"]
    assert response["generated_cols"] == ["dt_ts_year"]
    assert response["new_table"]


def test_datetime_wrapper_floor_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).floor("ts", "day")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert response["generated_cols"] == ["dt_ts_floor_day"]


def test_datetime_wrapper_ceill_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).ceil("ts", "month")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)
    assert response["generated_cols"] == ["dt_ts_ceil_month"]


def test_datetime_wrapper_round_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).round("ts", "hour")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_is_month_start_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).is_month_start("ts")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_timestamp_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).timestamp("ts")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_strftime_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).strftime("ts", "%Y-%m-%d")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_strptime_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).strptime("s", "%Y-%m-%d")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_add_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).add("ts", "1 day")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_wrapper_normalize_returns_dataframe(datetime_context):
    response = DateTimeWrapper(datetime_context).normalize("ts")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_dt_accessor_via_context(datetime_context):
    # via ctx.dt proxy should also succeed (unwrapped via _public_result, but wrapper direct is envelope)
    # wrapper direct is already tested; here test the ContextManager dt path returns envelope when bypassing proxy
    # using DateTimeWrapper directly mirrors dt behavior
    assert hasattr(datetime_context, "dt")
    # dt proxy unwraps to DataFrame when accessed via ContextManager getattr, but DateTimeWrapper returns dict
    # Check that dt property exists and exposes year
    assert hasattr(datetime_context.dt, "year")


def test_datetime_failure_has_common_shape():
    response = asyncio.run(DatetimeOps(object()).extract("t", "s", "ts", "year"))
    assert response["is_error"] is True
    assert response["message"] == ""
    assert response["error_message"]
    # ponytail: DatetimeOps._error_response omits 'result' key; check it is missing or None
    assert response.get("result") is None


def test_datetime_extract_unsupported_field_returns_error(datetime_context):
    response = DateTimeWrapper(datetime_context).extract("ts", "invalid_field")
    assert response["is_error"] is True
    assert "Unsupported datetime field" in response["error_message"]
