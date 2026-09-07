import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.datetime import DatetimeOps
from memframe.exceptions import OperationError
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
                        ["2022-01-15 10:30:00", "2023-02-15 14:45:00", "2023-12-31 23:59:59"]
                    ),
                    "ts2": pd.to_datetime(
                        ["2022-01-20 10:30:00", "2023-02-20 14:45:00", "2024-01-05 23:59:59"]
                    ),
                    "s": ["2022-01-15", "2023-02-15", "2023-12-31"],
                    "val": [1, 2, 3],
                }
            ),
            filename="datetime_response",
            dtypes={"s": "TEXT"},
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.fixture
def resample_context():
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
                        [
                            "2023-01-01 10:00:00",
                            "2023-01-01 14:00:00",
                            "2023-01-03 09:00:00",
                            "2023-02-01 12:00:00",
                        ]
                    ),
                    "sales": [10.0, 20.0, 30.0, 40.0],
                    "region": ["a", "a", "b", "b"],
                }
            ),
            filename="datetime_resample",
        )
    finally:
        asyncio.run(memframe.aclose())


@pytest.fixture
def asfreq_context():
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
                        ["2023-01-01 10:00:00", "2023-01-03 09:00:00", "2023-01-06 12:00:00"]
                    ),
                    "sales": [10.0, 30.0, 60.0],
                }
            ),
            filename="datetime_asfreq",
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
    # ponytail: error envelopes carry "result" so the proxy raises instead of leaking dicts
    assert "result" in response
    assert response["result"] is None


def test_datetime_extract_unsupported_field_returns_error(datetime_context):
    response = DateTimeWrapper(datetime_context).extract("ts", "invalid_field")
    assert response["is_error"] is True
    assert "Unsupported datetime field" in response["error_message"]


# ------------------------------------------------------------------
# Wave 1: names
# ------------------------------------------------------------------
def test_datetime_day_name_returns_weekday_names(datetime_context):
    response = DateTimeWrapper(datetime_context).day_name("ts")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["dt_ts_day_name"]
    assert response["result"]["dt_ts_day_name"].tolist() == ["Saturday", "Wednesday", "Sunday"]


def test_datetime_month_name_returns_month_names(datetime_context):
    response = DateTimeWrapper(datetime_context).month_name("ts")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["dt_ts_month_name"]
    assert response["result"]["dt_ts_month_name"].tolist() == ["January", "February", "December"]


# ------------------------------------------------------------------
# Wave 1: durations
# ------------------------------------------------------------------
def test_datetime_diff_days(datetime_context):
    response = DateTimeWrapper(datetime_context).diff("ts", "ts2")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["dt_ts_ts2_diff_day"]
    assert response["result"]["dt_ts_ts2_diff_day"].tolist() == [5.0, 5.0, 5.0]


def test_datetime_diff_hours_with_target_col(datetime_context):
    response = DateTimeWrapper(datetime_context).diff("ts", "ts2", unit="hour", target_col="h")
    assert response["is_error"] is False
    assert response["generated_cols"] == ["h"]
    assert response["result"]["h"].tolist() == [120.0, 120.0, 120.0]


def test_datetime_diff_bad_unit(datetime_context):
    response = DateTimeWrapper(datetime_context).diff("ts", "ts2", unit="fortnight")
    assert response["is_error"] is True
    assert "Unsupported diff unit" in response["error_message"]


# ------------------------------------------------------------------
# Wave 1: parsing
# ------------------------------------------------------------------
def test_datetime_to_datetime_format(datetime_context):
    response = DateTimeWrapper(datetime_context).to_datetime("s", format="%Y-%m-%d")
    assert response["is_error"] is False
    assert isinstance(response["result"], pd.DataFrame)


def test_datetime_to_datetime_coerce_nulls_invalid():
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"s": ["2023-01-15", "not-a-date", "2024-05-01"]}),
            filename="datetime_coerce",
            dtypes={"s": "TEXT"},
        )
        response = DateTimeWrapper(ctx).to_datetime("s", errors="coerce")
        assert response["is_error"] is False
        values = response["result"]["dt_s_todatetime"].tolist()
        assert values[0] == pd.Timestamp("2023-01-15")
        assert pd.isna(values[1])
        assert values[2] == pd.Timestamp("2024-05-01")
    finally:
        asyncio.run(memframe.aclose())


def test_datetime_to_datetime_raise_on_invalid():
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"s": ["2023-01-15", "not-a-date"]}),
            filename="datetime_raise",
            dtypes={"s": "TEXT"},
        )
        response = DateTimeWrapper(ctx).to_datetime("s", errors="raise")
        assert response["is_error"] is True
    finally:
        asyncio.run(memframe.aclose())


def test_datetime_to_datetime_bad_errors(datetime_context):
    response = DateTimeWrapper(datetime_context).to_datetime("s", errors="sometimes")
    assert response["is_error"] is True
    assert "errors must be" in response["error_message"]


def test_datetime_to_datetime_epoch_unit(datetime_context):
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"epoch": [1673778600, 1676901600]}),
            filename="datetime_epoch",
        )
        response = DateTimeWrapper(ctx).to_datetime("epoch", unit="s")
        assert response["is_error"] is False
        assert response["generated_cols"] == ["dt_epoch_todatetime"]
    finally:
        asyncio.run(memframe.aclose())


# ------------------------------------------------------------------
# Wave 1: filtering
# ------------------------------------------------------------------
def test_datetime_between_filters_rows(datetime_context):
    # ponytail: date-only bounds normalize to midnight (pandas semantics), so
    # the end bound must clear the Dec 31 23:59:59 row to include it.
    response = DateTimeWrapper(datetime_context).between("ts", "2023-01-01", "2024-01-01")
    assert response["is_error"] is False
    assert len(response["result"]) == 2
    assert response["new_table"]


def test_datetime_before_after_filters_rows(datetime_context):
    before = DateTimeWrapper(datetime_context).before("ts", "2023-01-01")
    assert before["is_error"] is False
    assert len(before["result"]) == 1
    after = DateTimeWrapper(datetime_context).after("ts", "2023-06-01")
    assert after["is_error"] is False
    assert len(after["result"]) == 1


def test_datetime_between_bad_bound(datetime_context):
    response = DateTimeWrapper(datetime_context).between("ts", "not-a-date", "2023-12-31")
    assert response["is_error"] is True
    assert "datetime-like" in response["error_message"]


def test_datetime_select_year_and_month(datetime_context):
    years = DateTimeWrapper(datetime_context).select_year("ts", [2023])
    assert years["is_error"] is False
    assert len(years["result"]) == 2
    months = DateTimeWrapper(datetime_context).select_month("ts", [1, 12])
    assert months["is_error"] is False
    assert len(months["result"]) == 2


def test_datetime_select_month_rejects_out_of_range(datetime_context):
    response = DateTimeWrapper(datetime_context).select_month("ts", [13])
    assert response["is_error"] is True
    assert "1..12" in response["error_message"]


# ------------------------------------------------------------------
# Wave 2: resample + asfreq
# ------------------------------------------------------------------
def test_datetime_resample_count(resample_context):
    response = DateTimeWrapper(resample_context).resample("ts", "D")
    assert response["is_error"] is False
    assert response["result"]["value"].tolist() == [2, 1, 1]


def test_datetime_resample_sum(resample_context):
    response = DateTimeWrapper(resample_context).resample("ts", "D", agg="sum", value_columns="sales")
    assert response["is_error"] is False
    assert response["result"]["value"].tolist() == [30.0, 30.0, 40.0]


def test_datetime_resample_multi_agg(resample_context):
    response = DateTimeWrapper(resample_context).resample("ts", "ME", agg=["sum", "mean"], value_columns="sales")
    assert response["is_error"] is False
    assert set(response["result"].columns) >= {"ts", "sales_sum", "sales_mean"}


def test_datetime_resample_dict_with_group_by(resample_context):
    response = DateTimeWrapper(resample_context).resample(
        "ts", "ME", agg={"sales": "sum"}, group_by=["region"]
    )
    assert response["is_error"] is False
    assert set(response["result"].columns) >= {"ts", "region", "sales_sum"}
    assert len(response["result"]) == 3


def test_datetime_resample_bad_freq(resample_context):
    response = DateTimeWrapper(resample_context).resample("ts", "2h")
    assert response["is_error"] is True
    assert "Unsupported freq" in response["error_message"]


def test_datetime_resample_bad_agg(resample_context):
    response = DateTimeWrapper(resample_context).resample("ts", "D", agg="explode", value_columns="sales")
    assert response["is_error"] is True
    assert "Unsupported agg" in response["error_message"]


def test_datetime_asfreq_plain_and_ffill(asfreq_context):
    plain = DateTimeWrapper(asfreq_context).asfreq("ts", "D")
    assert plain["is_error"] is False
    assert len(plain["result"]) == 6
    assert plain["result"]["sales"].isna().sum() == 3
    filled = DateTimeWrapper(asfreq_context).asfreq("ts", "D", method="ffill")
    assert filled["is_error"] is False
    assert filled["result"]["sales"].tolist() == [10.0, 10.0, 30.0, 30.0, 30.0, 60.0]


def test_datetime_asfreq_bad_method(asfreq_context):
    response = DateTimeWrapper(asfreq_context).asfreq("ts", "D", method="spline")
    assert response["is_error"] is True
    assert "method must be" in response["error_message"]


def test_datetime_asfreq_refuses_duplicate_buckets(resample_context):
    response = DateTimeWrapper(resample_context).asfreq("ts", "D")
    assert response["is_error"] is True
    assert "multiple rows per" in response["error_message"]


# ------------------------------------------------------------------
# Wave 3: add_offset
# ------------------------------------------------------------------
def test_datetime_add_offset_calendar(datetime_context):
    response = DateTimeWrapper(datetime_context).add_offset("ts", months=1)
    assert response["is_error"] is False
    assert response["generated_cols"] == ["dt_ts_offset"]
    got = pd.to_datetime(response["result"]["dt_ts_offset"]).dt.date.tolist()
    assert [str(d) for d in got] == ["2022-02-15", "2023-03-15", "2024-01-31"]


def test_datetime_add_offset_business_day_weekend_start():
    memframe = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"ts": pd.to_datetime(["2023-01-07", "2023-01-06", "2023-01-09"])}),
            filename="datetime_bizday",
        )
        response = DateTimeWrapper(ctx).add_offset("ts", days=1, business_day=True)
        assert response["is_error"] is False
        got = pd.to_datetime(response["result"]["dt_ts_offset"]).dt.date.tolist()
        # Sat -> Mon, Fri -> Mon, Mon -> Tue (pandas BDay semantics)
        assert [str(d) for d in got] == ["2023-01-09", "2023-01-09", "2023-01-10"]
    finally:
        asyncio.run(memframe.aclose())


def test_datetime_add_offset_validation(datetime_context):
    bad_int = DateTimeWrapper(datetime_context).add_offset("ts", months="x")
    assert bad_int["is_error"] is True
    assert "must be an integer" in bad_int["error_message"]
    mixed = DateTimeWrapper(datetime_context).add_offset("ts", months=1, days=1, business_day=True)
    assert mixed["is_error"] is True
    assert "only combine with 'days'" in mixed["error_message"]
    empty = DateTimeWrapper(datetime_context).add_offset("ts")
    assert empty["is_error"] is True
    assert "non-zero" in empty["error_message"]


# ------------------------------------------------------------------
# Public API surface (ctx.dt.* unwraps; errors raise)
# ------------------------------------------------------------------
def test_datetime_public_dt_returns_dataframe(datetime_context):
    result = datetime_context.dt.year("ts")
    assert isinstance(result, pd.DataFrame)
    assert "dt_ts_year" in result.columns


def test_datetime_public_dt_resample_returns_dataframe(resample_context):
    result = resample_context.dt.resample("ts", "D")
    assert isinstance(result, pd.DataFrame)
    assert "value" in result.columns


def test_datetime_public_dt_errors_raise(datetime_context):
    with pytest.raises(OperationError, match="Unsupported datetime field"):
        datetime_context.dt.extract("ts", "eon")
