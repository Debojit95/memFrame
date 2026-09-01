"""SQL-fingerprint regression net for the stats ops refactor.

Same pattern as the cleaning/arithmetic/selection fingerprint harnesses.
The stub adapter returns dict rows (stats reads them by name).

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_stats_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.stats.base import DataStatsOps
from memframe.core.analytix.stats.clickhouse import ClickHouseDataStatsOps
from memframe.core.analytix.stats.duckdb import DuckDBDataStatsOps
from memframe.core.analytix.stats.postgres import PostgresDataStatsOps

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "stats_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


ROW = {
    "min_dt": "2024-01-01 00:00:00", "max_dt": "2024-06-01 00:00:00",
    "total": 10, "seconds": 100, "cnt": 5, "time_unit": 1, "type": "weekday",
    "holiday": "New Year", "val": 0.5, "cur": 1.0, "prev": 1.0, "d": 1.0,
    "corr": 0.5, "cov": 0.5, "a": 1.0, "b": 2.0, "avg_val": 1.0,
    "value_epoch": 0, "median_epoch": 0, "p_25": 0.0, "p_50": 0.0, "p_75": 0.0,
}


class RecordingAdapter:
    def __init__(self):
        self.calls = []

    def _record(self, kind, sql, args):
        self.calls.append([kind, "".join(sql.split()), [str(a) for a in args]])

    async def execute(self, sql, *args):
        self._record("exec", sql, args)

    async def fetch(self, sql, *args):
        self._record("fetch", sql, args)
        return [dict(ROW)]

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        return 0.5

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)
        return dict(ROW)

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'

    def placeholder(self, index=1):
        return "$1"

    async def get_column_types(self, table, schema=None):
        return {"num": "DOUBLE PRECISION", "txt": "VARCHAR", "ts": "TIMESTAMP"}

    async def table_exists(self, table, schema=None):
        return False


def _scenarios():
    s = {}
    simple_numeric = [
        "numeric_count", "numeric_sum", "numeric_min", "numeric_max",
        "numeric_mean", "numeric_median", "numeric_mode", "numeric_prod",
        "numeric_unique", "numeric_nunique", "numeric_std", "numeric_var",
        "numeric_sem", "numeric_mad", "numeric_iqr", "numeric_range",
        "numeric_skew", "numeric_kurtosis", "numeric_entropy",
        "numeric_coefficient_of_variation", "numeric_outliers_iqr",
        "numeric_outliers_zscore",
    ]
    for name in simple_numeric:
        s[name] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "num")
        s[f"{name}_txt"] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "txt")
    s["numeric_value_counts"] = lambda ops: ops.numeric_value_counts("t", "s", "num", 5)
    s["numeric_quantile"] = lambda ops: ops.numeric_quantile("t", "s", "num", [0.25, 0.75])
    s["numeric_quantile_default"] = lambda ops: ops.numeric_quantile("t", "s", "num")
    s["numeric_autocorr"] = lambda ops: ops.numeric_autocorr("t", "s", "num", 2)
    s["numeric_multi_column_correlation"] = lambda ops: ops.numeric_multi_column_correlation("t", "s", ["num", "txt"])
    s["numeric_multi_column_covariance"] = lambda ops: ops.numeric_multi_column_covariance("t", "s", ["num", "txt"])
    s["categorical_count"] = lambda ops: ops.categorical_count("t", "s", "txt")
    s["categorical_unique"] = lambda ops: ops.categorical_unique("t", "s", "txt")
    s["categorical_nunique"] = lambda ops: ops.categorical_nunique("t", "s", "txt")
    s["categorical_value_counts"] = lambda ops: ops.categorical_value_counts("t", "s", "txt", 5)
    s["categorical_proportions"] = lambda ops: ops.categorical_proportions("t", "s", "txt")
    s["categorical_mode"] = lambda ops: ops.categorical_mode("t", "s", "txt")
    s["categorical_multi_column_crosstab"] = lambda ops: ops.categorical_multi_column_crosstab("t", "s", ["txt", "txt2"])
    s["categorical_multi_column_association"] = lambda ops: ops.categorical_multi_column_association("t", "s", ["txt", "txt2"])
    for name in ["datetime_min", "datetime_max", "datetime_mean", "datetime_median",
                 "datetime_count", "datetime_nunique", "datetime_diff",
                 "datetime_delta_stats", "datetime_event_rate"]:
        s[name] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "ts")
    s["datetime_time_unit_counts"] = lambda ops: ops.datetime_time_unit_counts("t", "s", "ts", "month")
    s["datetime_time_unit_counts_dow"] = lambda ops: ops.datetime_time_unit_counts("t", "s", "ts", "dow")
    s["datetime_weekday_weekend_counts"] = lambda ops: ops.datetime_weekday_weekend_counts("t", "s", "ts")
    s["datetime_holiday_counts"] = lambda ops: ops.datetime_holiday_counts("t", "s", "ts")
    return s


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": DuckDBDataStatsOps,
    "postgres": PostgresDataStatsOps,
    "clickhouse": ClickHouseDataStatsOps,
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.stats.base")
    real_datetime = base_mod.datetime
    base_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, cls in BACKENDS.items():
                adapter = RecordingAdapter()
                ops = cls(adapter)
                asyncio.run(scenario(ops))
                snapshot[name][backend_name] = adapter.calls
        return snapshot
    finally:
        base_mod.datetime = real_datetime


def test_stats_sql_fingerprint_unchanged():
    current = _capture()
    if os.environ.get("MEMFRAME_REGEN_FINGERPRINT"):
        os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
        with open(FIXTURE, "w") as fh:
            json.dump(current, fh, indent=1, sort_keys=True)
        pytest.skip("fingerprint regenerated")

    assert os.path.exists(FIXTURE), "run with MEMFRAME_REGEN_FINGERPRINT=1 first"
    with open(FIXTURE) as fh:
        expected = json.load(fh)

    assert set(current) == set(expected), "scenario set changed"
    diffs = []
    for name in expected:
        for backend in expected[name]:
            if current[name][backend] != expected[name][backend]:
                diffs.append(f"{name}/{backend}")
    assert not diffs, f"SQL changed for: {diffs}"


def test_all_backends_are_stats_ops():
    for cls in BACKENDS.values():
        assert issubclass(cls, DataStatsOps)
