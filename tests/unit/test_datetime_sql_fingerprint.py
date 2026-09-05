"""SQL-fingerprint regression net for datetime ops.

Records every SQL string the DatetimeOps class sends through its stub
adapter for a fixed scenario set and compares against a committed snapshot.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_datetime_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.datetime import DatetimeOps
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "datetime_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


class _RecordingMixin:
    def __init__(self):
        self.calls = []

    def _record(self, kind, sql, args):
        self.calls.append([kind, "".join(sql.split()), [str(a) for a in args]])

    async def execute(self, sql, *args):
        self._record("exec", sql, args)

    async def fetch(self, sql, *args):
        self._record("fetch", sql, args)
        return []

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        return 0

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)
        return None

    async def get_column_types(self, table, schema=None):
        return {"ts": "TIMESTAMP", "s": "VARCHAR", "val": "INTEGER"}

    async def table_exists(self, table, schema=None):
        return False


class RecordingDuckAdapter(_RecordingMixin, DuckDBAdapter):
    def __init__(self):
        _RecordingMixin.__init__(self)
        self._pool = None
        self._ddb_pool = None

    def placeholder(self, index=1):
        return "?"

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'


class RecordingPostgresAdapter(_RecordingMixin, PostgresAdapter):
    def __init__(self):
        _RecordingMixin.__init__(self)
        self._pool = None
        self._pg_pool = None

    def placeholder(self, index=1):
        return f"${index}"

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'


class RecordingClickHouseAdapter(_RecordingMixin, ClickHouseAdapter):
    def __init__(self):
        _RecordingMixin.__init__(self)
        self._pool = None

    def placeholder(self, index=1):
        return "?"

    def quote_identifier(self, name):
        return f"`{name}`"


SCENARIOS = {
    "extract_year": lambda ops: ops.extract("t", "s", "ts", "year"),
    "extract_month": lambda ops: ops.extract("t", "s", "ts", "month"),
    "extract_dayofweek": lambda ops: ops.extract("t", "s", "ts", "dayofweek"),
    "extract_quarter": lambda ops: ops.extract("t", "s", "ts", "quarter"),
    "extract_invalid": lambda ops: ops.extract("t", "s", "ts", "invalid"),
    "floor_day": lambda ops: ops.floor("t", "s", "ts", "day"),
    "ceil_month": lambda ops: ops.ceil("t", "s", "ts", "month"),
    "round_hour": lambda ops: ops.round("t", "s", "ts", "hour"),
    "tz_localize_utc": lambda ops: ops.tz_localize("t", "s", "ts", "UTC"),
    "tz_localize_none": lambda ops: ops.tz_localize("t", "s", "ts", None),
    "tz_convert_utc": lambda ops: ops.tz_convert("t", "s", "ts", "UTC"),
    "is_month_start": lambda ops: ops.is_month_start("t", "s", "ts"),
    "is_month_end": lambda ops: ops.is_month_end("t", "s", "ts"),
    "is_year_start": lambda ops: ops.is_year_start("t", "s", "ts"),
    "is_year_end": lambda ops: ops.is_year_end("t", "s", "ts"),
    "is_quarter_start": lambda ops: ops.is_quarter_start("t", "s", "ts"),
    "is_weekend": lambda ops: ops.is_weekend("t", "s", "ts"),
    "days_in_month": lambda ops: ops.days_in_month("t", "s", "ts"),
    "week_of_month": lambda ops: ops.week_of_month("t", "s", "ts"),
    "timestamp": lambda ops: ops.timestamp("t", "s", "ts"),
    "strftime": lambda ops: ops.strftime("t", "s", "ts", "%Y-%m-%d"),
    "strptime": lambda ops: ops.strptime("t", "s", "s", "%Y-%m-%d"),
    "add_timedelta": lambda ops: ops.add_timedelta("t", "s", "ts", "1 day"),
    "sub_timedelta": lambda ops: ops.sub_timedelta("t", "s", "ts", "1 hour"),
    "replace": lambda ops: ops.replace("t", "s", "ts", year=2000),
    "normalize": lambda ops: ops.normalize("t", "s", "ts"),
}


BACKENDS = {
    "duckdb": RecordingDuckAdapter,
    "postgres": RecordingPostgresAdapter,
    "clickhouse": RecordingClickHouseAdapter,
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.datetime")
    real_datetime = base_mod.datetime
    base_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, adapter_cls in BACKENDS.items():
                ops = DatetimeOps(adapter_cls())
                asyncio.run(scenario(ops))
                snapshot[name][backend_name] = ops.db.calls
        return snapshot
    finally:
        base_mod.datetime = real_datetime


def test_datetime_sql_fingerprint_unchanged():
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


def test_datetime_ops_is_correct_class():
    assert DatetimeOps.__name__ == "DatetimeOps"
