"""SQL-fingerprint regression net for cumulative ops.

Same pattern as test_arithmetic_sql_fingerprint.py: record every SQL string
the cumulative ops classes emit for a fixed scenario set and compare against
a snapshot. Backend personas are RecordingAdapter mixins over the real
adapter classes (real quoting and placeholder styles, stubbed I/O).

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_cumulative_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.cumulative import (
    ClickHouseCumulativeOps,
    CumulativeOps,
    DuckDBCumulativeOps,
    PostgresCumulativeOps,
)
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "cumulative_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


class _Recording:
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
        return {}

    async def get_table_info(self, table, schema=None):
        return {}

    async def table_exists(self, table, schema=None):
        return False


class _DuckDBPersona(_Recording, DuckDBAdapter):
    pass


class _PostgresPersona(_Recording, PostgresAdapter):
    pass


class _ClickHousePersona(_Recording, ClickHouseAdapter):
    pass


def _scenarios():
    s = {}
    for name in [
        "cumsum",
        "cumprod",
        "cummax",
        "cummin",
        "cummean",
        "cumcount",
        "cumstd",
        "cumvar",
    ]:
        s[name] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "num")
        s[f"{name}_ordered"] = lambda ops, n=name: ops.__getattribute__(n)(
            "t", "s", "num", order_col="g"
        )
        s[f"{name}_target"] = lambda ops, n=name: ops.__getattribute__(n)(
            "t", "s", "num", target_col="run"
        )
    return s


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": (DuckDBCumulativeOps, _DuckDBPersona),
    "postgres": (PostgresCumulativeOps, _PostgresPersona),
    "clickhouse": (ClickHouseCumulativeOps, _ClickHousePersona),
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.cumulative.base")
    real_datetime = base_mod.datetime
    base_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, (ops_cls, persona_cls) in BACKENDS.items():
                ops = ops_cls(persona_cls())
                asyncio.run(scenario(ops))
                snapshot[name][backend_name] = ops.db.calls
        return snapshot
    finally:
        base_mod.datetime = real_datetime


def test_cumulative_sql_fingerprint_unchanged():
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


def test_cumulative_personas_match_real_backends():
    assert isinstance(_DuckDBPersona(), DuckDBAdapter)
    assert isinstance(_PostgresPersona(), PostgresAdapter)
    assert isinstance(_ClickHousePersona(), ClickHouseAdapter)


def test_all_backends_are_cumulative_ops():
    for ops_cls, _ in BACKENDS.values():
        assert issubclass(ops_cls, CumulativeOps)
