"""SQL-fingerprint regression net for groupby stats ops.

Same pattern as test_cumulative_sql_fingerprint.py: record every SQL string
the groupby ops classes emit for a fixed scenario set and compare against
a snapshot. Backend personas are RecordingAdapter mixins over the real
adapter classes (real quoting and placeholder styles, stubbed I/O).

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_groupby_stats_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.groupby_stats import (
    ClickHouseGroupByStatsOps,
    DuckDBGroupByStatsOps,
    GroupByStatsOps,
    PostgresGroupByStatsOps,
)
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "groupby_stats_sql_fingerprint.json"
)


class _FakeBackend:
    transient_registry_table = "transient_registry"

    @staticmethod
    def placeholder(i):
        return f"${i}"


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
    backend, data_id = _FakeBackend(), "d1"
    return {
        "common_aggs": lambda ops: ops.group_aggregate(
            "t", "s", ["g"],
            {"num": ["count", "sum", "min", "max", "avg", "mean", "nunique", "range"]},
            backend=backend, data_id=data_id,
        ),
        "spread_aggs": lambda ops: ops.group_aggregate(
            "t", "s", ["g"], {"num": ["std", "var", "sem"]},
            backend=backend, data_id=data_id,
        ),
        "backend_specific_aggs": lambda ops: ops.group_aggregate(
            "t", "s", ["g"], {"num": ["product", "median", "mode"]},
            backend=backend, data_id=data_id,
        ),
        "multi_group_new_table": lambda ops: ops.group_aggregate(
            "t", "s", ["g1", "g2"], {"num": ["sum"]},
            backend=backend, data_id=data_id, new_table="custom_out",
        ),
        "event_rate_day": lambda ops: ops.group_event_rate(
            "t", "s", ["g"], "ts", "day", backend=backend, data_id=data_id,
        ),
        "event_rate_hour": lambda ops: ops.group_event_rate(
            "t", "s", ["g"], "ts", "hour", backend=backend, data_id=data_id,
        ),
        "map_feature": lambda ops: ops.group_aggregate(
            "t", "s", ["g"], {"num": ["sum", "mean"]},
            backend=backend, data_id=data_id, map_feature=True,
        ),
    }


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": (DuckDBGroupByStatsOps, _DuckDBPersona),
    "postgres": (PostgresGroupByStatsOps, _PostgresPersona),
    "clickhouse": (ClickHouseGroupByStatsOps, _ClickHousePersona),
}


def _capture():
    snapshot = {}
    for name, scenario in SCENARIOS.items():
        snapshot[name] = {}
        for backend_name, (ops_cls, persona_cls) in BACKENDS.items():
            persona = persona_cls()
            ops = ops_cls(persona)
            asyncio.run(scenario(ops))
            snapshot[name][backend_name] = persona.calls
    return snapshot


def test_groupby_stats_sql_fingerprint_unchanged():
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


def test_groupby_stats_personas_match_real_backends():
    assert isinstance(_DuckDBPersona(), DuckDBAdapter)
    assert isinstance(_PostgresPersona(), PostgresAdapter)
    assert isinstance(_ClickHousePersona(), ClickHouseAdapter)


def test_all_backends_are_groupby_stats_ops():
    for ops_cls, _ in BACKENDS.values():
        assert issubclass(ops_cls, GroupByStatsOps)
