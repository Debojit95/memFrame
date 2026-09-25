"""SQL-fingerprint regression net for groupby cumulative ops.

Same pattern as test_cumulative_sql_fingerprint.py: record every SQL string
the groupby cumulative ops classes emit for a fixed scenario set and compare
against a snapshot. Backend personas are RecordingAdapter mixins over the
real adapter classes (real quoting and placeholder styles, stubbed I/O).

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_groupby_cumulative_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.groupby_cumulative import (
    ClickHouseGroupbyCumulativeOps,
    DuckDBGroupbyCumulativeOps,
    GroupbyCumulativeOps,
    PostgresGroupbyCumulativeOps,
)
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "groupby_cumulative_sql_fingerprint.json"
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
        # ponytail: realistic original-table columns so map_feature scenarios
        # exercise the join/swap SQL instead of the empty-columns error path.
        return {"g": "TEXT", "o": "INTEGER", "num": "DOUBLE"}

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
    scenarios = {}
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
        scenarios[name] = (
            lambda ops, n=name: getattr(ops, n)(
                "t", "s", "num", ["g"], order_col="o", backend=backend,
                data_id=data_id,
            )
        )
    scenarios["no_order"] = lambda ops: ops.cumsum(
        "t", "s", "num", ["g"], backend=backend, data_id=data_id,
    )
    scenarios["multi_group_target"] = lambda ops: ops.cummean(
        "t", "s", "num", ["g1", "g2"], order_col="o", target_col="run",
        backend=backend, data_id=data_id,
    )
    scenarios["map_feature"] = lambda ops: ops.cumsum(
        "t", "s", "num", ["g"], order_col="o", backend=backend,
        data_id=data_id, map_feature=True,
    )
    return scenarios


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": (DuckDBGroupbyCumulativeOps, _DuckDBPersona),
    "postgres": (PostgresGroupbyCumulativeOps, _PostgresPersona),
    "clickhouse": (ClickHouseGroupbyCumulativeOps, _ClickHousePersona),
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


def test_groupby_cumulative_sql_fingerprint_unchanged():
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


def test_groupby_cumulative_personas_match_real_backends():
    assert isinstance(_DuckDBPersona(), DuckDBAdapter)
    assert isinstance(_PostgresPersona(), PostgresAdapter)
    assert isinstance(_ClickHousePersona(), ClickHouseAdapter)


def test_all_backends_are_groupby_cumulative_ops():
    for ops_cls, _ in BACKENDS.values():
        assert issubclass(ops_cls, GroupbyCumulativeOps)
