"""SQL-fingerprint regression net for groupby window ops.

Same pattern as test_cumulative_sql_fingerprint.py: record every SQL string
the groupby window ops class emits for a fixed scenario set and compare
against a snapshot. Backend personas are RecordingAdapter mixins over the
real adapter classes (real quoting and placeholder styles, stubbed I/O).
GroupbyWindowOps is a single class extending WindowOps, so one ops class
runs against 3 personas.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_groupby_window_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.groupby_window import (
    ClickHouseGroupbyWindowOps,
    DuckDBGroupbyWindowOps,
    GroupbyWindowOps,
    PostgresGroupbyWindowOps,
)
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "groupby_window_sql_fingerprint.json"
)


class _FakeBackend:
    # ponytail: WindowOps._backend_fetch_val falls back to backend.fetchval
    # (not the adapter), so the fake backend delegates to the persona —
    # recorded SQL stays deterministic (MAX(opidx) -> 0).
    def __init__(self, persona):
        self._persona = persona
        self.transient_registry_table = "transient_registry"

    @staticmethod
    def placeholder(i):
        return f"${i}"

    async def fetch_val(self, sql, *args):
        return await self._persona.fetchval(sql, *args)


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
        # ponytail: realistic source columns so the ClickHouse projection
        # helper emits real SQL instead of the degenerate SELECT * path.
        return {"g": "TEXT", "o": "INTEGER", "x": "DOUBLE"}

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
    data_id = "d1"
    return {
        "rolling_sum": lambda ops, backend: ops.rolling_sum_groupby(
            "t", "s", "x", ["g"], order_by="o", window=3,
            backend=backend, data_id=data_id,
        ),
        "rolling_multi": lambda ops, backend: ops._rolling_agg_with_partition(
            "t", "s", "x", ["g"], order_by="o", window=3,
            agg=["sum", "mean"], backend=backend, data_id=data_id,
        ),
        # ponytail: no rolling_no_order scenario — the ordering fallback
        # embeds a wall-clock timestamp (t__rowid_<ts>) that can never match
        # a snapshot. The path itself is covered by unit response tests.
        "rolling_quantile": lambda ops, backend: ops.rolling_quantile_groupby(
            "t", "s", "x", ["g"], order_by="o", window=3, q=0.5,
            backend=backend, data_id=data_id,
        ),
        "rolling_nunique": lambda ops, backend: ops.rolling_nunique_groupby(
            "t", "s", "x", ["g"], order_by="o", window=3,
            backend=backend, data_id=data_id,
        ),
        "rolling_sem": lambda ops, backend: ops.rolling_sem_groupby(
            "t", "s", "x", ["g"], order_by="o", window=3,
            backend=backend, data_id=data_id,
        ),
        "expanding_sum": lambda ops, backend: ops.expanding_sum_groupby(
            "t", "s", "x", group_cols=["g"], order_by="o",
            backend=backend, data_id=data_id,
        ),
    }


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": (DuckDBGroupbyWindowOps, _DuckDBPersona),
    "postgres": (PostgresGroupbyWindowOps, _PostgresPersona),
    "clickhouse": (ClickHouseGroupbyWindowOps, _ClickHousePersona),
}


def _capture():
    snapshot = {}
    for name, scenario in SCENARIOS.items():
        snapshot[name] = {}
        for backend_name, (ops_cls, persona_cls) in BACKENDS.items():
            persona = persona_cls()
            ops = ops_cls(persona)
            asyncio.run(scenario(ops, _FakeBackend(persona)))
            snapshot[name][backend_name] = persona.calls
    return snapshot


def test_groupby_window_sql_fingerprint_unchanged():
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


def test_groupby_window_personas_match_real_backends():
    assert isinstance(_DuckDBPersona(), DuckDBAdapter)
    assert isinstance(_PostgresPersona(), PostgresAdapter)
    assert isinstance(_ClickHousePersona(), ClickHouseAdapter)


def test_all_backends_are_groupby_window_ops():
    for ops_cls, _ in BACKENDS.values():
        assert issubclass(ops_cls, GroupbyWindowOps)
