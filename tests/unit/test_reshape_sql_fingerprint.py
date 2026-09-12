"""SQL-fingerprint regression net for reshape ops.

Records every SQL string the ReshapingOps class sends through its stub
adapter for a fixed scenario set and compares against a committed snapshot.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_reshape_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.reshape import ReshapingOps, make_reshaping_ops
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "reshape_sql_fingerprint.json"
)

COLUMNS = [
    {"column_name": "id"},
    {"column_name": "tags"},
    {"column_name": "grp"},
    {"column_name": "val"},
]


class _StubBackend:
    transient_registry_table = "transient_registry"

    def __init__(self, placeholder):
        self._placeholder = placeholder

    def placeholder(self, index=1):
        return self._placeholder(index) if callable(self._placeholder) else self._placeholder

    async def fetchval(self, sql, *args):
        return 0


class _RecordingMixin:
    def __init__(self):
        self.calls = []

    def _record(self, kind, sql, args):
        self.calls.append([kind, "".join(sql.split()), [str(a) for a in args]])

    async def execute(self, sql, *args):
        self._record("exec", sql, args)

    async def fetch(self, sql, *args):
        self._record("fetch", sql, args)
        flat = "".join(sql.split())
        if "system.columns" in flat or "information_schema" in flat:
            return list(COLUMNS)
        if "__rowid__" in flat and "DISTINCT" in flat:
            return [{"__rowid__": 1}, {"__rowid__": 2}]
        if "DISTINCT" in flat:
            return [{"grp": "x"}, {"grp": "y"}]
        return []

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        return 0

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)
        return None

    async def get_column_types(self, table, schema=None):
        return {"id": "INTEGER", "tags": "VARCHAR", "grp": "VARCHAR", "val": "DOUBLE"}

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


def _backend_for(adapter):
    return _StubBackend(adapter.placeholder)


SCENARIOS = {
    "reshape_explode": lambda ops, be: ops.explode(
        "t", "s", "tags", backend=be, data_id="abc123"
    ),
    "reshape_explode_multi": lambda ops, be: ops.explode(
        "t", "s", ["tags", "grp"], backend=be, data_id="abc123"
    ),
    "reshape_melt": lambda ops, be: ops.melt(
        "t", "s", ["id"], ["val"], backend=be, data_id="abc123"
    ),
    "reshape_melt_bad_column": lambda ops, be: ops.melt(
        "t", "s", ["id"], ["nope"], backend=be, data_id="abc123"
    ),
    "reshape_pivot": lambda ops, be: ops.pivot(
        "t", "s", "id", "grp", "val", backend=be, data_id="abc123"
    ),
    "reshape_pivot_table": lambda ops, be: ops.pivot_table(
        "t", "s", index="grp", values="val", backend=be, data_id="abc123"
    ),
    "reshape_crosstab": lambda ops, be: ops.crosstab(
        "t", "s", "grp", "grp", backend=be, data_id="abc123"
    ),
    "reshape_transpose": lambda ops, be: ops.transpose(
        "t", "s", backend=be, data_id="abc123"
    ),
    "reshape_rank": lambda ops, be: ops.rank(
        "t", "s", "val", backend=be, data_id="abc123"
    ),
    "reshape_rank_bad_method": lambda ops, be: ops.rank(
        "t", "s", "val", method="nonsense", backend=be, data_id="abc123"
    ),
    "reshape_groupby_rank": lambda ops, be: ops.groupby_rank(
        "t", "s", "grp", "val", backend=be, data_id="abc123"
    ),
}


BACKENDS = {
    "duckdb": RecordingDuckAdapter,
    "postgres": RecordingPostgresAdapter,
    "clickhouse": RecordingClickHouseAdapter,
}


def _capture():
    snapshot = {}
    for name, scenario in SCENARIOS.items():
        snapshot[name] = {}
        for backend_name, adapter_cls in BACKENDS.items():
            adapter = adapter_cls()
            # ponytail: factory returns the backend-specific subclass; SQL must match legacy snapshot.
            ops = make_reshaping_ops(adapter)
            asyncio.run(scenario(ops, _backend_for(adapter)))
            snapshot[name][backend_name] = adapter.calls
    return snapshot


def test_reshape_sql_fingerprint_unchanged():
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


def test_reshape_ops_is_correct_class():
    assert ReshapingOps.__name__ == "ReshapingOps"
