"""SQL-fingerprint regression net for index ops.

Records every SQL string the DataIndexOps class sends through its stub
adapter (plus the stub backend used for registry I/O) for a fixed scenario
set and compares against a committed snapshot.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_index_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.index import DataIndexOps, make_index_ops
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "index_sql_fingerprint.json"
)

TABLE_COLS = {"month": "INTEGER", "year": "INTEGER", "sale": "DOUBLE"}


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
        return dict(TABLE_COLS)

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


class _FakeBackend:
    """Stub DatabaseBackend: records registry SQL, keeps index_cols in memory."""

    csv_registry_table = "memframe_csv_registry.memframe_csv_registry"
    upload_schema = "memframe_upload"
    transient_schema = "memframe_transient"

    def __init__(self, style, preset=None, others=None):
        self.calls = []
        self._style = style
        self._store = {"abc123": list(preset or [])}
        self._others = others or {}

    def placeholder(self, index):
        return f"${index}" if self._style == "postgres" else "?"

    async def fetchval(self, sql, *args):
        self.calls.append(["fetchval", "".join(sql.split()), [str(a) for a in args]])
        data_id = args[0] if args else None
        cols = self._store.get(data_id, self._others.get(data_id, {}).get("index_cols", []))
        return json.dumps(cols) if cols else None

    async def fetch(self, sql, *args):
        self.calls.append(["fetch", "".join(sql.split()), [str(a) for a in args]])
        data_id = args[0] if args else None
        if data_id in self._others:
            o = self._others[data_id]
            return [(o["table"], o["schema"], json.dumps(o["index_cols"]))]
        return []

    async def execute(self, sql, *args):
        self.calls.append(["exec", "".join(sql.split()), [str(a) for a in args]])
        if args and len(args) >= 2:
            value, data_id = args[0], args[1]
            self._store[data_id] = json.loads(value) if value else []


OTHERS = {"def456": {"table": "otbl", "schema": "memframe_upload", "index_cols": ["month"]}}

# name -> (preset index_cols, others, scenario lambda)
SCENARIOS = {
    "set_single": ([], None, lambda ops, be: ops.set_index(
        "t", "s", "month", backend=be, data_id="abc123")),
    "set_multi": ([], None, lambda ops, be: ops.set_index(
        "t", "s", ["year", "month"], backend=be, data_id="abc123")),
    "set_append": (["year"], None, lambda ops, be: ops.set_index(
        "t", "s", "month", backend=be, data_id="abc123", append=True)),
    "set_verify": ([], None, lambda ops, be: ops.set_index(
        "t", "s", "month", backend=be, data_id="abc123", verify_integrity=True)),
    "set_unknown_col": ([], None, lambda ops, be: ops.set_index(
        "t", "s", "nope", backend=be, data_id="abc123")),
    "reset_all": (["month"], None, lambda ops, be: ops.reset_index(
        "t", "s", backend=be, data_id="abc123")),
    "reset_level": (["year", "month"], None, lambda ops, be: ops.reset_index(
        "t", "s", backend=be, data_id="abc123", level="year")),
    "reset_empty": ([], None, lambda ops, be: ops.reset_index(
        "t", "s", backend=be, data_id="abc123")),
    "get_set": (["month"], None, lambda ops, be: ops.get_index(
        "t", "s", backend=be, data_id="abc123")),
    "get_synthetic": ([], None, lambda ops, be: ops.get_index(
        "t", "s", backend=be, data_id="abc123")),
    "reindex_plain": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2, 4], backend=be, data_id="abc123")),
    "reindex_fill": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2, 4], backend=be, data_id="abc123", fill_value=0)),
    "reindex_columns": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", columns=["sale", "nope"], backend=be, data_id="abc123", fill_value=-1)),
    "reindex_multi": (["year", "month"], None, lambda ops, be: ops.reindex(
        "t", "s", [(2012, 1), (2014, 5)], backend=be, data_id="abc123", fill_value=0)),
    "reindex_ffill": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2, 4], backend=be, data_id="abc123", method="ffill")),
    "reindex_ffill_limit": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2, 4], backend=be, data_id="abc123", method="ffill", limit=1)),
    "reindex_bfill": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2, 4], backend=be, data_id="abc123", method="bfill")),
    "reindex_nearest": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [2, 9], backend=be, data_id="abc123", method="nearest")),
    "reindex_no_index": ([], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2], backend=be, data_id="abc123")),
    "reindex_bad_method": (["month"], None, lambda ops, be: ops.reindex(
        "t", "s", [1, 2], backend=be, data_id="abc123", method="nope")),
    "reindex_like": (["month"], OTHERS, lambda ops, be: ops.reindex_like(
        "t", "s", "def456", backend=be, data_id="abc123", fill_value=-1)),
}


BACKENDS = {
    "duckdb": (RecordingDuckAdapter, "duckdb"),
    "postgres": (RecordingPostgresAdapter, "postgres"),
    "clickhouse": (RecordingClickHouseAdapter, "clickhouse"),
}


def _capture():
    snapshot = {}
    for name, (preset, others, scenario) in SCENARIOS.items():
        snapshot[name] = {}
        for backend_name, (adapter_cls, style) in BACKENDS.items():
            ops = make_index_ops(adapter_cls())
            backend = _FakeBackend(style, preset=preset, others=others)
            asyncio.run(scenario(ops, backend))
            snapshot[name][backend_name] = {
                "adapter": ops.db.calls,
                "backend": backend.calls,
            }
    return snapshot


def test_index_sql_fingerprint_unchanged():
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


def test_index_ops_is_correct_class():
    assert DataIndexOps.__name__ == "DataIndexOps"
