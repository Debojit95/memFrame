"""SQL-fingerprint regression net for sorting ops.

Records every SQL string the DataSortingOps class sends through its stub
adapter for a fixed scenario set and compares against a committed snapshot.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_sorting_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.sorting import DataSortingOps, make_sorting_ops
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "sorting_sql_fingerprint.json"
)


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
        return {"name": "VARCHAR", "score": "DOUBLE", "val": "INTEGER"}

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
    "sort_single_asc_last": lambda ops: ops.sort_values(
        "t", "s", by="score", backend=object(), data_id="abc123"
    ),
    "sort_single_desc_first": lambda ops: ops.sort_values(
        "t", "s", by="score", ascending=False, na_position="first", backend=object(), data_id="abc123"
    ),
    "sort_multi_mixed": lambda ops: ops.sort_values(
        "t", "s", by=["name", "score"], ascending=[True, False], backend=object(), data_id="abc123"
    ),
    "sort_columns_subset": lambda ops: ops.sort_values(
        "t", "s", by="score", columns=["name"], backend=object(), data_id="abc123"
    ),
    "sort_columns_str": lambda ops: ops.sort_values(
        "t", "s", by="score", columns="name", backend=object(), data_id="abc123"
    ),
    "sort_ascending_scalar_broadcast": lambda ops: ops.sort_values(
        "t", "s", by=["name", "score"], ascending=True, backend=object(), data_id="abc123"
    ),
    "sort_collect_all": lambda ops: ops.sort_values(
        "t", "s", by="score", columns="*", backend=object(), data_id="abc123"
    ),
    "sort_bad_ascending_len": lambda ops: ops.sort_values(
        "t", "s", by=["name", "score"], ascending=[True], backend=object(), data_id="abc123"
    ),
    "sort_bad_na_position": lambda ops: ops.sort_values(
        "t", "s", by="score", na_position="middle", backend=object(), data_id="abc123"
    ),
}


BACKENDS = {
    "duckdb": RecordingDuckAdapter,
    "postgres": RecordingPostgresAdapter,
    "clickhouse": RecordingClickHouseAdapter,
}


def _make_ops(adapter_cls):
    # ponytail: factory returns the backend-specific subclass; SQL must match legacy snapshot.
    return make_sorting_ops(adapter_cls())


def _capture():
    snapshot = {}
    for name, scenario in SCENARIOS.items():
        snapshot[name] = {}
        for backend_name, adapter_cls in BACKENDS.items():
            ops = _make_ops(adapter_cls)
            asyncio.run(scenario(ops))
            snapshot[name][backend_name] = ops.db.calls
    return snapshot


def test_sorting_sql_fingerprint_unchanged():
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


def test_sorting_ops_is_correct_class():
    assert DataSortingOps.__name__ == "DataSortingOps"
