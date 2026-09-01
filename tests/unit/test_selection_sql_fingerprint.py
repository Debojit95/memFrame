"""SQL-fingerprint regression net for the selection ops refactor.

Same pattern as the cleaning/arithmetic fingerprint harnesses. The stub
adapter returns backend-appropriate metadata row shapes (PRAGMA tuples for
DuckDB, dict rows for Postgres/ClickHouse) because _row_get consumes them.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_selection_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from memframe.core.analytix.selection.base import DataSelectionOps
from memframe.core.analytix.selection.clickhouse import ClickHouseSelectionOps
from memframe.core.analytix.selection.duckdb import DuckDBSelectionOps
from memframe.core.analytix.selection.postgres import PostgresSelectionOps

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "selection_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


class RecordingAdapter:
    """Stub adapter; fetch returns per-flavor metadata rows."""

    def __init__(self, flavor: str):
        self.flavor = flavor
        self.calls = []

    def _record(self, kind, sql, args):
        self.calls.append([kind, "".join(sql.split()), [str(a) for a in args]])

    async def execute(self, sql, *args):
        self._record("exec", sql, args)

    async def fetch(self, sql, *args):
        self._record("fetch", sql, args)
        return self._rows(sql)

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        return 0

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)
        return None

    def _rows(self, sql):
        # PRAGMA (duckdb) -> tuples; information_schema/system.columns -> dicts
        if self.flavor == "duckdb":
            return [("0", "num", "INTEGER"), ("1", "txt", "VARCHAR"), ("2", "ts", "TIMESTAMP")]
        if self.flavor == "postgres":
            return [
                {"column_name": "num", "data_type": "integer"},
                {"column_name": "txt", "data_type": "character varying"},
                {"column_name": "ts", "data_type": "timestamp without time zone"},
            ]
        return [
            {"name": "num", "type": "Int32"},
            {"name": "txt", "type": "String"},
            {"name": "ts", "type": "DateTime"},
        ]

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'

    def placeholder(self, index=1):
        return f"${index}" if True else "?"

    async def get_column_types(self, table, schema=None):
        return {"num": "INTEGER", "txt": "VARCHAR", "ts": "TIMESTAMP"}

    async def table_exists(self, table, schema=None):
        return False


STUB_BACKEND = SimpleNamespace(
    transient_schema="memframe_transient",
    transient_registry_table="memframe_transient.memframe_transient_registry",
    placeholder=lambda i=1: "$1",
    fetchval=None,  # replaced with an async fn below
)


async def _stub_fetchval(sql, *args):
    return 0


STUB_BACKEND.fetchval = _stub_fetchval


def _scenarios():
    s = {}

    def add(name, fn):
        s[name] = fn

    add("at_in_bounds", lambda ops: ops.at("t", "s", 1, "num"))
    add("at_out_of_bounds", lambda ops: ops.at("t", "s", 99, "num"))
    add("iat", lambda ops: ops.iat("t", "s", 0, "num", "ts"))
    add("get", lambda ops: ops.get("t", "s", "num"))
    add("get_star", lambda ops: ops.get("t", "s", "*"))
    add("asof_str", lambda ops: ops.asof("t", "s", "2024-01-01", "ts", "num"))
    add("asof_datetime", lambda ops: ops.asof("t", "s", "2024-01-01 10:00:00", "ts", "num"))
    add("asof_on_txt", lambda ops: ops.asof("t", "s", "abc", "txt", "num"))
    add("select_dtypes_include", lambda ops: ops.select_dtypes("t", "s", include=["numeric"]))
    add("select_dtypes_exclude", lambda ops: ops.select_dtypes("t", "s", exclude=["categorical"]))
    add("select_dtypes_list", lambda ops: ops.select_dtypes("t", "s", include=["numeric", "timestamp"]))
    add("select_dtypes_bad", lambda ops: ops.select_dtypes("t", "s", include=["bogus"]))
    add("iloc_slice", lambda ops: ops.iloc("t", "s", row_indexer=slice(0, 5)))
    add("iloc_int", lambda ops: ops.iloc("t", "s", row_indexer=2))
    add("iloc_list", lambda ops: ops.iloc("t", "s", row_indexer=[0, 2]))
    add("iloc_bool", lambda ops: ops.iloc("t", "s", row_indexer=[True, False, True]))
    add("iloc_out_of_range", lambda ops: ops.iloc("t", "s", row_indexer=999))
    add("iloc_labels", lambda ops: ops.iloc("t", "s", row_indexer=["a", "b"], index_column="txt"))
    add("iloc_where", lambda ops: ops.iloc("t", "s", row_indexer="num > 10"))
    add("iloc_multistmt", lambda ops: ops.iloc("t", "s", row_indexer="num > 10; DROP TABLE x"))
    add("iloc_tuple", lambda ops: ops.iloc("t", "s", row_indexer=(slice(0, 4), slice(0, 2))))
    add("iloc_cols_positions", lambda ops: ops.iloc("t", "s", row_indexer=slice(0, 3), col_indexer=[0, 1]))
    add("iloc_transient", lambda ops: ops.iloc(
        "t", "s", row_indexer="num > 10", backend=STUB_BACKEND, data_id="d1"))
    add("select_dtypes_transient", lambda ops: ops.select_dtypes(
        "t", "s", include=["numeric"], backend=STUB_BACKEND, data_id="d1"))
    return s


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": DuckDBSelectionOps,
    "postgres": PostgresSelectionOps,
    "clickhouse": ClickHouseSelectionOps,
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.selection.base")
    real_datetime = base_mod.datetime if hasattr(base_mod, "datetime") else datetime
    base_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, cls in BACKENDS.items():
                adapter = RecordingAdapter(backend_name)
                ops = cls(adapter)
                asyncio.run(scenario(ops))
                snapshot[name][backend_name] = adapter.calls
        return snapshot
    finally:
        base_mod.datetime = real_datetime


def test_selection_sql_fingerprint_unchanged():
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


def test_all_backends_are_selection_ops():
    for cls in BACKENDS.values():
        assert issubclass(cls, DataSelectionOps)
