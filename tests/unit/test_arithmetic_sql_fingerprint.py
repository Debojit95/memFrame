"""SQL-fingerprint regression net for the arithmetic ops refactor.

Same pattern as test_cleaning_sql_fingerprint.py: record every SQL string
each backend ops class emits for a fixed scenario set and compare against a
snapshot captured from the pre-refactor code.

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_arithmetic_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.arithmetic.base import ArithmeticOps
from memframe.core.analytix.arithmetic.clickhouse import ClickHouseArithmeticOps
from memframe.core.analytix.arithmetic.duckdb import DuckDBArithmeticOps
from memframe.core.analytix.arithmetic.postgres import PostgresArithmeticOps

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "arithmetic_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


class RecordingAdapter:
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

    def quote_identifier(self, name):
        return '"' + name.replace('"', '""') + '"'

    def placeholder(self, index=1):
        return f"?{index}" if index > 1 else "?"

    async def get_column_types(self, table, schema=None):
        # "txt" exercises the textual-dtype numeric cast hook
        return {"num": "DOUBLE PRECISION", "txt": "VARCHAR", "n2": "INTEGER"}

    async def table_exists(self, table, schema=None):
        return False


def _scenarios():
    s = {}
    binary_ops = {
        "add": "add", "subtract": "sub", "multiply": "mul",
        "divide": "div", "modulo": "mod", "power": "pow",
        "atan2": "atan2", "weighted_average": "wavg",
    }
    for name, kw in binary_ops.items():
        s[f"{name}_col_col"] = lambda ops, n=name, k=kw: ops.__getattribute__(n)("t", "s", "num", "n2")
        s[f"{name}_col_scalar"] = lambda ops, n=name, k=kw: ops.__getattribute__(n)("t", "s", "num", 2.5)
        s[f"{name}_scalar_col"] = lambda ops, n=name, k=kw: ops.__getattribute__(n)("t", "s", 3, "num")
    unary = ["absolute", "negate", "ceil", "floor", "exp", "log", "log10",
             "sqrt", "sin", "cos", "tan", "asin", "acos", "atan"]
    for name in unary:
        s[f"{name}"] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "num")
        s[f"{name}_textcast"] = lambda ops, n=name: ops.__getattribute__(n)("t", "s", "txt")
    s["percentage_change"] = lambda ops: ops.percentage_change("t", "s", "num", "n2")
    s["percentage_change_textcast"] = lambda ops: ops.percentage_change("t", "s", "txt", "n2")
    s["round"] = lambda ops: ops.round("t", "s", "num", 2)
    s["round_textcast"] = lambda ops: ops.round("t", "s", "txt", 2)
    s["truncate"] = lambda ops: ops.truncate("t", "s", "num", 2)
    s["truncate_textcast"] = lambda ops: ops.truncate("t", "s", "txt", 2)
    s["normalize_range"] = lambda ops: ops.normalize_range("t", "s", "num")
    s["normalize_range_textcast"] = lambda ops: ops.normalize_range("t", "s", "txt")
    return s


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": DuckDBArithmeticOps,
    "postgres": PostgresArithmeticOps,
    "clickhouse": ClickHouseArithmeticOps,
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.arithmetic.base")
    real_datetime = base_mod.datetime
    base_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, cls in BACKENDS.items():
                ops = cls(RecordingAdapter())
                asyncio.run(scenario(ops))
                snapshot[name][backend_name] = ops.db.calls
        return snapshot
    finally:
        base_mod.datetime = real_datetime


def test_arithmetic_sql_fingerprint_unchanged():
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


def test_all_backends_are_arithmetic_ops():
    for cls in BACKENDS.values():
        assert issubclass(cls, ArithmeticOps)
