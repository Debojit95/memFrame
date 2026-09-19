"""SQL-fingerprint regression net for window ops.

Same pattern as test_cumulative_sql_fingerprint.py: record every SQL string
the window ops class emits for a fixed scenario set and compare against a
snapshot. Backend personas are RecordingAdapter mixins over the real adapter
classes (real quoting and placeholder styles, stubbed I/O).

Regenerate with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_window_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.window import WindowOps
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "window_sql_fingerprint.json"
)

DATA_ID = "abc123"


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

    async def insert_rows(self, table_name, rows, columns):
        self._record("insert_rows", table_name, list(columns))

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


class _FakeBackend:
    """Minimal stand-in for DatabaseBackend (registry lookup + placeholders)."""

    def __init__(self, rec):
        self._rec = rec

    @property
    def transient_registry_table(self):
        return "memframe_csv_registry.memframe_transient_registry"

    def placeholder(self, index=1):
        return self._rec.placeholder(index)

    async def fetchval(self, sql, *args):
        return await self._rec.fetchval(sql, *args)

    async def fetch_val(self, sql, *args):
        return await self._rec.fetchval(sql, *args)


def _scenarios():
    s = {}

    for name, agg in [("sum", "SUM"), ("std", "STDDEV_POP"), ("var", "VAR_SAMP")]:
        s[f"rolling_{name}_ordered"] = lambda o, b, a=agg: o._rolling_agg(
            "t", "s", "num", order_by="g", window=3, agg=a, backend=b, data_id=DATA_ID
        )
    s["rolling_sum_unordered"] = lambda o, b: o._rolling_agg(
        "t", "s", "num", window=3, agg="SUM", backend=b, data_id=DATA_ID
    )
    s["rolling_multi_agg"] = lambda o, b: o._rolling_agg(
        "t", "s", "num", order_by="g", window=3, agg=["SUM", "AVG"], backend=b, data_id=DATA_ID
    )
    s["rolling_first"] = lambda o, b: o.rolling_first(
        "t", "s", "num", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_last"] = lambda o, b: o.rolling_last(
        "t", "s", "num", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_quantile"] = lambda o, b: o.rolling_quantile(
        "t", "s", "num", order_by="g", window=3, q=0.5, backend=b, data_id=DATA_ID
    )
    s["rolling_sem"] = lambda o, b: o.rolling_sem(
        "t", "s", "num", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_nunique"] = lambda o, b: o.rolling_nunique(
        "t", "s", "num", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_rank"] = lambda o, b: o.rolling_rank(
        "t", "s", "num", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_dt_mean_unordered"] = lambda o, b: o.rolling_mean_datetime(
        "t", "s", "ts", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_dt_median_ordered"] = lambda o, b: o.rolling_median_datetime(
        "t", "s", "ts", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["rolling_dt_mode_ordered"] = lambda o, b: o.rolling_mode_datetime(
        "t", "s", "ts", order_by="g", window=3, backend=b, data_id=DATA_ID
    )
    s["expanding_sum_ordered"] = lambda o, b: o.expanding_sum(
        "t", "s", "num", order_by="g", min_periods=2, backend=b, data_id=DATA_ID
    )
    s["expanding_mean_unordered"] = lambda o, b: o.expanding_mean(
        "t", "s", "num", min_periods=1, backend=b, data_id=DATA_ID
    )
    s["expanding_std_ordered"] = lambda o, b: o.expanding_std(
        "t", "s", "num", order_by="g", min_periods=2, backend=b, data_id=DATA_ID
    )
    s["expanding_quantile"] = lambda o, b: o.expanding_quantile(
        "t", "s", "num", order_by="g", q=0.5, min_periods=2, backend=b, data_id=DATA_ID
    )
    s["expanding_sem"] = lambda o, b: o.expanding_sem(
        "t", "s", "num", order_by="g", min_periods=2, backend=b, data_id=DATA_ID
    )
    s["ewm_mean_ordered"] = lambda o, b: o.ewm(
        "t", "s", "num", order_by="g", span=2, agg="mean", backend=b, data_id=DATA_ID
    )
    s["ewm_multi_unordered"] = lambda o, b: o.ewm(
        "t", "s", "num", span=2, agg=["mean", "std"], backend=b, data_id=DATA_ID
    )
    return s


SCENARIOS = _scenarios()

BACKENDS = {
    "duckdb": _DuckDBPersona,
    "postgres": _PostgresPersona,
    "clickhouse": _ClickHousePersona,
}


def _capture():
    window_mod = importlib.import_module("memframe.core.analytix.window")
    real_datetime = window_mod.datetime
    window_mod.datetime = _FrozenDatetime
    try:
        snapshot = {}
        for name, scenario in SCENARIOS.items():
            snapshot[name] = {}
            for backend_name, persona_cls in BACKENDS.items():
                rec = persona_cls()
                ops = WindowOps(rec)
                asyncio.run(scenario(ops, _FakeBackend(rec)))
                snapshot[name][backend_name] = rec.calls
        return snapshot
    finally:
        window_mod.datetime = real_datetime


def test_window_sql_fingerprint_unchanged():
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


def test_window_personas_match_real_backends():
    assert isinstance(_DuckDBPersona(), DuckDBAdapter)
    assert isinstance(_PostgresPersona(), PostgresAdapter)
    assert isinstance(_ClickHousePersona(), ClickHouseAdapter)
