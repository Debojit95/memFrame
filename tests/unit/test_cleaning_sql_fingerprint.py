"""SQL-fingerprint regression net for the cleaning ops refactor.

Records every SQL string each backend ops class sends through its stub
adapter for a fixed set of scenarios and compares against a committed
snapshot (tests/fixtures/cleaning_sql_fingerprint.json). This proves the
per-backend SQL is byte-identical across refactors without needing live
Postgres/ClickHouse servers.

Regenerate the snapshot with:  MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_cleaning_sql_fingerprint.py
"""

import asyncio
import importlib
import json
import os
from datetime import datetime, timezone

import pytest

from memframe.core.analytix.cleaning.base import DataCleaningOps
from memframe.core.analytix.cleaning.clickhouse import ClickHouseCleaningOps
from memframe.core.analytix.cleaning.duckdb import DuckDBCleaningOps
from memframe.core.analytix.cleaning.postgres import PostgresCleaningOps

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "cleaning_sql_fingerprint.json"
)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # kills the __op_<timestamp> table-name suffix
        return datetime(2024, 1, 1, tzinfo=timezone.utc)


class RecordingAdapter:
    """Stub adapter: records every SQL statement, returns canned results."""

    def __init__(self):
        self.calls = []

    def _record(self, kind, sql, args):
        # collapse ALL whitespace: formatting-only diffs (e.g. a multi-line
        # NULLIF(...) rewritten single-line) are not semantic changes
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
        return {"num": "INTEGER", "cat": "VARCHAR", "ts": "TIMESTAMP", "g": "VARCHAR"}

    async def table_exists(self, table, schema=None):
        return False


SCENARIOS = {
    "numeric_fillna_constant": lambda ops: ops.numeric_fillna("t", "s", "num", value=5, mode="constant"),
    "numeric_fillna_mean": lambda ops: ops.numeric_fillna("t", "s", "num", mode="mean"),
    "numeric_fillna_ffill": lambda ops: ops.numeric_fillna("t", "s", "num", mode="ffill"),
    "numeric_fillna_median": lambda ops: ops.numeric_fillna("t", "s", "num", mode="median"),
    "numeric_enforce_range": lambda ops: ops.numeric_enforce_range("t", "s", "num", 2, 8),
    "numeric_drop_outliers_zscore": lambda ops: ops.numeric_drop_outliers_zscore("t", "s", "num", 2.5),
    "numeric_convert_text": lambda ops: ops.numeric_convert_text("t", "s", "num"),
    "categorical_fillna_constant": lambda ops: ops.categorical_fillna("t", "s", "cat", mode="constant", value="x"),
    "categorical_fillna_mode": lambda ops: ops.categorical_fillna("t", "s", "cat", mode="mode"),
    "categorical_fillna_map": lambda ops: ops.categorical_fillna("t", "s", "cat", mode="map", mapping={"a": "b"}),
    "categorical_fillna_ffill": lambda ops: ops.categorical_fillna("t", "s", "cat", mode="ffill"),
    "categorical_map_values": lambda ops: ops.categorical_map_values("t", "s", "cat", {"a": "b"}),
    "categorical_filter_invalid": lambda ops: ops.categorical_filter_invalid("t", "s", "cat", ["a", "b"]),
    "categorical_compress_rare": lambda ops: ops.categorical_compress_rare("t", "s", "cat", 5, "other"),
    "datetime_fillna_constant": lambda ops: ops.datetime_fillna("t", "s", "ts", mode="constant", value="2024-01-01"),
    "datetime_fillna_mean": lambda ops: ops.datetime_fillna("t", "s", "ts", mode="mean"),
    "datetime_fillna_ffill": lambda ops: ops.datetime_fillna("t", "s", "ts", mode="ffill"),
    "datetime_fillna_now": lambda ops: ops.datetime_fillna("t", "s", "ts", mode="now"),
    "datetime_fix_invalid": lambda ops: ops.datetime_fix_invalid("t", "s", "ts"),
    "datetime_remove_out_of_range": lambda ops: ops.datetime_remove_out_of_range("t", "s", "ts", "2020-01-01", "2030-01-01"),
    "numeric_fillna_groupby_mean": lambda ops: ops.numeric_fillna_groupby("t", "s", "num", group_cols=["g"], mode="mean"),
    "numeric_fillna_groupby_ungrouped": lambda ops: ops.numeric_fillna_groupby("t", "s", "num", mode="mean"),
    "numeric_fillna_groupby_ffill": lambda ops: ops.numeric_fillna_groupby("t", "s", "num", group_cols=["g"], mode="ffill"),
    "categorical_fillna_groupby_mode": lambda ops: ops.categorical_fillna_groupby("t", "s", "cat", group_cols=["g"], mode="mode"),
    "categorical_fillna_groupby_ffill": lambda ops: ops.categorical_fillna_groupby("t", "s", "cat", group_cols=["g"], mode="ffill"),
    "datetime_fillna_groupby_mean": lambda ops: ops.datetime_fillna_groupby("t", "s", "ts", group_cols=["g"], mode="mean"),
    "datetime_fillna_groupby_ffill": lambda ops: ops.datetime_fillna_groupby("t", "s", "ts", group_cols=["g"], mode="ffill"),
}

BACKENDS = {
    "duckdb": DuckDBCleaningOps,
    "postgres": PostgresCleaningOps,
    "clickhouse": ClickHouseCleaningOps,
}


def _capture():
    base_mod = importlib.import_module("memframe.core.analytix.cleaning.base")
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


def test_cleaning_sql_fingerprint_unchanged():
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


def test_all_backends_are_data_cleaning_ops():
    for cls in BACKENDS.values():
        assert issubclass(cls, DataCleaningOps)
