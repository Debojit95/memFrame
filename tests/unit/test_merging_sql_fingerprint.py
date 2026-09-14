"""SQL-fingerprint regression net for merge/join/concat ops.

Records every SQL string ``DataMergeOps`` emits through a recording adapter for
a fixed scenario set and compares against a committed snapshot.

Unlike the selection fingerprint, the recording adapters must subclass the real
backend adapters: ``DataMergeOps`` dispatches on ``isinstance(self.db, ...)``,
so a plain recording object falls through to the unsupported-backend error.

Regenerate with:
    MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_merging_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.merging import DataMergeOps
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "merging_sql_fingerprint.json"
)

# Per-table column-type maps: the join/merge code reads these to detect overlap
# (suffix handling) and timestamp/date pairs (auto-cast).
TABLE_COLUMNS = {
    "left_t": {
        "id": "INTEGER",
        "val": "DOUBLE PRECISION",
        "grp": "VARCHAR",
        "ts": "TIMESTAMP",
    },
    "right_t": {
        "id": "INTEGER",
        "val": "DOUBLE PRECISION",
        "extra": "VARCHAR",
        "ts": "TIMESTAMP",
    },
    "lts": {"k": "TIMESTAMP", "v": "INTEGER"},
    "rdate": {"k": "DATE", "v": "INTEGER"},
    "ldate": {"k": "DATE", "v": "INTEGER"},
    "rts": {"k": "TIMESTAMP", "v": "INTEGER"},
    "lch": {"k": "DateTime", "v": "INTEGER"},
    "rch": {"k": "Date", "v": "INTEGER"},
    "lchd": {"k": "Date", "v": "INTEGER"},
    "rcht": {"k": "DateTime", "v": "INTEGER"},
    "c1": {"a": "INTEGER", "b": "VARCHAR"},
    "c2": {"b": "VARCHAR", "c": "DOUBLE PRECISION"},
    "d1t": {"a": "INTEGER"},
    "d2t": {"b": "VARCHAR"},
    "empty_t": {},
}


class _StubBackend:
    transient_schema = "memframe_transient"
    transient_registry_table = "memframe_transient.memframe_transient_registry"

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
        return []

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        return 0

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)

    async def get_column_types(self, table, schema=None):
        return dict(TABLE_COLUMNS.get(table, {}))

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
        self._ch_pool = None

    def placeholder(self, index=1):
        return "?"

    def quote_identifier(self, name):
        return f"`{name}`"


def _backend_for(adapter):
    return _StubBackend(adapter.placeholder)


def _scenarios():
    s = {}

    def merge(left, right=None, **kw):
        return lambda ops, be: ops.merge(
            left, right or "right_t", "s", backend=be, data_id="d1", **kw
        )

    def join(left, right=None, **kw):
        return lambda ops, be: ops.join(
            left, right or "right_t", "s", backend=be, data_id="d1", **kw
        )

    def concat(tables, **kw):
        return lambda ops, be: ops.concat(
            tables, "s", backend=be, data_id="d1", **kw
        )

    # ── merge ────────────────────────────────────────────────────────
    s["merge_on_inner"] = merge("left_t", how="inner", on="id")
    s["merge_on_left"] = merge("left_t", how="left", on="id")
    s["merge_on_right"] = merge("left_t", how="right", on="id")
    s["merge_on_outer"] = merge("left_t", how="outer", on="id")
    s["merge_cross"] = merge("left_t", how="cross", on="id")
    s["merge_split_cols"] = merge("left_t", left_on="id", right_on="extra")
    s["merge_default_suffix"] = merge("left_t", how="inner", on="id")
    s["merge_custom_suffix"] = merge(
        "left_t", how="inner", on="id", suffixes=("_l", "_r")
    )
    s["merge_left_anti"] = merge("left_t", how="left_anti", on="id")
    s["merge_right_anti"] = merge("left_t", how="right_anti", on="id")
    s["merge_autocast_ts_date"] = merge("lts", right="rdate", on="k")
    s["merge_autocast_date_ts"] = merge("ldate", right="rts", on="k")
    s["merge_clickhouse_autocast"] = merge("lch", right="rch", on="k")
    s["merge_clickhouse_autocast_rev"] = merge("lchd", right="rcht", on="k")
    s["merge_bad_how"] = merge("left_t", how="nonsense", on="id")
    s["merge_missing_keys"] = merge("left_t")
    s["merge_mismatch_keys"] = merge(
        "left_t", left_on=["id"], right_on=["id", "val"]
    )
    s["merge_bad_chunk"] = merge("left_t", on="id", chunk_size=0)
    s["merge_missing_data_id"] = lambda ops, be: ops.merge(
        "left_t", "right_t", "s", on="id"
    )
    s["merge_no_left_cols"] = merge("empty_t", on="id")
    s["merge_streaming"] = merge("left_t", on="id", chunk_size=2)

    # ── join ─────────────────────────────────────────────────────────
    s["join_on_none_common"] = join("c1", right="c2")
    s["join_on_str"] = join("left_t", on="id")
    s["join_default_right_suffix"] = join("left_t", on="id")
    s["join_custom_suffix"] = join("left_t", on="id", lsuffix="_l", rsuffix="_r")
    s["join_bad_how"] = join("left_t", how="nonsense", on="id")
    s["join_no_common"] = join("d1t", right="d2t")
    s["join_left_anti"] = join("left_t", how="left_anti", on="id")
    s["join_right_anti"] = join("left_t", how="right_anti", on="id")
    s["join_streaming"] = join("left_t", on="id", chunk_size=2)

    # ── concat ───────────────────────────────────────────────────────
    s["concat_axis0_outer"] = concat(["c1", "c2"], axis=0, join="outer")
    s["concat_axis0_inner"] = concat(["c1", "c2"], axis=0, join="inner")
    s["concat_axis0_ignore_index"] = concat(
        ["c1", "c2"], axis=0, join="outer", ignore_index=True
    )
    s["concat_axis1_outer"] = concat(["c1", "c2"], axis=1, join="outer")
    s["concat_axis1_inner"] = concat(["c1", "c2"], axis=1, join="inner")
    s["concat_too_few"] = concat(["c1"])
    s["concat_bad_axis"] = concat(["c1", "c2"], axis=2)
    s["concat_bad_join"] = concat(["c1", "c2"], join="left")
    s["concat_no_cols"] = concat(["d1t", "d2t"], axis=0, join="inner")
    s["concat_streaming"] = concat(["c1", "c2"], axis=0, chunk_size=2)

    return s


SCENARIOS = _scenarios()

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
            ops = DataMergeOps(adapter)
            asyncio.run(scenario(ops, _backend_for(adapter)))
            snapshot[name][backend_name] = adapter.calls
    return snapshot


def test_merging_sql_fingerprint_unchanged():
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


def test_all_backends_are_merge_ops():
    for adapter_cls in BACKENDS.values():
        assert issubclass(adapter_cls, (DuckDBAdapter, PostgresAdapter, ClickHouseAdapter))
