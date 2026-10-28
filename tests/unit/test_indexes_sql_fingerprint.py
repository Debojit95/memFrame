"""SQL-fingerprint net for database-native index operations.

Regenerate with:
    MEMFRAME_REGEN_FINGERPRINT=1 pytest tests/unit/test_indexes_sql_fingerprint.py
"""

import asyncio
import json
import os

import pytest

from memframe.core.analytix.index import (
    ClickHouseIndexOps,
    DuckDBIndexOps,
    PostgresIndexOps,
)

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "indexes_sql_fingerprint.json"
)

CH_DDL = (
    "CREATE TABLE `db`.`t` (`id` Nullable(Int64), `name` Nullable(String)) "
    "ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192"
)


class _FakeAdapter:
    def __init__(self, handler, columns, quote=None, placeholder="?"):
        self.calls = []
        self._handler = handler
        self._columns = columns
        self._quote = quote or (lambda name: f'"{name}"')
        self._placeholder = placeholder

    def _record(self, kind, sql, args):
        self.calls.append([kind, " ".join(sql.split()), [str(a) for a in args]])

    async def execute(self, sql, *args):
        self._record("execute", sql, args)

    async def fetch(self, sql, *args):
        self._record("fetch", sql, args)
        return self._handler(sql, *args)

    async def fetchval(self, sql, *args):
        self._record("fetchval", sql, args)
        upper = " ".join(sql.split()).upper()
        if "COUNT()" in upper:
            return 3
        if "SYSTEM.PARTS" in upper:
            return 1024
        return None

    async def fetchrow(self, sql, *args):
        self._record("fetchrow", sql, args)
        return None

    async def get_column_types(self, table, schema=None):
        return dict(self._columns)

    async def table_exists(self, table, schema=None):
        return False

    def placeholder(self, index=1):
        return self._placeholder

    def quote_identifier(self, name):
        return self._quote(name)


def _duck(rows=None):
    rows = rows or []

    def handler(sql, *args):
        upper = " ".join(sql.split()).upper()
        if "DUCKDB_INDEXES" in upper:
            return list(rows)
        if "DUCKDB_CONSTRAINTS" in upper:
            return []
        return []

    return handler


def _pg(rows=None):
    rows = rows or []

    def handler(sql, *args):
        if "pg_index" in sql:
            return list(rows)
        return []

    return handler


def _ch(skipping=None, ddl=CH_DDL, engine="MergeTree"):
    skipping = skipping or []

    def handler(sql, *args):
        upper = " ".join(sql.split()).upper()
        if "CREATE_TABLE_QUERY" in upper:
            return [{"create_table_query": ddl}]
        if "SYSTEM.TABLES" in upper:
            return [
                {
                    "engine": engine,
                    "sorting_key": "tuple()",
                    "primary_key": "tuple()",
                    "partition_key": "",
                }
            ]
        if "SYSTEM.DATA_SKIPPING_INDICES" in upper:
            return list(skipping)
        return []

    return handler


DUCK_ROW = {
    "schema_name": "s",
    "index_name": "idx",
    "table_name": "t",
    "is_unique": False,
    "sql": "CREATE INDEX idx ON s.t(id)",
}
PG_ROW = {
    "index_name": "idx",
    "table_name": "t",
    "is_unique": False,
    "is_valid": True,
    "method": "btree",
    "definition": "CREATE INDEX idx ON s.t USING btree (id)",
    "constraint_type": None,
}
CH_ROW = {
    "name": "idx",
    "type_full": "minmax",
    "expr": "id",
    "granularity": 1,
}


def _capture_with_calls():
    snapshot = {}
    for name, (adapter, run) in SCENARIOS.items():
        asyncio.run(run(adapter))
        snapshot[name] = adapter.calls
    return snapshot


def _build_scenarios():
    """Return scenario name -> (adapter, coroutine factory)."""
    scenarios = {}

    scenarios["duckdb_create"] = (
        _FakeAdapter(_duck(), {"id": "BIGINT", "name": "TEXT"}),
        lambda a: DuckDBIndexOps(a).create_index(
            "t", "s", columns=["id"], name="idx"
        ),
    )
    scenarios["duckdb_create_unique"] = (
        _FakeAdapter(_duck(), {"id": "BIGINT", "name": "TEXT"}),
        lambda a: DuckDBIndexOps(a).create_index(
            "t", "s", columns=["id", "name"], name="idx", unique=True
        ),
    )
    scenarios["duckdb_drop"] = (
        _FakeAdapter(_duck([DUCK_ROW]), {"id": "BIGINT"}),
        lambda a: DuckDBIndexOps(a).drop_index("t", "s", "idx"),
    )
    scenarios["duckdb_list"] = (
        _FakeAdapter(_duck([DUCK_ROW]), {"id": "BIGINT"}),
        lambda a: DuckDBIndexOps(a).list_indexes("t", "s"),
    )

    scenarios["postgres_create"] = (
        _FakeAdapter(_pg(), {"id": "BIGINT", "name": "TEXT"}, placeholder="$1"),
        lambda a: PostgresIndexOps(a).create_index(
            "t", "s", columns=["id"], name="idx", method="btree"
        ),
    )
    scenarios["postgres_create_concurrent"] = (
        _FakeAdapter(_pg(), {"id": "BIGINT", "name": "TEXT"}, placeholder="$1"),
        lambda a: PostgresIndexOps(a).create_index(
            "t", "s", columns=["id"], name="idx", concurrently=True
        ),
    )
    scenarios["postgres_create_partial_include"] = (
        _FakeAdapter(_pg(), {"id": "BIGINT", "name": "TEXT"}, placeholder="$1"),
        lambda a: PostgresIndexOps(a).create_index(
            "t", "s", columns=["id"], name="idx",
            where="id > 1", include=["name"],
        ),
    )
    scenarios["postgres_drop"] = (
        _FakeAdapter(_pg([PG_ROW]), {"id": "BIGINT"}, placeholder="$1"),
        lambda a: PostgresIndexOps(a).drop_index("t", "s", "idx"),
    )
    scenarios["postgres_list"] = (
        _FakeAdapter(_pg([PG_ROW]), {"id": "BIGINT"}, placeholder="$1"),
        lambda a: PostgresIndexOps(a).list_indexes("t", "s"),
    )

    CH_COLS = {"id": "Nullable(Int64)", "name": "Nullable(String)"}

    def ch_quote(name):
        return f"`{name}`"
    scenarios["clickhouse_create_minmax"] = (
        _FakeAdapter(_ch(), CH_COLS, quote=ch_quote),
        lambda a: ClickHouseIndexOps(a).create_index(
            "t", "db", columns=["id"], name="idx", index_type="minmax"
        ),
    )
    scenarios["clickhouse_create_bloom"] = (
        _FakeAdapter(_ch(), CH_COLS, quote=ch_quote),
        lambda a: ClickHouseIndexOps(a).create_index(
            "t", "db", columns=["name"], name="idx_name",
            index_type="bloom_filter", granularity=2,
        ),
    )
    scenarios["clickhouse_drop"] = (
        _FakeAdapter(_ch([CH_ROW]), CH_COLS, quote=ch_quote),
        lambda a: ClickHouseIndexOps(a).drop_index("t", "db", "idx"),
    )
    scenarios["clickhouse_list"] = (
        _FakeAdapter(_ch([CH_ROW]), CH_COLS, quote=ch_quote),
        lambda a: ClickHouseIndexOps(a).list_indexes("t", "db"),
    )
    scenarios["clickhouse_order_by_dry_run"] = (
        _FakeAdapter(_ch(), CH_COLS, quote=ch_quote),
        lambda a: ClickHouseIndexOps(a).set_order_by(
            "t", "db", columns=["id"], dry_run=True
        ),
    )
    return scenarios


SCENARIOS = _build_scenarios()


def test_indexes_sql_fingerprint_unchanged():
    current = _capture_with_calls()
    if os.environ.get("MEMFRAME_REGEN_FINGERPRINT"):
        os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
        with open(FIXTURE, "w") as fh:
            json.dump(current, fh, indent=1, sort_keys=True)
        pytest.skip("fingerprint regenerated")

    assert os.path.exists(FIXTURE), "run with MEMFRAME_REGEN_FINGERPRINT=1 first"
    with open(FIXTURE) as fh:
        expected = json.load(fh)
    assert set(current) == set(expected), "scenario set changed"
    diffs = [name for name in expected if current[name] != expected[name]]
    assert not diffs, f"SQL changed for: {diffs}"
