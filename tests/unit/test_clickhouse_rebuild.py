import asyncio

from memframe.core.analytix.index.clickhouse import ClickHouseIndexOps
from memframe.core.analytix.index.clickhouse_ddl import (
    ensure_setting,
    replace_table_name,
    rewrite_order_by,
)


class _CHAdapter:
    def __init__(self, ddl: str, engine: str = "MergeTree", version=None):
        self.ddl = ddl
        self.engine = engine
        self.version = version
        self.executed = []

    async def fetch(self, sql, *args):
        upper = " ".join(sql.split()).upper()
        if "SYSTEM.TABLES" in upper and "CREATE_TABLE_QUERY" in upper:
            return [{"create_table_query": self.ddl}]
        if "SYSTEM.TABLES" in upper:
            return [
                {
                    "engine": self.engine,
                    "sorting_key": "tuple()",
                    "primary_key": "tuple()",
                    "partition_key": "",
                }
            ]
        if "SYSTEM.DATA_SKIPPING_INDICES" in upper:
            return []
        return []

    async def fetchval(self, sql, *args):
        upper = " ".join(sql.split()).upper()
        if "VERSION()" in upper:
            return self.version
        if "COUNT()" in upper:
            return 3
        if "SYSTEM.PARTS" in upper:
            return 1024
        return None

    async def execute(self, sql, *args):
        self.executed.append(" ".join(sql.split()))

    async def get_column_types(self, table, schema=None):
        return {"id": "Nullable(Int64)", "name": "Nullable(String)"}

    def placeholder(self, index=1):
        return "?"

    def quote_identifier(self, name):
        return f"`{name}`"


DDL = (
    "CREATE TABLE `db`.`t` (`id` Nullable(Int64), `name` Nullable(String), "
    "INDEX idx_name name TYPE bloom_filter GRANULARITY 2) "
    "ENGINE = MergeTree PARTITION BY toYYYYMM(id) ORDER BY tuple() "
    "PRIMARY KEY tuple() TTL id + INTERVAL 1 DAY "
    "SETTINGS index_granularity = 8192"
)


def test_rewrite_order_by_and_primary_key():
    rewritten = rewrite_order_by(DDL, "(`id`, `name`)", "(`id`)")

    assert "ORDER BY (`id`, `name`)" in rewritten
    assert "PRIMARY KEY (`id`)" in rewritten
    assert "PARTITION BY toYYYYMM(id)" in rewritten
    assert "TTL id + INTERVAL 1 DAY" in rewritten
    assert "SETTINGS index_granularity = 8192" in rewritten
    assert "INDEX idx_name name TYPE bloom_filter GRANULARITY 2" in rewritten


def test_rewrite_removes_existing_primary_key_when_omitted():
    rewritten = rewrite_order_by(DDL, "(`id`)")

    assert "ORDER BY (`id`)" in rewritten
    assert "PRIMARY KEY" not in rewritten


def test_ensure_setting_appends_or_replaces():
    assert "allow_nullable_key = 1" in ensure_setting(DDL, "allow_nullable_key", "1")
    assert ensure_setting(
        "CREATE TABLE t (a Int) ENGINE = MergeTree ORDER BY a",
        "allow_nullable_key",
        "1",
    ).endswith("SETTINGS allow_nullable_key = 1")
    replaced = ensure_setting(
        "CREATE TABLE t (a Int) ENGINE = MergeTree ORDER BY a "
        "SETTINGS allow_nullable_key = 0",
        "allow_nullable_key",
        "1",
    )
    assert "allow_nullable_key = 1" in replaced
    assert "allow_nullable_key = 0" not in replaced


def test_replace_table_name():
    rewritten = replace_table_name(DDL, "`db`.`tmp`")
    assert rewritten.startswith("CREATE TABLE `db`.`tmp`")


def test_set_order_by_dry_run_does_not_execute():
    adapter = _CHAdapter(DDL)
    response = asyncio.run(
        ClickHouseIndexOps(adapter).set_order_by(
            "t", "db", columns=["id"], dry_run=True
        )
    )

    assert response["is_error"] is False
    assert response["result"]["rebuilt"] is False
    assert "allow_nullable_key = 1" in response["result"]["ddl"]
    assert adapter.executed == []


def test_set_order_by_requires_prefix_primary_key():
    adapter = _CHAdapter(DDL)
    response = asyncio.run(
        ClickHouseIndexOps(adapter).set_order_by(
            "t", "db", columns=["id", "name"], primary_key=["name"],
            dry_run=True,
        )
    )

    assert response["is_error"] is True
    assert "prefix" in response["error_message"]


def test_set_order_by_rejects_non_mergetree():
    adapter = _CHAdapter(DDL, engine="Log")
    response = asyncio.run(
        ClickHouseIndexOps(adapter).set_order_by(
            "t", "db", columns=["id"], dry_run=True
        )
    )

    assert response["is_error"] is True
    assert "MergeTree" in response["error_message"]


def test_clickhouse_unique_index_is_rejected():
    adapter = _CHAdapter(DDL)
    response = asyncio.run(
        ClickHouseIndexOps(adapter).create_index(
            "t", "db", columns=["id"], unique=True
        )
    )

    assert response["is_error"] is True
    assert "unique" in response["error_message"].lower()
    assert adapter.executed == []


def test_clickhouse_create_index_defaults_to_projection():
    adapter = _CHAdapter(DDL)
    response = asyncio.run(
        ClickHouseIndexOps(adapter).create_index(
            "t", "db", columns=["id"], name="proj"
        )
    )

    assert response["is_error"] is False
    joined = "\n".join(adapter.executed)
    assert "ADD PROJECTION" in joined
    assert "_part_offset" in joined


def test_clickhouse_multi_column_creates_composite_projection():
    adapter = _CHAdapter(DDL)
    response = asyncio.run(
        ClickHouseIndexOps(adapter).create_index(
            "t", "db", columns=["id", "name"], name="proj"
        )
    )

    assert response["is_error"] is False
    assert "ORDER BY (`id`, `name`)" in "\n".join(adapter.executed)


def test_clickhouse_projection_requires_version_25_5():
    adapter = _CHAdapter(DDL, version="25.4.1")
    response = asyncio.run(
        ClickHouseIndexOps(adapter).create_index(
            "t", "db", columns=["id"], name="proj"
        )
    )

    assert response["is_error"] is True
    assert "25.5" in response["error_message"]
    assert adapter.executed == []
