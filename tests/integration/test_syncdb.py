"""SyncDB integration tests: register pre-existing DB tables into csv_registry.

Runs against whatever backends are selected via MEMFRAME_TEST_BACKENDS
(default: duckdb only). Postgres/ClickHouse require a live server and are
skipped automatically when unreachable.
"""

import asyncio
import os
import tempfile
from typing import Any, Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
import pytest

from memframe.main import MemFrame

pytestmark = pytest.mark.integration

SCHEMAS = ["sales", "hr", "finance", "inventory"]
TABLE_SPECS: List[Tuple[str, str, int]] = [
    ("sales", "orders", 100),
    ("sales", "customers", 250),
    ("hr", "employees", 400),
    ("hr", "departments", 550),
    ("hr", "salaries", 700),
    ("finance", "transactions", 850),
    ("finance", "accounts", 1000),
    ("inventory", "products", 1200),
    ("inventory", "suppliers", 1500),
    ("inventory", "stock", 1800),
]

MEMFRAME_SCHEMAS = ["upload", "transient", "registry"]


def _make_df(row_count: int) -> pd.DataFrame:
    rng = np.random.default_rng(row_count)
    return pd.DataFrame(
        {
            "id": list(range(1, row_count + 1)),
            "name": [f"name_{i}" for i in range(1, row_count + 1)],
            "value": rng.random(row_count) * 100,
        }
    )


def _gen_duckdb(db_path: str) -> None:
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = duckdb.connect(db_path)
    try:
        for schema in SCHEMAS:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        with tempfile.TemporaryDirectory() as tmp:
            for schema, table, rc in TABLE_SPECS:
                csv = os.path.join(tmp, f"{schema}_{table}.csv")
                _make_df(rc).to_csv(csv, index=False)
                conn.execute(
                    f"CREATE OR REPLACE TABLE {schema}.{table} "
                    f"AS SELECT * FROM read_csv_auto('{csv}')"
                )
    finally:
        conn.close()


async def _gen_postgres(params: Dict[str, Any]) -> None:
    import asyncpg

    conn = await asyncpg.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        database=params["database"],
    )
    try:
        for schema in SCHEMAS + MEMFRAME_SCHEMAS:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for schema in SCHEMAS:
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for schema, table, rc in TABLE_SPECS:
            await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            await conn.execute(
                f'CREATE TABLE "{schema}"."{table}" '
                f"(id INT PRIMARY KEY, name VARCHAR(255), value DOUBLE PRECISION)"
            )
            records = list(_make_df(rc).itertuples(index=False, name=None))
            await conn.copy_records_to_table(table, schema_name=schema, records=records)
    finally:
        await conn.close()


def _gen_clickhouse(params: Dict[str, Any]) -> None:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        secure=params.get("secure", False),
    )
    for schema in SCHEMAS + MEMFRAME_SCHEMAS:
        client.command(f"DROP DATABASE IF EXISTS {schema}")
    for schema in SCHEMAS:
        client.command(f"CREATE DATABASE IF NOT EXISTS {schema}")
    for schema, table, rc in TABLE_SPECS:
        client.command(f"DROP TABLE IF EXISTS {schema}.{table}")
        client.command(
            f"CREATE TABLE {schema}.{table} "
            f"(id Int32, name String, value Float64) ENGINE = MergeTree ORDER BY id"
        )
        client.insert_df(f"{schema}.{table}", _make_df(rc))


def _generate_multischema(backend: str, params: Dict[str, Any]) -> None:
    if backend == "duckdb":
        return _gen_duckdb(params["db_path"])
    if backend == "postgres":
        try:
            asyncio.run(_gen_postgres(params))
        except Exception as exc:  # noqa: BLE001 - unreachable server => skip
            pytest.skip(f"Postgres unavailable for SyncDB test: {exc}")
        return
    if backend == "clickhouse":
        try:
            _gen_clickhouse(params)
        except Exception as exc:  # noqa: BLE001 - unreachable server => skip
            pytest.skip(f"ClickHouse unavailable for SyncDB test: {exc}")
        return


@pytest.fixture
def synced_memframe(backend_config, tmp_path):
    backend = backend_config["backend"]
    params = dict(backend_config.get("params", {}))
    # ponytail: isolated file for duckdb so we never touch memFrame_new.duckdb
    # used by other integration tests, and avoid stale-registry false passes.
    if backend == "duckdb":
        params = {"db_path": str(tmp_path / "syncdb.duckdb")}
    _generate_multischema(backend, params)
    mf = MemFrame(
        connection_type=backend_config["connection_type"],
        connection_params=params,
    )
    asyncio.run(mf.aconnect())
    try:
        yield mf
    finally:
        asyncio.run(mf.aclose())


def _assert_no_system_leak(registered: Dict[str, List[Dict[str, Any]]], backend: str) -> None:
    keys = set(registered.keys())
    if backend == "clickhouse":
        assert "INFORMATION_SCHEMA" not in keys
        assert "system" not in keys
        assert "information_schema" not in keys
    elif backend == "postgres":
        assert not any(k.startswith("pg_") or k == "information_schema" for k in keys)
    elif backend == "duckdb":
        assert keys == set(SCHEMAS)


class TestSyncDBRegister:
    def test_register_discovers_all_schemas(self, synced_memframe):
        registered = asyncio.run(synced_memframe.aregister_tables())
        expected = {(s, t) for s, t, _ in TABLE_SPECS}
        got = {(s, t["table_name"]) for s, tbls in registered.items() for t in tbls}
        assert expected <= got, f"missing expected tables: {expected - got}"
        for s, t, rc in TABLE_SPECS:
            row = next(x for x in registered[s] if x["table_name"] == t)
            assert row["row_count"] == rc
        _assert_no_system_leak(registered, synced_memframe._backend.backend)

    def test_registered_tables_queryable(self, synced_memframe, get_result_df):
        registered = asyncio.run(synced_memframe.aregister_tables())
        for s, t, rc in TABLE_SPECS:
            data_id = next(x["data_id"] for x in registered[s] if x["table_name"] == t)
            asyncio.run(synced_memframe.aset_active(data_id))
            ds = synced_memframe.memFrame()
            df = get_result_df(asyncio.run(ds.ahead(n=rc)))
            assert len(df) == rc

    def test_register_idempotent(self, synced_memframe):
        first = asyncio.run(synced_memframe.aregister_tables())
        total = sum(len(v) for v in first.values())
        assert total >= len(TABLE_SPECS)
        second = asyncio.run(synced_memframe.aregister_tables())
        assert second == {}

    def test_delete_keeps_external_table(self, synced_memframe):
        registered = asyncio.run(synced_memframe.aregister_tables())
        s, t, rc = TABLE_SPECS[0]
        data_id = next(x["data_id"] for x in registered[s] if x["table_name"] == t)
        asyncio.run(synced_memframe.adelete_table(data_id=data_id))
        tables = asyncio.run(synced_memframe.alist_tables())
        assert not any(x["data_id"] == data_id for x in tables)
        # real table survives: a fresh register re-discovers it (new data_id).
        reregistered = asyncio.run(synced_memframe.aregister_tables())
        assert any(x["table_name"] == t for x in reregistered.get(s, []))
