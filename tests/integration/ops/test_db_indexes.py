import uuid

import pandas as pd
import pytest

from memframe.exceptions import OperationError


def _name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def indexed_ctx(connected_memframe):
    ctx = connected_memframe.upload_df(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["alpha", "beta", "gamma"],
            }
        ),
        filename="db_indexes",
    )
    try:
        yield ctx
    finally:
        try:
            connected_memframe.delete_table(filename="db_indexes")
        except Exception:
            pass


def test_create_list_drop(indexed_ctx, backend_config):
    backend = backend_config["backend"]

    name = _name("idx_id")
    if backend == "clickhouse":
        created = indexed_ctx.create_index(
            columns=["id"], name=name, index_type="minmax"
        )
        assert created["source"] == "skipping"
        listed = indexed_ctx.list_indexes()
        assert any(row["name"] == name for row in listed)
        assert any(row["source"] == "order_by" for row in listed)
        assert indexed_ctx.drop_index(name)["dropped"] is True
        assert all(row["name"] != name for row in indexed_ctx.list_indexes())
    else:
        created = indexed_ctx.create_index(columns=["id"], name=name)
        assert created["source"] == "index"
        assert created["name"] == name
        assert any(row["name"] == name for row in indexed_ctx.list_indexes())
        assert indexed_ctx.drop_index(name)["dropped"] is True
        assert all(row["name"] != name for row in indexed_ctx.list_indexes())


def test_unique_index(indexed_ctx, backend_config):
    backend = backend_config["backend"]
    if backend == "clickhouse":
        with pytest.raises(OperationError, match="unique"):
            indexed_ctx.create_index(columns=["name"], unique=True)
        return

    created = indexed_ctx.create_index(
        columns=["name"], name=_name("idx_name"), unique=True
    )
    assert created["unique"] is True


def test_unique_index_rejects_duplicates(connected_memframe, backend_config):
    if backend_config["backend"] == "clickhouse":
        pytest.skip("ClickHouse has no unique secondary indexes")

    ctx = connected_memframe.upload_df(
        pd.DataFrame({"name": ["dup", "dup"]}), filename="db_indexes_dupes"
    )
    try:
        with pytest.raises(OperationError, match="create_index"):
            ctx.create_index(columns=["name"], unique=True)
    finally:
        connected_memframe.delete_table(filename="db_indexes_dupes")


def test_postgres_options(indexed_ctx, backend_config):
    if backend_config["backend"] != "postgres":
        pytest.skip("PostgreSQL-specific index options")

    partial = indexed_ctx.create_index(
        columns=["name"],
        name=_name("idx_partial"),
        where="id > 1",
        include=["id"],
    )
    assert partial["valid"] is True

    concurrent = indexed_ctx.create_index(
        columns=["id"], name=_name("idx_concurrent"), concurrently=True
    )
    assert concurrent["valid"] is True

    brin = indexed_ctx.create_index(
        columns=["id"], name=_name("idx_brin"), method="brin"
    )
    assert brin["method"] == "brin"


def test_clickhouse_skipping_index(indexed_ctx, backend_config):
    if backend_config["backend"] != "clickhouse":
        pytest.skip("ClickHouse-specific skipping indexes")

    bloom_name = _name("idx_name_bloom")
    created = indexed_ctx.create_index(
        columns=["name"],
        name=bloom_name,
        index_type="bloom_filter",
        granularity=2,
    )
    assert created["type"] == "bloom_filter"
    assert created["granularity"] == 2

    expr_name = _name("idx_name_expr")
    expression = indexed_ctx.create_index(
        expression="lower(name)",
        name=expr_name,
        index_type="minmax",
    )
    assert expression["expression"] == "lower(name)"

    listed = indexed_ctx.list_indexes()
    names = {row["name"] for row in listed}
    assert {bloom_name, expr_name} <= names


def test_registry_has_no_index_cols(backend_config):
    import asyncio

    from memframe import MemFrame

    async def run():
        instance = MemFrame(
            connection_type=backend_config["connection_type"],
            connection_params=backend_config.get("params", {}),
        )
        await instance.aconnect()
        try:
            backend = instance._backend
            if backend.backend == "clickhouse":
                rows = await backend.fetch(
                    "SELECT name FROM system.columns "
                    "WHERE database = ? AND table = 'memframe_csv_registry' "
                    "AND name = 'index_cols'",
                    backend.registry_schema,
                )
            else:
                p1 = backend.placeholder(1)
                p2 = backend.placeholder(2)
                rows = await backend.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = {p1} "
                    "AND table_name = 'memframe_csv_registry' "
                    f"AND column_name = {p2}",
                    backend.registry_schema,
                    "index_cols",
                )
            assert rows == []
        finally:
            await instance.aclose()

    asyncio.run(run())


def test_dropping_dataset_removes_indexes(connected_memframe, backend_config):
    ctx = connected_memframe.upload_df(
        pd.DataFrame({"id": [1, 2]}), filename="db_indexes_drop"
    )
    name = _name("idx_drop")
    if backend_config["backend"] == "clickhouse":
        ctx.create_index(columns=["id"], name=name, index_type="minmax")
    else:
        ctx.create_index(columns=["id"], name=name)
    connected_memframe.delete_table(filename="db_indexes_drop")

    with pytest.raises(Exception):
        ctx.list_indexes()
