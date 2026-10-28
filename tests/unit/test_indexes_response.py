import asyncio

import pandas as pd
import pytest

from memframe.exceptions import OperationError
from memframe.main import MemFrame


@pytest.fixture
def ctx():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["a", "b", "c"],
                }
            ),
            filename="indexes_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_create_list_drop(ctx):
    created = ctx.create_index(columns=["id"], name="idx_id")

    assert created["name"] == "idx_id"
    assert created["unique"] is False
    assert created["source"] == "index"
    assert any(row["name"] == "idx_id" for row in ctx.list_indexes())

    dropped = ctx.drop_index("idx_id")
    assert dropped["dropped"] is True
    assert all(row["name"] != "idx_id" for row in ctx.list_indexes())


def test_generated_index_name(ctx):
    created = ctx.create_index(columns=["id"])

    assert created["name"].endswith("_id_idx")


def test_unique_index_rejects_duplicate_data():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        ctx = memframe.upload_df(
            pd.DataFrame({"name": ["a", "a"]}), filename="indexes_dupes"
        )
        with pytest.raises(OperationError, match="create_index"):
            ctx.create_index(columns=["name"], unique=True)
    finally:
        asyncio.run(memframe.aclose())


def test_missing_column_raises(ctx):
    with pytest.raises(OperationError, match="not found"):
        ctx.create_index(columns=["missing"])


def test_if_not_exists_is_idempotent(ctx):
    first = ctx.create_index(columns=["id"], name="idx_id")
    second = ctx.create_index(columns=["id"], name="idx_id")

    assert first["name"] == second["name"] == "idx_id"


def test_drop_missing_index_if_exists(ctx):
    result = ctx.drop_index("does_not_exist")

    assert result["dropped"] is False


def test_logical_index_api_is_gone(ctx):
    with pytest.raises(AttributeError):
        ctx.set_index
    with pytest.raises(AttributeError):
        ctx.index


def test_set_order_by_is_clickhouse_only(ctx):
    with pytest.raises(OperationError, match="ClickHouse"):
        ctx.set_order_by(columns=["id"])


def test_async_twins():
    async def run():
        memframe = MemFrame(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
        )
        await memframe.aconnect()
        try:
            ctx = await memframe.aupload_df(
                pd.DataFrame({"id": [1, 2]}), filename="indexes_async"
            )
            created = await ctx.acreate_index(columns=["id"], name="idx_async")
            assert created["name"] == "idx_async"
            assert any(
                row["name"] == "idx_async" for row in await ctx.alist_indexes()
            )
            assert (await ctx.adrop_index("idx_async"))["dropped"] is True
        finally:
            await memframe.aclose()

    asyncio.run(run())
