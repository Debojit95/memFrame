import asyncio

import pandas as pd
import pytest


def test_set_order_by_rebuild(backend_config):
    if backend_config["backend"] != "clickhouse":
        pytest.skip("ClickHouse-only ORDER BY rebuild")

    from memframe import MemFrame

    async def run():
        instance = MemFrame(
            connection_type=backend_config["connection_type"],
            connection_params=backend_config.get("params", {}),
        )
        await instance.aconnect()
        try:
            ctx = await instance.aupload_df(
                pd.DataFrame(
                    {
                        "id": [1, 2, 3],
                        "name": ["alpha", "beta", "gamma"],
                    }
                ),
                filename="ch_order_by",
            )
            await ctx.acreate_index(
                columns=["id"], name="idx_before_rebuild", index_type="minmax"
            )

            before = await instance._backend.fetch(
                "SELECT sorting_key, primary_key FROM system.tables "
                "WHERE database = ? AND name = ?",
                instance._backend.upload_schema,
                ctx._data_id,
            )
            plan = await ctx.aset_order_by(columns=["id"], dry_run=True)
            assert plan["rebuilt"] is False
            assert "ORDER BY (`id`)" in plan["ddl"]
            after_dry = await instance._backend.fetch(
                "SELECT sorting_key FROM system.tables "
                "WHERE database = ? AND name = ?",
                instance._backend.upload_schema,
                ctx._data_id,
            )
            assert before[0][0] == after_dry[0][0]

            result = await ctx.aset_order_by(
                columns=["id"], dry_run=False, keep_backup=True
            )
            assert result["rebuilt"] is True
            assert result["backup_table"]

            after = await instance._backend.fetch(
                "SELECT sorting_key, primary_key FROM system.tables "
                "WHERE database = ? AND name = ?",
                instance._backend.upload_schema,
                ctx._data_id,
            )
            assert "id" in after[0][0]
            assert "id" in after[0][1]

            head = await ctx.ahead(10)
            assert sorted(head["id"].tolist()) == [1, 2, 3]
            assert sorted(head["name"].tolist()) == ["alpha", "beta", "gamma"]

            listed = await ctx.alist_indexes()
            assert any(row["name"] == "idx_before_rebuild" for row in listed)
            assert any(row["source"] == "order_by" for row in listed)

            await instance._backend.execute(
                f"DROP TABLE IF EXISTS `{instance._backend.upload_schema}`."
                f"`{result['backup_table']}`"
            )
            await instance.adelete_table(filename="ch_order_by")
        finally:
            await instance.aclose()

    asyncio.run(run())
