import pandas as pd
import pytest

from memframe.core.indexing import (
    MF_IDX,
    assert_user_column,
    mf_idx_sql_type,
    select_mf_idx_dtype,
    strip_hidden,
)


def test_mf_idx_dtype_boundaries():
    assert select_mf_idx_dtype(0) == "uint8"
    assert select_mf_idx_dtype(255) == "uint8"
    assert select_mf_idx_dtype(256) == "uint16"
    assert select_mf_idx_dtype(65535) == "uint16"
    assert select_mf_idx_dtype(65536) == "uint32"
    assert select_mf_idx_dtype(2**32 - 1) == "uint32"
    assert select_mf_idx_dtype(2**32) == "uint64"


@pytest.mark.parametrize(
    ("logical", "duckdb", "postgres", "clickhouse"),
    [
        ("uint8", "UTINYINT", "SMALLINT", "UInt8"),
        ("uint16", "USMALLINT", "INTEGER", "UInt16"),
        ("uint32", "UINTEGER", "BIGINT", "UInt32"),
        ("uint64", "UBIGINT", "NUMERIC(20,0)", "UInt64"),
    ],
)
def test_mf_idx_sql_types(logical, duckdb, postgres, clickhouse):
    assert mf_idx_sql_type(logical, "duckdb") == duckdb
    assert mf_idx_sql_type(logical, "postgres") == postgres
    assert mf_idx_sql_type(logical, "clickhouse") == clickhouse


def test_reserved_column_guard():
    with pytest.raises(ValueError, match="reserved"):
        assert_user_column(MF_IDX)
    assert_user_column("price")


def test_strip_hidden():
    df = pd.DataFrame({MF_IDX: [0, 1], "price": [10, 20]})
    out = strip_hidden(df)
    assert list(out.columns) == ["price"]


def test_public_view_hides_mf_idx():
    import asyncio

    from memframe.main import MemFrame

    mf = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(mf.aconnect())
    try:
        ctx = mf.upload_df(pd.DataFrame({"price": [10, 20]}), filename="idx_view")
        rows = asyncio.run(mf._backend.fetch(
            f'SELECT * FROM memframe_upload."{ctx._data_id}"'
        ))
        cols = asyncio.run(mf._backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_upload' AND table_name = ?",
            ctx._data_id,
        ))
        assert [r[0] for r in cols] == ["price"]
        assert rows == [(10,), (20,)]
    finally:
        asyncio.run(mf.aclose())


def test_derived_public_views_hide_mf_idx():
    import asyncio

    from memframe.main import MemFrame

    async def run():
        mf = MemFrame(
            connection_type="local", connection_params={"db_path": ":memory:"}
        )
        await mf.aconnect()
        try:
            left = await mf.aupload_df(
                pd.DataFrame({"id": [1, 2], "a": [10, 20]}), filename="left"
            )
            right = await mf.aupload_df(
                pd.DataFrame({"id": [1, 2], "b": [1, 2]}), filename="right"
            )
            merged = await left.amerge(right, on="id")
            await left.amelt(id_vars=["id"], value_vars=["a"])
            melted_id = await mf._backend.fetchval(
                f"SELECT data_id FROM {mf._backend.csv_registry_table} "
                "WHERE filename LIKE 'melt_%'"
            )
            for data_id in (merged._data_id, melted_id):
                base = await mf._backend.fetchval(
                    f"SELECT base_table FROM {mf._backend.csv_registry_table} "
                    f"WHERE data_id = {mf._backend.placeholder(1)}",
                    data_id,
                )
                public_cols = await mf._backend.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'memframe_upload' AND table_name = ? "
                    "ORDER BY ordinal_position",
                    data_id,
                )
                base_cols = await mf._backend.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'memframe_upload' AND table_name = ?",
                    base,
                )
                assert public_cols, f"missing public view for {data_id}"
                assert MF_IDX not in [c[0] for c in public_cols]
                assert MF_IDX in [c[0] for c in base_cols]
        finally:
            await mf.aclose()

    asyncio.run(run())
