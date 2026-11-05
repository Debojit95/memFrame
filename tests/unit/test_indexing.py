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
