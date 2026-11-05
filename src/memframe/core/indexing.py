"""Immutable positional index (``_mf_idx``) helpers.

The index is an internal, frozen row identity. It is only used for cache
ordering/replay and is never exposed as a user column.
"""

from __future__ import annotations

from typing import Any, Iterable, List

import pandas as pd

MF_IDX = "_mf_idx"
RESERVED_COLUMNS = frozenset({MF_IDX})
HIDDEN_COLUMNS = RESERVED_COLUMNS


def select_mf_idx_dtype(row_count: int) -> str:
    """Smallest unsigned dtype that can hold indices ``0..row_count-1``."""
    if row_count <= 2**8 - 1:
        return "uint8"
    if row_count <= 2**16 - 1:
        return "uint16"
    if row_count <= 2**32 - 1:
        return "uint32"
    return "uint64"


def mf_idx_sql_type(logical: str, backend: str) -> str:
    """Map a logical index dtype to a backend column type."""
    be = str(backend or "").lower()
    if be == "duckdb":
        return {
            "uint8": "UTINYINT",
            "uint16": "USMALLINT",
            "uint32": "UINTEGER",
            "uint64": "UBIGINT",
        }[logical]
    if be == "clickhouse":
        return {
            "uint8": "UInt8",
            "uint16": "UInt16",
            "uint32": "UInt32",
            "uint64": "UInt64",
        }[logical]
    # PostgreSQL has no native unsigned integers.
    return {
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "NUMERIC(20,0)",
    }[logical]


def assert_user_column(name: Any) -> None:
    """Raise for a user-supplied column that collides with a hidden index."""
    text = str(name)
    if text in RESERVED_COLUMNS:
        raise ValueError(f"'{text}' is a reserved internal column")


def assert_user_columns(names: Iterable[Any]) -> None:
    for name in names:
        assert_user_column(name)


def visible_columns(columns: Iterable[Any]) -> List[Any]:
    return [c for c in columns if str(c) not in HIDDEN_COLUMNS]


def strip_hidden(df: Any) -> Any:
    """Remove hidden internal columns from a user-facing DataFrame."""
    if isinstance(df, pd.DataFrame):
        drop = [c for c in df.columns if str(c) in HIDDEN_COLUMNS]
        return df.drop(columns=drop) if drop else df
    return df
