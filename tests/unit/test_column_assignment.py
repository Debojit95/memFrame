"""Column assignment: ``ctx["name"] = value`` / ``insert``.

Covers the scalar/list/tuple/Series inputs, overwrite semantics, validation,
cache-epoch invalidation, and the per-backend SQL hooks.
"""

import asyncio

import numpy as np
import pandas as pd
import pytest

from memframe import MemFrame
from memframe.core.analytix.inspection.clickhouse import ClickHouseTableOps
from memframe.core.analytix.inspection.duckdb import DuckDBTableOps
from memframe.core.analytix.inspection.postgres import PostgresTableOps
from memframe.exceptions import OperationError


@pytest.fixture
def ctx():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"a": [1, 2, 3]}), filename="column_assignment"
        )
    finally:
        asyncio.run(memframe.aclose())


def _values(ctx, column):
    return ctx.head(10)[column].tolist()


def _epoch(memframe, data_id):
    backend = memframe._backend
    return asyncio.run(
        backend.fetchval(
            f"SELECT cache_epoch FROM {backend.csv_registry_table} "
            f"WHERE data_id = {backend.placeholder(1)}",
            data_id,
        )
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (7, [7, 7, 7]),
        (2.5, [2.5, 2.5, 2.5]),
        ("x", ["x", "x", "x"]),
        (True, [True, True, True]),
    ],
)
def test_setitem_broadcasts_scalar(ctx, value, expected):
    ctx["assigned"] = value
    assert _values(ctx, "assigned") == expected


def test_setitem_accepts_list_tuple_and_series(ctx):
    ctx["from_list"] = [10, 20, 30]
    ctx["from_tuple"] = (1.5, 2.5, 3.5)
    ctx["from_series"] = pd.Series([9, 8, 7], index=[30, 20, 10])
    ctx["from_array"] = np.array([4.0, 5.0, 6.0])

    assert _values(ctx, "from_list") == [10, 20, 30]
    assert _values(ctx, "from_tuple") == [1.5, 2.5, 3.5]
    # The Series index is ignored: assignment is positional.
    assert _values(ctx, "from_series") == [9, 8, 7]
    assert _values(ctx, "from_array") == [4.0, 5.0, 6.0]


def test_setitem_overwrites_existing_column(ctx):
    ctx["assigned"] = [1, 2, 3]
    ctx["assigned"] = [4, 5, 6]

    assert _values(ctx, "assigned") == [4, 5, 6]
    assert ctx.head(10).columns.tolist().count("assigned") == 1


def test_setitem_converts_nan_to_null(ctx):
    ctx["assigned"] = [1.0, np.nan, 3.0]
    values = ctx.head(10)["assigned"]

    assert values.isna().tolist() == [False, True, False]


def test_setitem_rejects_length_mismatch(ctx):
    with pytest.raises(OperationError, match="Length mismatch"):
        ctx["assigned"] = [1, 2]


def test_setitem_rejects_unordered_set(ctx):
    with pytest.raises(OperationError, match="Sets are unordered"):
        ctx["assigned"] = {1, 2, 3}


def test_setitem_rejects_unsupported_value(ctx):
    with pytest.raises(OperationError, match="Unsupported value type"):
        ctx["assigned"] = {"a": 1}


def test_setitem_rejects_reserved_column(ctx):
    with pytest.raises(OperationError, match="reserved"):
        ctx["_mf_idx"] = [1, 2, 3]


def test_insert_still_accepts_list(ctx):
    ctx.insert(column="inserted", value=[7, 8, 9])
    assert _values(ctx, "inserted") == [7, 8, 9]


def test_new_column_visible_in_public_view(ctx):
    ctx["assigned"] = [1, 2, 3]
    backend = ctx.memframe._backend
    rows = asyncio.run(
        backend.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'memframe_upload' AND table_name = "
            f"{backend.placeholder(1)} ORDER BY ordinal_position",
            ctx._data_id,
        )
    )
    assert [r[0] for r in rows] == ["a", "assigned"]


def test_overwrite_bumps_cache_epoch(ctx):
    memframe = ctx.memframe
    before = _epoch(memframe, ctx._data_id)

    ctx["assigned"] = [1, 2, 3]
    after_create = _epoch(memframe, ctx._data_id)
    ctx["assigned"] = [4, 5, 6]
    after_overwrite = _epoch(memframe, ctx._data_id)

    assert after_create > before
    assert after_overwrite > after_create


class _FakeAdapter:
    def quote_identifier(self, name):
        return f'"{name}"'

    def placeholder(self, index=1):
        return "?"


def test_assignment_sql_hooks_per_backend():
    duck = DuckDBTableOps(_FakeAdapter())
    postgres = PostgresTableOps(_FakeAdapter())
    clickhouse = ClickHouseTableOps(_FakeAdapter())

    assert "list_extract" in duck._assignment_values_expr("DOUBLE PRECISION")
    assert "::DOUBLE PRECISION[]" in postgres._assignment_values_expr(
        "DOUBLE PRECISION"
    )
    assert "arrayElement" in clickhouse._assignment_values_expr(
        "Nullable(Float64)"
    )
    assert "mutations_sync = 1" in clickhouse._update_stmt(
        '"db"."t"', '"x" = 1'
    )
