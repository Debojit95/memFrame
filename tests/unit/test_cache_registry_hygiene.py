"""Cache registry hygiene: refs, failures, owned tables and leaks."""

import asyncio

import pandas as pd

from memframe.main import MemFrame


def _open():
    mf = MemFrame(
        connection_type="local", connection_params={"db_path": ":memory:"},
        deep_cache=True,
    )
    asyncio.run(mf.aconnect())
    ctx = asyncio.run(mf.aupload_df(
        pd.DataFrame({
            "g": ["a", "a", "b", "b"],
            "x": [1, 2, 3, 4],
            "y": [2, 1, 4, 3],
            "z": [1.0, None, 3.0, 4.0],
            "created_at": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
            ),
        }),
        filename="cache_hygiene",
    ))
    return mf, ctx


def _upload_tables(mf):
    return {
        r[0] for r in asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_upload'"
        ))
    }


def _cache_tables(mf):
    return {
        r[0] for r in asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_cache'"
        ))
    }


def _method_rows(mf, data_id):
    return asyncio.run(mf._backend.fetch(
        f"SELECT class_name, method_name, cache_kind, involved_cols, "
        f"generated_cols, result_cols, owns_table, ref_schema, epoch "
        f"FROM {mf._backend.cache_registry_table} "
        f"WHERE data_id = {mf._backend.placeholder(1)} "
        "AND operation_type = 'method_call' ORDER BY opidx",
        data_id,
    ))


def test_owned_tables_never_include_the_dataset():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.rolling(column="z", window=2, func="mean")
        assert asyncio.run(mf._owned_cache_tables(did)) == []

        asyncio.run(mf.adelete_table(data_id=did))
        assert _upload_tables(mf) == set()
        assert asyncio.run(mf._backend.fetchval(
            f"SELECT COUNT(*) FROM {mf._backend.cache_markers_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)}",
            did,
        )) == 0
    finally:
        asyncio.run(mf.aclose())


def test_compare_is_a_source_ref_and_replays():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.compare("x >= y")
        ctx.compare("x >= y")

        rows = _method_rows(mf, did)
        assert len(rows) == 1
        _, _, kind, involved, generated, result_cols, owns, _, epoch = rows[0]
        assert kind == "ref"
        assert involved == '["x", "y"]'
        assert generated == '["cmp_x_ge_y"]'
        assert result_cols == '["x", "y", "cmp_x_ge_y"]'
        assert not owns
        assert epoch == 1
    finally:
        asyncio.run(mf.aclose())


def test_failed_operation_is_not_cached():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        assert asyncio.run(ctx.groupby("g").asum("missing"))["is_error"]
        assert asyncio.run(ctx.groupby("g").asum("missing"))["is_error"]
        assert _method_rows(mf, did) == []
        assert _upload_tables(mf) == {did, f"{did}__base"}
    finally:
        asyncio.run(mf.aclose())


def test_window_chains_and_expanding_leave_no_working_tables():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.rolling(column="z", window=2, func=["mean", "sum"], order_by="created_at")
        ctx.expanding(column="z", func="mean", min_periods=1)
        ctx.groupby("g").fillna("z", method="mean")

        assert _upload_tables(mf) == {did, f"{did}__base"}
        assert _cache_tables(mf) == set()
    finally:
        asyncio.run(mf.aclose())


def test_fillna_caches_only_once():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.groupby("g").fillna("z", method="mean")
        rows = [
            r for r in _method_rows(mf, did)
            if r[1] in ("fillna", "groupby_fillna")
        ]
        assert len(rows) == 1
    finally:
        asyncio.run(mf.aclose())


def test_explicit_result_table_moves_to_cache_schema():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        response = asyncio.run(ctx.groupby("g").aagg(
            {"z": ["sum"]}, new_table="agg_out"
        ))
        assert "agg_out" not in _upload_tables(mf)
        assert "agg_out" in _cache_tables(mf)
        assert response["new_table"] == "agg_out"

        row = asyncio.run(mf._backend.fetch_row(
            f"SELECT ref_schema, owns_table FROM {mf._backend.cache_registry_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)} "
            "AND operation_type = 'method_call' ORDER BY opidx DESC LIMIT 1",
            did,
        ))
        assert row[0] == "memframe_cache"
        assert row[1] is True
    finally:
        asyncio.run(mf.aclose())


def test_non_deep_source_writeback_keeps_the_dataset():
    mf = MemFrame(
        connection_type="local", connection_params={"db_path": ":memory:"},
        deep_cache=False,
    )
    asyncio.run(mf.aconnect())
    try:
        ctx = asyncio.run(mf.aupload_df(
            pd.DataFrame({
                "g": ["a", "a", "b", "b"],
                "z": [1.0, 2.0, 3.0, 4.0],
                "created_at": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
                ),
            }),
            filename="cache_hygiene_non_deep",
        ))
        did = ctx._data_id
        ctx.rolling(column="z", window=2, func="mean")
        ctx.dt.floor("created_at", "day")
        ctx.compare("z >= z")

        assert _upload_tables(mf) == {did, f"{did}__base"}
        assert _cache_tables(mf) == set()
        assert len(ctx.head(n=1)) == 1
    finally:
        asyncio.run(mf.aclose())


def test_cache_markers_are_not_registry_entries():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.rolling(column="z", window=2, func="mean")
        ctx.groupby("g").agg({"z": ["sum"]}, map_feature=True)

        registry_types = asyncio.run(mf._backend.fetch(
            "SELECT DISTINCT operation_type FROM "
            f"{mf._backend.cache_registry_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)}",
            did,
        ))
        assert [r[0] for r in registry_types] == ["method_call"]

        markers = asyncio.run(mf._backend.fetch(
            f"SELECT operation_type, source_table FROM {mf._backend.cache_markers_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)} ORDER BY operation_type",
            did,
        ))
        assert {r[0] for r in markers} == {"groupby_map", "window_map"}
        assert {r[1] for r in markers} == {f"{did}__base"}
        assert _upload_tables(mf) == {did, f"{did}__base"}
    finally:
        asyncio.run(mf.aclose())


def test_legacy_markers_migrate_out_of_the_registry():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        asyncio.run(mf._backend.execute(
            f"INSERT INTO {mf._backend.cache_registry_table} "
            "(data_id, opidx, operation_type, class_name, method_name, "
            "args, kwargs, generated_table_name, is_deep_cache, schema) "
            f"VALUES ({mf._backend.placeholder(1)}, 999, 'window_map', "
            "'WindowWrapper', 'rolling', '', '{}', "
            f"{mf._backend.placeholder(2)}, FALSE, 'memframe_upload')",
            did, f"{did}__base",
        ))
        asyncio.run(mf._backend._migrate_cache_markers())

        legacy = asyncio.run(mf._backend.fetch(
            f"SELECT count(*) FROM {mf._backend.cache_registry_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)} "
            "AND operation_type = 'window_map'",
            did,
        ))
        assert legacy[0][0] == 0

        migrated = asyncio.run(mf._backend.fetch(
            f"SELECT count(*) FROM {mf._backend.cache_markers_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)}",
            did,
        ))
        assert migrated[0][0] == 1
    finally:
        asyncio.run(mf.aclose())


def test_epoch_advances_on_inferred_source_mutations():
    mf, ctx = _open()
    did = ctx._data_id
    try:
        ctx.compare("x >= y")
        ctx.dt.floor("created_at", "day")
        ctx.log_transform("z")

        epochs = [r[8] for r in _method_rows(mf, did)]
        assert epochs == sorted(epochs)
        assert len(set(epochs)) == len(epochs)
    finally:
        asyncio.run(mf.aclose())
