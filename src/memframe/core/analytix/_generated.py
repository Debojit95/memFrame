"""Publish row-preserving generated columns back to the source table.

Row-preserving ops (window, groupby window, groupby cumulative) compute a
wide result table that has exactly one row per source row. This module
swaps that wide table into the source (keeping the source table name), then
creates the separate narrow result table that the deep cache stores: only
the involved and generated columns.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from memframe.core.ingestion.datatype_detector import Backend
from memframe.utils.helper import SQLIdentifierSanitizer


def _san(name: str) -> str:
    return SQLIdentifierSanitizer.sanitize(name)


def _qualified(db, schema: str, table: str) -> str:
    return (
        f"{db.quote_identifier(_san(schema))}."
        f"{db.quote_identifier(_san(table))}"
    )


def _backend_name(backend) -> Optional[str]:
    return getattr(backend, "backend", None) if backend is not None else None


def _dedupe(cols: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(_san(c) for c in cols if c))


def make_sig(**kwargs: Any) -> str:
    return json.dumps(kwargs, sort_keys=True, default=str)


async def swap_table_into_source(
    db, backend, schema: str, source: str, wide: str,
) -> None:
    """Replace ``source`` with ``wide`` (one row per source row)."""
    src_q = _qualified(db, schema, source)
    wide_q = _qualified(db, schema, wide)
    be = _backend_name(backend)

    if be == Backend.CLICKHOUSE:
        await db.execute(f"EXCHANGE TABLES {src_q} AND {wide_q}")
        await db.execute(f"DROP TABLE {wide_q}")
    elif be == Backend.POSTGRES:
        await db.execute(f"DROP TABLE {src_q}")
        await db.execute(
            f"ALTER TABLE {wide_q} RENAME TO {db.quote_identifier(_san(source))}"
        )
    else:
        # DuckDB: CREATE OR REPLACE keeps the source name without a
        # drop/rename dance and works for the single-file adapter.
        await db.execute(
            f"CREATE OR REPLACE TABLE {src_q} AS SELECT * FROM {wide_q}"
        )
        await db.execute(f"DROP TABLE {wide_q}")


async def _drop_columns(db, backend, schema: str, table: str, cols: List[str]) -> None:
    if not cols:
        return
    q = _qualified(db, schema, table)
    for col in cols:
        await db.execute(
            f"ALTER TABLE {q} DROP COLUMN IF EXISTS {db.quote_identifier(col)}"
        )
    if _backend_name(backend) != Backend.CLICKHOUSE:
        return

    # ClickHouse applies DROP COLUMN as an async mutation.
    import asyncio

    bare = _san(table)
    for _ in range(120):
        rows = await db.fetch(
            "SELECT count() AS pending FROM system.mutations "
            f"WHERE database = {backend.placeholder(1)} "
            f"AND table = {backend.placeholder(2)} AND is_done = 0",
            _san(schema), bare,
        )
        pending = 0
        if rows:
            first = rows[0]
            pending = first.get("pending", 0) if isinstance(first, dict) else first[0]
        if not pending:
            return
        await asyncio.sleep(0.1)


async def _known_generated_columns(
    ops, table: str, op_type: str, backend, data_id: str,
) -> set:
    rows = await ops.db.fetch(
        f"""SELECT kwargs FROM {backend.cache_registry_table}
            WHERE data_id = {backend.placeholder(1)}
              AND operation_type = {backend.placeholder(2)}
              AND generated_table_name = {backend.placeholder(3)}""",
        data_id, op_type, table,
    )
    known = set()
    for row in rows or []:
        try:
            stored = row.get("kwargs") if isinstance(row, dict) else row[0]
            data = json.loads(stored or "{}")
            cols = data.get("new_columns") if isinstance(data, dict) else None
            if isinstance(cols, list):
                known.update(cols)
        except Exception:
            continue
    return known


async def assert_owned_columns(
    ops, table: str, schema: str, generated: List[str],
    op_type: str, backend, data_id: str,
) -> Optional[str]:
    """Return an error when a generated name already belongs to someone else."""
    if backend is None or data_id is None:
        return None
    try:
        cols = set(await ops.db.get_column_types(table, schema) or {})
    except Exception:
        cols = set()
    clashes = [c for c in generated if c in cols]
    if not clashes:
        return None
    known = await _known_generated_columns(ops, table, op_type, backend, data_id)
    if set(clashes) <= known:
        return None
    return (
        f"column(s) {clashes} already exist on {schema}.{table} and were not "
        "created by a previous identical operation. Drop or rename them first."
    )


async def record_marker(
    ops, table: str, schema: str, sig: str, op_type: str,
    class_name: str, method_name: str, backend, data_id: str,
) -> None:
    max_op = await ops._backend_fetch_val(
        backend,
        f"""SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.cache_registry_table}
            WHERE data_id = {backend.placeholder(1)}""",
        data_id,
    )
    opidx = (max_op or 0) + 1
    await ops.db.execute(
        f"""INSERT INTO {backend.cache_registry_table}
            (data_id, opidx, operation_type, class_name, method_name,
             args, kwargs, generated_table_name, is_deep_cache, schema)
            VALUES ({backend.placeholder(1)}, {backend.placeholder(2)},
                    {backend.placeholder(3)}, {backend.placeholder(4)},
                    {backend.placeholder(5)}, {backend.placeholder(6)},
                    {backend.placeholder(7)}, {backend.placeholder(8)},
                    {backend.placeholder(9)}, {backend.placeholder(10)})""",
        data_id, opidx, op_type, class_name, method_name,
        "", sig, table, False, schema,
    )


async def publish_row_aligned(
    ops,
    table: str,
    schema: str,
    result: Dict[str, Any],
    involved: Iterable[str],
    *,
    sig: str,
    op_type: str,
    class_name: str,
    method_name: str,
    backend,
    data_id,
) -> Dict[str, Any]:
    """Swap the wide result into the source and return the narrow payload."""
    if not isinstance(result, dict) or result.get("is_error"):
        return result

    wide = result.get("new_table")
    generated = result.get("new_columns")
    if not isinstance(generated, list) or not generated:
        generated = [result["new_column"]] if result.get("new_column") else []
    generated = _dedupe(generated)
    if not wide or not generated:
        return result

    involved = list(involved)
    try:
        before_cols = set(await ops.db.get_column_types(table, schema) or {})
        wide_cols = set(await ops.db.get_column_types(wide, schema) or {})
    except Exception:
        before_cols, wide_cols = set(), set()
    # Internal ordering columns (rn, __rn, __totem_*) are not part of the
    # source; do not let the swap publish them.
    temp_cols = sorted(wide_cols - before_cols - set(generated))

    error = await assert_owned_columns(
        ops, table, schema, generated, op_type, backend, data_id,
    )
    if error:
        return {
            "is_error": True,
            "message": "",
            "error_message": error,
            "result": None,
            "new_table": None,
            "new_columns": [],
        }

    await swap_table_into_source(ops.db, backend, schema, table, wide)
    await _drop_columns(ops.db, backend, schema, table, temp_cols)
    marker = json.dumps(
        {"sig": sig, "new_columns": generated}, sort_keys=True,
    )
    await record_marker(
        ops, table, schema, marker, op_type, class_name, method_name,
        backend, data_id,
    )

    cols = _dedupe([*involved, *generated])
    result["new_table"] = table
    result["mapped_table"] = table
    result["mapped_columns"] = generated
    result["cache_kind"] = "ref"
    result["ref_schema"] = schema
    result["ref_table"] = table
    result["involved_cols"] = involved
    result["generated_cols"] = generated
    result["result_cols"] = cols
    result["mutates"] = True

    # Make the first response match the cached replay order:
    # involved columns first, then generated columns.
    df = result.get("result")
    if df is not None and hasattr(df, "columns") and all(c in df.columns for c in cols):
        result["result"] = df[cols].reset_index(drop=True)
    return result
