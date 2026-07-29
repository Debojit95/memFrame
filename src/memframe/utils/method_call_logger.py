import asyncio
import json
from functools import wraps
from typing import Any, Callable, Dict, Optional

import pandas as pd
import pyarrow as pa

from memframe.core.ingestion.datatype_detector import Backend


def _json_signature(value: Any) -> str:
    return json.dumps(value)


def _qualify(schema: str, bare: str, backend) -> str:
    if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
        return f"`{schema}`.`{bare}`"
    return f'{schema}."{bare}"'


def _make_writer(mf):
    async def writer(payload, data_id, generated_table_name=None, is_deep_cache=False, schema=None):
        await mf._arecord_method_call(
            data_id=data_id,
            class_name=payload["class_name"],
            method_name=payload["method_name"],
            args=payload["args"],
            kwargs=payload["kwargs"],
            generated_table_name=generated_table_name,
            is_deep_cache=is_deep_cache,
            schema=schema,
        )
    return writer


async def _load_generated_table(backend, qualified: str) -> Optional[pd.DataFrame]:
    try:
        rows = await backend.fetch(f"SELECT * FROM {qualified}")
        if not rows:
            return None
        # ponytail: per-backend column name query — real backend difference
        be = getattr(backend, "backend", None)
        if be == Backend.POSTGRES:
            schema, table = qualified.replace('"', "").split(".")
            col_rows = await backend.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2",
                schema.strip(), table.strip(),
            )
            col_names = [r[0] for r in col_rows]
        elif be == Backend.CLICKHOUSE:
            col_rows = await backend.fetch(f"DESCRIBE TABLE {qualified}")
            col_names = [r[0] for r in col_rows]
        else:
            col_rows = await backend.fetch(f"DESCRIBE {qualified}")
            col_names = [r[0] for r in col_rows]
        return pd.DataFrame(rows, columns=col_names)
    except Exception:
        return None


async def _create_deep_cache_table(mf, backend, data_id: str, df: pd.DataFrame) -> str:
    op = await backend.fetch_val(
        f"SELECT COALESCE(MAX(opidx), 0) FROM {backend.transient_registry_table} "
        f"WHERE data_id = {backend.placeholder(1)}", data_id,
    )
    op = (op or 0) + 1
    qualified = backend.get_transient_table_name(data_id, op)
    arrow = pa.Table.from_pandas(df, preserve_index=False)
    await mf._create_final_table_all_text(qualified, arrow.schema.names)
    await mf._insert_arrow_table(qualified, arrow)
    return qualified


def _make_hit_response(payload, cached, table_name):
    return {
        "is_error": False,
        "message": (
            f"Cache hit for {payload['class_name']}."
            f"{payload['method_name']}; "
            f"reused generated table '{table_name}'"
        ),
        "error_message": None,
        "involved_cols": [],
        "generated_cols": list(cached.columns),
        "result": cached,
        "new_table": table_name,
        "result_metadata": {
            "from_cache": True,
            "saved_table": table_name,
            "row_count": len(cached),
            "column_count": len(cached.columns),
            "strict_args_kwargs_match": True,
        },
    }


def record_call(func=None, deep_cache=None):
    """Two-level cache decorator.
    Level 1 (default, deep_cache=False): record arg/kwarg signatures only.
    On repeat call with same args → re-execute (no table to load).
    Level 2 (deep, deep_cache=True): saves result DataFrames as transient tables.
    On repeat call with same args → load table directly, skip execution.
    Resolution order: decorator arg → MemFrame.deep_cache → False.
    Usage:
        @record_call
        @record_call()
        @record_call(deep_cache=True)
        @record_call(deep_cache=False)
    """
    if func is not None:
        return _make_decorator(func, deep_cache)
    return lambda f: _make_decorator(f, deep_cache)


def _make_decorator(func, decorator_deep_cache):
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            mf = getattr(self, "_memframe", None)
            if mf is None:
                raise RuntimeError(
                    f"Cannot cache {func.__qualname__}: instance lacks `_memframe`. "
                    "Inherit from LoggableMixin or set `self._memframe`."
                )

            data_id = getattr(self, "_data_id", None) or mf._active_id
            if not data_id:
                raise RuntimeError(
                    f"Cannot cache {func.__qualname__}: no data_id available."
                )

            # ponytail: master switch — MemFrame(deep_cache=False) overrides all
            mf_deep = getattr(mf, "deep_cache", None)
            is_deep = False
            if decorator_deep_cache is not None:
                is_deep = decorator_deep_cache
            elif mf_deep is True:
                is_deep = True
            if mf_deep is False:
                is_deep = False
            backend = getattr(mf, "_backend", None)

            writer = getattr(self, "_call_writer", None)
            if writer is None:
                writer = _make_writer(mf)
                self._call_writer = writer

            payload = {
                "class_name": self.__class__.__name__,
                "method_name": func.__name__,
                "args": args,
                "kwargs": kwargs,
            }

            # --- Lookup: only for deep cache entries ---
            if is_deep and backend:
                args_sig = _json_signature(args)
                kwargs_sig = _json_signature(kwargs)
                row = await backend.fetch_row(
                    f"""
                    SELECT generated_table_name, schema
                    FROM {backend.transient_registry_table}
                    WHERE data_id = {backend.placeholder(1)}
                      AND operation_type = 'method_call'
                      AND class_name = {backend.placeholder(2)}
                      AND method_name = {backend.placeholder(3)}
                      AND args = {backend.placeholder(4)}
                      AND kwargs = {backend.placeholder(5)}
                      AND is_deep_cache = TRUE
                      AND generated_table_name IS NOT NULL
                    ORDER BY opidx DESC LIMIT 1
                    """,
                    data_id,
                    payload["class_name"],
                    payload["method_name"],
                    args_sig,
                    kwargs_sig,
                )
                if row:
                    bare, sch = row[0], row[1]
                    sch = sch or backend.transient_schema
                    q = _qualify(sch, bare, backend)
                    cached = await _load_generated_table(backend, q)
                    if cached is not None:
                        return _make_hit_response(payload, cached, q)

            # --- Execute ---
            result = await func(self, *args, **kwargs)
            generated_table_name = None
            schema = None

            if is_deep and isinstance(result, dict) and not result.get("is_error", False):
                bare_table = result.get("new_table") or result.get("generated_table_name")
                if bare_table and backend:
                    if await backend.table_exists(_qualify(backend.transient_schema, bare_table, backend)):
                        generated_table_name = bare_table
                        schema = backend.transient_schema
                    elif await backend.table_exists(_qualify("transient", bare_table, backend)):
                        generated_table_name = bare_table
                        schema = "transient"
                    elif await backend.table_exists(_qualify(backend.upload_schema, bare_table, backend)):
                        be = getattr(backend, "backend", None)
                        if be == Backend.CLICKHOUSE:
                            await backend.execute(
                                f"RENAME TABLE {_qualify(backend.upload_schema, bare_table, backend)} "
                                f"TO {_qualify(backend.transient_schema, bare_table, backend)}"
                            )
                        elif be == Backend.DUCKDB:
                            await backend.execute(
                                f"CREATE TABLE {_qualify(backend.transient_schema, bare_table, backend)} "
                                f"AS SELECT * FROM {_qualify(backend.upload_schema, bare_table, backend)}"
                            )
                            await backend.execute(
                                f"DROP TABLE {_qualify(backend.upload_schema, bare_table, backend)}"
                            )
                        else:
                            await backend.execute(
                                f"ALTER TABLE {_qualify(backend.upload_schema, bare_table, backend)} "
                                f"SET SCHEMA {backend.transient_schema}"
                            )
                        generated_table_name = bare_table
                        schema = backend.transient_schema
                elif "result" in result:
                    df = result["result"]
                    if isinstance(df, pd.DataFrame) and not df.empty and backend:
                        qualified = await _create_deep_cache_table(
                            mf, backend, data_id, df
                        )
                        if qualified:
                            _sch, _bare = backend._split_qualified_table_name(qualified)
                            generated_table_name = _bare
                            schema = _sch
            elif not is_deep and isinstance(result, dict) and not result.get("is_error", False):
                # ponytail: drop any table the method created — deep_cache=False means no tables
                bare_table = result.get("new_table") or result.get("generated_table_name")
                if bare_table and backend:
                    for sch in (backend.transient_schema, "transient", backend.upload_schema):
                        q = _qualify(sch, bare_table, backend)
                        if q and await backend.table_exists(q):
                            await backend.drop_table(q)
                            break

            await writer(payload, data_id, generated_table_name, is_deep_cache=is_deep, schema=schema)
            return result
        return async_wrapper
    else:
        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        return sync_wrapper
