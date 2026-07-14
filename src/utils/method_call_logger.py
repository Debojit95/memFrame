import asyncio
import json
from functools import wraps
from typing import Any, Dict, Optional

import pandas as pd

from src.core.ingestion.datatype_detector import Backend
from src.utils.helper import SQLIdentifierSanitizer


def _json_signature(value: Any) -> str:
    """
    Keep signature generation aligned with _arecord_method_call storage.
    This provides strict byte-level arg/kwarg matching.
    """
    return json.dumps(value)


def _quote_ident(name: str) -> str:
    return f'"{name}"'


async def _find_cached_generated_table(
    memframe_instance,
    data_id: str,
    class_name: str,
    method_name: str,
    args_sig: str,
    kwargs_sig: str,
) -> Optional[str]:
    backend = getattr(memframe_instance, "_backend", None)
    if backend is None:
        return None

    p1 = backend.placeholder(1)
    p2 = backend.placeholder(2)
    p3 = backend.placeholder(3)
    p4 = backend.placeholder(4)
    p5 = backend.placeholder(5)

    row = await backend.fetch_one(
        f"""
        SELECT generated_table_name
        FROM {backend.transient_registry_table}
        WHERE data_id = {p1}
          AND operation_type = 'method_call'
          AND class_name = {p2}
          AND method_name = {p3}
          AND args = {p4}
          AND kwargs = {p5}
          AND generated_table_name IS NOT NULL
        ORDER BY opidx DESC
        LIMIT 1
        """,
        data_id,
        class_name,
        method_name,
        args_sig,
        kwargs_sig,
    )
    if not row:
        return None
    table_name = row[0]
    return table_name or None


async def _load_generated_table(memframe_instance, generated_table_name: str) -> Optional[pd.DataFrame]:
    backend = getattr(memframe_instance, "_backend", None)
    if backend is None:
        return None

    if "." in generated_table_name:
        schema_part, table_part = generated_table_name.split(".", 1)
        candidates = [(schema_part, table_part)]
    else:
        candidates = [
            (backend.upload_schema, generated_table_name),
            (backend.transient_schema, generated_table_name),
            ("main", generated_table_name),
        ]

    for schema_name, table_name in candidates:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema_name)
        safe_table = SQLIdentifierSanitizer.sanitize(table_name)
        qualified = f"{_quote_ident(safe_schema)}.{_quote_ident(safe_table)}"
        query = f"SELECT * FROM {qualified}"

        try:
            if backend.backend == Backend.POSTGRES:
                await backend._ensure_postgres_connection()
                rows = await backend._conn.fetch(query)
                return pd.DataFrame([dict(r) for r in rows])

            if backend.backend == Backend.DUCKDB:
                cur = backend._conn.execute(query)
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall()
                return pd.DataFrame(rows, columns=cols)
        except Exception:
            continue

    return None


async def _close_postgres_operation_adapter(self_instance) -> None:
    mf = getattr(self_instance, "_memframe", None)
    if mf is None or getattr(mf, "connection_type", None) != "remote":
        return

    ops_parent = getattr(self_instance, "_ops_parent", None)
    if ops_parent is None or not hasattr(ops_parent, "close"):
        return

    await ops_parent.close()


async def _get_cached_method_result(
    self_instance,
    data_id: str,
    class_name: str,
    method_name: str,
    args: tuple,
    kwargs: dict,
) -> Optional[Dict[str, Any]]:
    mf = getattr(self_instance, "_memframe", None)
    if mf is None:
        return None

    args_sig = _json_signature(args)
    kwargs_sig = _json_signature(kwargs)

    generated_table_name = await _find_cached_generated_table(
        memframe_instance=mf,
        data_id=data_id,
        class_name=class_name,
        method_name=method_name,
        args_sig=args_sig,
        kwargs_sig=kwargs_sig,
    )
    if not generated_table_name:
        return None

    cached_df = await _load_generated_table(mf, generated_table_name)
    if cached_df is None:
        return None

    return {
        "is_error": False,
        "message": f"Cache hit for {class_name}.{method_name}; reused generated table '{generated_table_name}'",
        "error_message": None,
        "involved_cols": [],
        "generated_cols": list(cached_df.columns),
        "result": cached_df,
        "new_table": generated_table_name,
        "result_metadata": {
            "from_cache": True,
            "saved_table": generated_table_name,
            "row_count": len(cached_df),
            "column_count": len(cached_df.columns),
            "strict_args_kwargs_match": True,
        },
    }


# ----------------------------------------------------------------------
# Async writer factories (accept payload, data_id, generated_table_name)
# ----------------------------------------------------------------------
def _pg_writer_async(memframe_instance):
    """Returns an async callable with signature (payload, data_id, generated_table_name)."""
    async def pg_record(payload, data_id, generated_table_name=None):
        await memframe_instance._arecord_method_call(
            data_id=data_id,
            class_name=payload["class_name"],
            method_name=payload["method_name"],
            args=payload["args"],
            kwargs=payload["kwargs"],
            generated_table_name=generated_table_name,
        )
    return pg_record


def _duckdb_writer_async(memframe_instance):
    """Returns an async callable with signature (payload, data_id, generated_table_name)."""
    async def duckdb_record(payload, data_id, generated_table_name=None):
        await memframe_instance._arecord_method_call(
            data_id=data_id,
            class_name=payload["class_name"],
            method_name=payload["method_name"],
            args=payload["args"],
            kwargs=payload["kwargs"],
            generated_table_name=generated_table_name,
        )
    return duckdb_record


# ----------------------------------------------------------------------
# The decorator (async‑aware, determines data_id automatically)
# ----------------------------------------------------------------------
def record_call(func):
    """
    Decorator that logs method calls automatically.
    Works with both sync and async methods.
    """
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            writer = getattr(self, "_call_writer", None)
            if writer is None:
                mf = getattr(self, "_memframe", None)
                if mf is None:
                    raise RuntimeError(
                        f"Cannot log call for {func.__qualname__}: "
                        "instance lacks `_call_writer` or `_memframe`. "
                        "Inherit from LoggableMixin or set `self._memframe`."
                    )
                # Build async writer and cache it
                if mf.connection_type == "local":
                    writer = _duckdb_writer_async(mf)
                else:
                    writer = _pg_writer_async(mf)
                self._call_writer = writer

            # Determine data_id
            data_id = getattr(self, "_data_id", None) or (
                mf._active_id if (mf := getattr(self, "_memframe", None)) else None
            )
            if not data_id:
                raise RuntimeError(
                    f"Cannot log call for {func.__qualname__}: "
                    "no data_id available. Set an active dataset on MemFrame "
                    "or provide a data_id to the context."
                )

            payload = {
                "class_name": self.__class__.__name__,
                "method_name": func.__name__,
                "args": args,
                "kwargs": kwargs,
            }

            generated_table_name = None
            should_record_call = True
            try:
                cached_result = await _get_cached_method_result(
                    self_instance=self,
                    data_id=data_id,
                    class_name=payload["class_name"],
                    method_name=payload["method_name"],
                    args=args,
                    kwargs=kwargs,
                )
                if cached_result is not None:
                    should_record_call = False
                    generated_table_name = cached_result.get("new_table")
                    return cached_result

                result = await func(self, *args, **kwargs)

                if isinstance(result, dict) and not result.get("is_error", False):
                    generated_table_name = (
                        result.get("new_table")
                        or result.get("generated_table_name")
                    )

                return result
            finally:
                try:
                    # Do not write a new operation row for cache hits.
                    if should_record_call:
                        await writer(payload, data_id, generated_table_name)
                finally:
                    await _close_postgres_operation_adapter(self)
        return async_wrapper
    else:
        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        return sync_wrapper
