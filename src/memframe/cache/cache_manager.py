import asyncio
import json
import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa

from memframe.core.ingestion.datatype_detector import Backend
from memframe.exceptions import OperationError

logger = logging.getLogger("memFrame.cache")


class CacheManager:
    """Two-level method-call cache backed by the transient registry.

    Level 1 (default, deep_cache=False): records arg/kwarg signatures only —
    an audit/lineage log; repeat calls re-execute (no table to replay).
    Level 2 (deep, deep_cache=True): persists result DataFrames as typed
    transient tables; repeat calls load the table and skip execution.
    Resolution order: decorator arg -> MemFrame.deep_cache -> False.

    The instance itself is the ``record_call`` decorator:
        @record_call
        @record_call()
        @record_call(deep_cache=True)
    """

    # ── cache key (signature) ──────────────────────────────────────
    @staticmethod
    def _json_default(o: Any) -> Any:
        if isinstance(o, np.generic):
            return o.item()
        if isinstance(o, np.ndarray):
            return o.tolist()
        if isinstance(o, pd.DataFrame):
            return {"__dataframe__": list(o.shape), "columns": list(o.columns)}
        if isinstance(o, pd.Series):
            return {"__series__": list(o.shape), "name": str(o.name)}
        return f"<{type(o).__name__}>"

    def _signature(self, value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=self._json_default)

    def _qualify(self, schema: str, bare: str, backend) -> str:
        if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
            return f"`{schema}`.`{bare}`"
        return f'{schema}."{bare}"'

    # ── table load / store ─────────────────────────────────────────
    async def _load_generated_table(self, backend, qualified: str) -> Optional[pd.DataFrame]:
        start = time.perf_counter()
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
            df = pd.DataFrame(rows, columns=col_names)
            logger.debug(
                "[cache] reloaded %s (%d rows, %.3fs)", qualified, len(df),
                time.perf_counter() - start,
            )
            return df
        except Exception as exc:
            logger.warning("[cache] reload failed for %s: %s", qualified, exc)
            return None

    async def _create_deep_cache_table(self, mf, backend, data_id: str, df: pd.DataFrame) -> str:
        """Persist a DataFrame as a typed transient table (deep-cache payload)."""
        op = await backend.fetch_val(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {backend.transient_registry_table} "
            f"WHERE data_id = {backend.placeholder(1)}", data_id,
        )
        op = (op or 0) + 1
        qualified = backend.get_transient_table_name(data_id, op)
        arrow = pa.Table.from_pandas(df, preserve_index=False)
        uploader = mf._uploader
        schema = {}
        for i, name in enumerate(arrow.schema.names):
            pg_type = uploader._arrow_type_to_postgres(arrow.schema.field(i).type)
            schema[name] = {
                "postgres_type": pg_type,
                "clickhouse_type": uploader._postgres_type_to_clickhouse(pg_type),
                "is_nullable": True,
            }
        await uploader._create_final_table_typed(qualified, arrow.schema.names, schema)
        await uploader._insert_arrow_table(qualified, arrow)
        return qualified

    def _make_hit_response(self, payload: Dict[str, Any], cached: pd.DataFrame, bare_name: str, schema: str) -> Dict[str, Any]:
        qualified = self._qualify(schema, bare_name, payload.get("_backend"))
        return {
            "is_error": False,
            "message": (
                f"Cache hit for {payload['class_name']}."
                f"{payload['method_name']}; "
                f"reused generated table '{bare_name}'"
            ),
            "error_message": None,
            "involved_cols": [],
            "generated_cols": list(cached.columns),
            "result": cached,
            "new_table": bare_name,
            "result_metadata": {
                "from_cache": True,
                "saved_table": qualified,
                "row_count": len(cached),
                "column_count": len(cached.columns),
                "strict_args_kwargs_match": True,
            },
        }

    # ── decorator ──────────────────────────────────────────────────
    def __call__(self, func: Optional[Callable] = None, deep_cache: Optional[bool] = None):
        if func is not None:
            return self._make_decorator(func, deep_cache)
        return lambda f: self._make_decorator(f, deep_cache)

    def _make_writer(self, mf):
        async def writer(payload, data_id, generated_table_name=None, is_deep_cache=False, schema=None):
            await mf._arecord_method_call(
                data_id=data_id,
                class_name=payload["class_name"],
                method_name=payload["method_name"],
                args_sig=payload["args_sig"],
                kwargs_sig=payload["kwargs_sig"],
                generated_table_name=generated_table_name,
                is_deep_cache=is_deep_cache,
                schema=schema,
            )
        return writer

    def _make_decorator(self, func: Callable, decorator_deep_cache: Optional[bool]):
        manager = self

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                mf = getattr(self, "_memframe", None)
                if mf is None:
                    raise OperationError(
                        f"Cannot cache {func.__qualname__}: instance lacks `_memframe`. "
                        "Inherit from LoggableMixin or set `self._memframe`."
                    )

                data_id = getattr(self, "_data_id", None) or mf._active_id
                if not data_id:
                    raise OperationError(
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
                    writer = manager._make_writer(mf)
                    self._call_writer = writer

                payload = {
                    "class_name": self.__class__.__name__,
                    "method_name": func.__name__,
                    "args_sig": manager._signature(args),
                    "kwargs_sig": manager._signature(kwargs),
                    "_backend": backend,
                }

                # --- Lookup: only for deep cache entries ---
                if is_deep and backend:
                    method = f"{payload['class_name']}.{payload['method_name']}"
                    logger.debug(
                        "[cache] LOOKUP %s data_id=%s args=%s kwargs=%s",
                        method, data_id, payload["args_sig"], payload["kwargs_sig"],
                    )
                    lookup_start = time.perf_counter()
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
                           AND {backend.backend == "clickhouse" and "CAST(is_deep_cache AS UInt8) = 1" or "is_deep_cache = TRUE"}
                           AND generated_table_name IS NOT NULL
                        ORDER BY opidx DESC LIMIT 1
                        """,
                        data_id,
                        payload["class_name"],
                        payload["method_name"],
                        payload["args_sig"],
                        payload["kwargs_sig"],
                    )
                    if row:
                        bare, sch = row[0], row[1]
                        sch = sch or backend.transient_schema
                        cached = await manager._load_generated_table(backend, manager._qualify(sch, bare, backend))
                        if cached is not None:
                            logger.info(
                                "[cache] HIT %s data_id=%s → %s (%d rows, lookup+reload %.3fs)",
                                method, data_id, bare, len(cached),
                                time.perf_counter() - lookup_start,
                            )
                            return manager._make_hit_response(payload, cached, bare, sch)
                        logger.info(
                            "[cache] MISS %s data_id=%s — registry row found but reload failed for %s",
                            method, data_id, bare,
                        )
                    else:
                        logger.info(
                            "[cache] MISS %s data_id=%s — no matching registry row",
                            method, data_id,
                        )

                # --- Execute ---
                result = await func(self, *args, **kwargs)
                generated_table_name = None
                schema = None

                if is_deep and isinstance(result, dict) and not result.get("is_error", False):
                    bare_table = result.get("new_table") or result.get("generated_table_name")
                    if bare_table and backend:
                        if await backend.table_exists(manager._qualify(backend.transient_schema, bare_table, backend)):
                            generated_table_name = bare_table
                            schema = backend.transient_schema
                        elif await backend.table_exists(manager._qualify("transient", bare_table, backend)):
                            generated_table_name = bare_table
                            schema = "transient"
                        elif await backend.table_exists(manager._qualify(backend.upload_schema, bare_table, backend)):
                            be = getattr(backend, "backend", None)
                            if be == Backend.CLICKHOUSE:
                                await backend.execute(
                                    f"RENAME TABLE {manager._qualify(backend.upload_schema, bare_table, backend)} "
                                    f"TO {manager._qualify(backend.transient_schema, bare_table, backend)}"
                                )
                            elif be == Backend.DUCKDB:
                                await backend.execute(
                                    f"CREATE TABLE {manager._qualify(backend.transient_schema, bare_table, backend)} "
                                    f"AS SELECT * FROM {manager._qualify(backend.upload_schema, bare_table, backend)}"
                                )
                                await backend.execute(
                                    f"DROP TABLE {manager._qualify(backend.upload_schema, bare_table, backend)}"
                                )
                            else:
                                await backend.execute(
                                    f"ALTER TABLE {manager._qualify(backend.upload_schema, bare_table, backend)} "
                                    f"SET SCHEMA {backend.transient_schema}"
                                )
                            generated_table_name = bare_table
                            schema = backend.transient_schema
                    elif "result" in result:
                        df = result["result"]
                        if isinstance(df, pd.DataFrame) and not df.empty and backend:
                            qualified = await manager._create_deep_cache_table(
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
                            q = manager._qualify(sch, bare_table, backend)
                            if q and await backend.table_exists(q):
                                await backend.drop_table(q)
                                break

                await writer(payload, data_id, generated_table_name, is_deep_cache=is_deep, schema=schema)
                if is_deep and generated_table_name:
                    logger.info(
                        "[cache] STORE %s.%s data_id=%s → %s.%s (deep)",
                        payload["class_name"], payload["method_name"],
                        data_id, schema, generated_table_name,
                    )
                elif not is_deep:
                    logger.debug(
                        "[cache] STORE %s.%s data_id=%s (signature-only)",
                        payload["class_name"], payload["method_name"], data_id,
                    )
                return result
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                return func(self, *args, **kwargs)
            return sync_wrapper


record_call = CacheManager()
