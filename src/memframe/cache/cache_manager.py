import asyncio
import base64
import datetime as _dt
import decimal
import hashlib
import inspect
import io
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from memframe.core.indexing import MF_IDX, strip_hidden
from memframe.core.ingestion.datatype_detector import Backend
from memframe.exceptions import OperationError

logger = logging.getLogger("memFrame.cache")


@asynccontextmanager
async def _no_lock():
    yield


class CacheManager:
    """Two-level method-call cache backed by the cache registry.

    Level 1 (default, deep_cache=False): records arg/kwarg signatures only —
    an audit/lineage log; repeat calls re-execute (no table to replay).
    Level 2 (deep, deep_cache=True): persists result DataFrames as typed
    cache tables; repeat calls load the table and skip execution.
    Resolution order: decorator arg -> MemFrame.deep_cache -> False.

    The instance itself is the ``record_call`` decorator:
        @record_call
        @record_call()
        @record_call(deep_cache=True)
    """

    # ── cache key (signature) ──────────────────────────────────────
    @staticmethod
    def _content_hash(o: Any) -> Optional[str]:
        """Full content hash so same-shape frames never collide in signatures."""
        try:
            raw = pd.util.hash_pandas_object(o, index=True).to_numpy().tobytes()
            return hashlib.sha256(raw).hexdigest()
        except Exception:
            return None  # ponytail: exotic dtypes fall back to shape-only

    @staticmethod
    def _self_key(instance: Any) -> Any:
        """Opt-in result-affecting instance state (e.g. groupby columns).

        Call arguments omit ``self``; a builder whose state changes the result
        (``GroupBy.group_cols``) implements ``cache_key()`` so two builders
        can't share one entry. Classes without it keep their existing key.
        """
        cache_key = getattr(instance, "cache_key", None)
        if not callable(cache_key):
            return None
        try:
            return cache_key()
        except Exception:
            return None

    @staticmethod
    def _json_default(o: Any) -> Any:
        if isinstance(o, np.generic):
            return o.item()
        if isinstance(o, np.ndarray):
            return o.tolist()
        if isinstance(o, pd.DataFrame):
            return {
                "__dataframe__": list(o.shape),
                "columns": list(o.columns),
                "hash": CacheManager._content_hash(o),
            }
        if isinstance(o, pd.Series):
            return {
                "__series__": list(o.shape),
                "name": str(o.name),
                "hash": CacheManager._content_hash(o),
            }
        # ponytail: structural key for arg objects (e.g. filter Predicates
        # compile to their SQL+params). The repr fallback below never falsely
        # matches distinct objects (worst case: extra MISSes, never wrong HITs).
        cache_key = getattr(o, "cache_key", None)
        if callable(cache_key):
            try:
                return {"__cache_key__": o.cache_key(), "__type__": type(o).__name__}
            except Exception:
                pass
        try:
            return {"__repr__": repr(o), "__type__": type(o).__name__}
        except Exception:
            return f"<{type(o).__name__}:{id(o)}>"

    def _signature(self, value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=self._json_default)

    # ── actual payloads ────────────────────────────────────────────
    @classmethod
    def _encode_value(cls, value: Any) -> Any:
        """Typed JSON-safe encoding for dict/list/scalar actual results."""
        if value is None or isinstance(value, (bool, str, int)):
            return value
        if isinstance(value, float):
            if math.isfinite(value):
                return value
            return {"__type__": "float", "value": repr(value)}
        if isinstance(value, np.generic):
            return cls._encode_value(value.item())
        if isinstance(value, np.ndarray):
            return {"__type__": "list", "items": [cls._encode_value(v) for v in value.tolist()]}
        if isinstance(value, pd.Timestamp):
            return {"__type__": "datetime", "value": value.isoformat()}
        if isinstance(value, pd.Timedelta):
            return {"__type__": "timedelta", "value": value.total_seconds()}
        if isinstance(value, _dt.datetime):
            return {"__type__": "datetime", "value": value.isoformat()}
        if isinstance(value, _dt.date):
            return {"__type__": "date", "value": value.isoformat()}
        if isinstance(value, _dt.time):
            return {"__type__": "time", "value": value.isoformat()}
        if isinstance(value, decimal.Decimal):
            return {"__type__": "decimal", "value": str(value)}
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {
                "__type__": "bytes",
                "value": base64.b64encode(bytes(value)).decode("ascii"),
            }
        if isinstance(value, dict):
            return {
                "__type__": "dict",
                "items": [
                    [cls._encode_value(k), cls._encode_value(v)]
                    for k, v in value.items()
                ],
            }
        if isinstance(value, (list, tuple, set)):
            return {
                "__type__": "list",
                "items": [cls._encode_value(v) for v in value],
            }
        if isinstance(value, pd.Series):
            return {
                "__type__": "series",
                "name": str(value.name) if value.name is not None else None,
                "items": [cls._encode_value(v) for v in value.tolist()],
            }
        try:
            return {"__type__": "repr", "value": repr(value)}
        except Exception:
            return {"__type__": "repr", "value": f"<{type(value).__name__}>"}

    @classmethod
    def _decode_value(cls, node: Any) -> Any:
        if not isinstance(node, dict) or "__type__" not in node:
            return node
        kind = node["__type__"]
        if kind == "float":
            return float(node["value"])
        if kind == "datetime":
            return _dt.datetime.fromisoformat(node["value"])
        if kind == "date":
            return _dt.date.fromisoformat(node["value"])
        if kind == "time":
            return _dt.time.fromisoformat(node["value"])
        if kind == "timedelta":
            return _dt.timedelta(seconds=float(node["value"]))
        if kind == "decimal":
            return decimal.Decimal(node["value"])
        if kind == "bytes":
            return base64.b64decode(node["value"])
        if kind == "dict":
            return {
                cls._decode_value(k): cls._decode_value(v)
                for k, v in node.get("items", [])
            }
        if kind == "list":
            return [cls._decode_value(v) for v in node.get("items", [])]
        if kind == "series":
            return pd.Series(
                [cls._decode_value(v) for v in node.get("items", [])],
                name=node.get("name"),
            )
        return node.get("value")

    @classmethod
    def _encode_actual(cls, value: Any) -> tuple[str, bytes]:
        if isinstance(value, pd.DataFrame):
            buf = io.BytesIO()
            value.to_parquet(buf, index=True)
            return "parquet", buf.getvalue()
        return "json", json.dumps(cls._encode_value(value)).encode("utf-8")

    @classmethod
    def _decode_actual(cls, fmt: str, data: bytes) -> Any:
        if fmt == "parquet":
            return pd.read_parquet(io.BytesIO(data))
        return cls._decode_value(json.loads(data.decode("utf-8")))

    @staticmethod
    def _payload_for_backend(data: bytes, backend) -> Any:
        if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
            return base64.b64encode(data).decode("ascii")
        return data

    @staticmethod
    def _payload_from_backend(data: Any, backend) -> bytes:
        if isinstance(data, memoryview):
            data = data.tobytes()
        if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
            if isinstance(data, str):
                data = data.encode("ascii")
            return base64.b64decode(data)
        return bytes(data)

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    @staticmethod
    def _quote_col(name: str, backend) -> str:
        safe = str(name).replace('"', '""').replace("`", "``")
        if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
            return f"`{safe}`"
        return f'"{safe}"'

    # ── validity epoch ─────────────────────────────────────────────
    async def _current_epoch(self, backend, data_id: str) -> int:
        try:
            val = await backend.fetchval(
                f"SELECT cache_epoch FROM {backend.csv_registry_table} "
                f"WHERE data_id = {backend.placeholder(1)}",
                data_id,
            )
            return int(val or 0)
        except Exception:
            return 0

    async def _bump_epoch(self, backend, data_id: str) -> int:
        try:
            await backend.execute(
                f"UPDATE {backend.csv_registry_table} "
                f"SET cache_epoch = COALESCE(cache_epoch, 0) + 1 "
                f"WHERE data_id = {backend.placeholder(1)}",
                data_id,
            )
        except Exception as exc:
            logger.warning("[cache] epoch bump failed for data_id=%s: %s", data_id, exc)
        return await self._current_epoch(backend, data_id)

    # ── ref results ────────────────────────────────────────────────
    def _ref_columns(self, result: Dict[str, Any]) -> List[str]:
        cols = result.get("result_cols")
        if isinstance(cols, list) and cols:
            return cols
        involved = result.get("involved_cols") or []
        generated = result.get("generated_cols") or result.get("new_columns") or []
        if isinstance(involved, str):
            involved = [involved]
        if isinstance(generated, str):
            generated = [generated]
        ordered: List[str] = []
        for col in list(involved) + list(generated):
            if col not in ordered:
                ordered.append(col)
        return ordered

    async def _load_ref_table(
        self, backend, schema: str, table: str, columns: List[str],
        order_schema: Optional[str] = None, order_table: Optional[str] = None,
        order_limit: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        if not columns:
            return None
        qualified = self._qualify(schema, table, backend)
        cols = ", ".join(self._quote_col(c, backend) for c in columns)
        try:
            if order_table:
                qualified_order = self._qualify(
                    order_schema or schema, order_table, backend
                )
                src_cols = ", ".join(
                    f"s.{self._quote_col(c, backend)}" for c in columns
                )
                sql = (
                    f"SELECT {src_cols} FROM {qualified} s "
                    f"JOIN {qualified_order} o "
                    f"ON s.{self._quote_col(MF_IDX, backend)} = "
                    f"o.{self._quote_col(MF_IDX, backend)} "
                    f'ORDER BY o."position"'
                )
                if order_limit is not None:
                    sql += f" LIMIT {int(order_limit)}"
            else:
                sql = f"SELECT {cols} FROM {qualified}"
            rows = await backend.fetch(sql)
            records = [dict(r) if hasattr(r, "keys") else r for r in rows]
            if records and isinstance(records[0], dict):
                return pd.DataFrame(records, columns=columns)
            return pd.DataFrame(rows, columns=columns)
        except Exception as exc:
            logger.warning("[cache] ref reload failed for %s: %s", qualified, exc)
            return None

    @staticmethod
    def _op_lock_for(backend, data_id: str):
        """Per-dataset lock, or a no-op when there is nothing to guard."""
        locked = getattr(backend, "_locked_op", None) if backend is not None else None
        if locked is None:
            return _no_lock()
        return locked(data_id)

    @staticmethod
    def _txn_for(backend):
        """Transaction block, or a no-op when the backend has none."""
        txn = getattr(backend, "transaction", None) if backend is not None else None
        if txn is None:
            return _no_lock()
        return txn()

    def _qualify(self, schema: str, bare: str, backend) -> str:
        if getattr(backend, "backend", None) == Backend.CLICKHOUSE:
            return f"`{schema}`.`{bare}`"
        return f'{schema}."{bare}"'

    async def _base_fingerprint(self, backend, table: str, schema: str) -> Optional[str]:
        """Row count + sorted column set of one table: validity stamp for deep entries."""
        try:
            be = getattr(backend, "backend", None)
            if be == Backend.CLICKHOUSE:
                qualified = f"`{schema}`.`{table}`"
                count = await backend.fetchval(f"SELECT COUNT(*) FROM {qualified}")
                col_rows = await backend.fetch(f"DESCRIBE TABLE {qualified}")
                cols = [r[0] for r in col_rows]
            else:
                qualified = f'{schema}."{table}"'
                count = await backend.fetchval(f"SELECT COUNT(*) FROM {qualified}")
                col_rows = await backend.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = {backend.placeholder(1)} "
                    f"AND table_name = {backend.placeholder(2)} "
                    "ORDER BY ordinal_position",
                    schema, table,
                )
                cols = [r[0] for r in col_rows]
            return f"{count}|{','.join(sorted(cols))}"
        except Exception as exc:
            logger.warning("[cache] fingerprint failed for %s.%s: %s", schema, table, exc)
            return None

    async def _active_context(self, backend, data_id: str, ops_parent):
        """Return (table, schema) of the table this op operates on."""
        try:
            if ops_parent is not None and hasattr(ops_parent, "_get_active_context"):
                return await ops_parent._get_active_context()
            row = await backend.fetch_row(
                f"SELECT table_name, schema FROM {backend.csv_registry_table} "
                f"WHERE data_id = {backend.placeholder(1)}",
                data_id,
            )
            if not row:
                return None, None
            return row[0], row[1] or backend.upload_schema
        except Exception:
            return None, None

    async def _read_fingerprint(self, backend, data_id: str, ops_parent) -> Optional[str]:
        """Fingerprint of the table this op reads (override-aware). None if unresolvable."""
        try:
            if ops_parent is not None and hasattr(ops_parent, "_get_active_context"):
                table, schema = await ops_parent._get_active_context()
            else:
                row = await backend.fetch_row(
                    f"SELECT table_name, schema FROM {backend.csv_registry_table} "
                    f"WHERE data_id = {backend.placeholder(1)}",
                    data_id,
                )
                if not row:
                    return None
                table, schema = row[0], row[1] or backend.upload_schema
            return await self._base_fingerprint(backend, table, schema)
        except Exception as exc:
            logger.warning("[cache] fingerprint failed for data_id=%s: %s", data_id, exc)
            return None

    # ── table load / store ─────────────────────────────────────────
    async def _load_generated_table(self, backend, qualified: str) -> Optional[pd.DataFrame]:
        start = time.perf_counter()
        try:
            # ponytail: no ORDER BY — the live path reads the same unordered
            # way, so replay matches live; don't "fix" one side only.
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

    def _make_hit_response(
        self, payload: Dict[str, Any], cached: Any,
        bare_name: Optional[str] = None, schema: Optional[str] = None,
        envelope: Optional[str] = None, kind: str = "ref",
    ) -> Dict[str, Any]:
        cached = strip_hidden(cached)
        stored: Dict[str, Any] = {}
        if envelope:
            try:
                parsed = json.loads(envelope)
                if isinstance(parsed, dict):
                    stored = parsed
            except Exception:
                stored = {}

        generated = stored.get("new_columns")
        if not isinstance(generated, list) or not generated:
            single = stored.get("new_column")
            generated = [single] if single else []

        row_count = None
        column_count = None
        if isinstance(cached, pd.DataFrame):
            row_count = len(cached)
            column_count = len(cached.columns)
        else:
            try:
                row_count = len(cached)
            except Exception:
                row_count = None

        if kind == "actual":
            response = {
                "is_error": False,
                "message": (
                    f"Cache hit for {payload['class_name']}."
                    f"{payload['method_name']}; replayed actual result"
                ),
                "error_message": None,
                **stored,
                "result": cached,
            }
            response.setdefault("involved_cols", [])
            response["generated_cols"] = list(
                stored.get("generated_cols") or generated
            )
            response.setdefault("new_columns", generated)
            response["result_metadata"] = {
                "from_cache": True,
                "cache_kind": "actual",
                "row_count": row_count,
                "column_count": column_count,
                "strict_args_kwargs_match": True,
            }
            return response

        qualified = self._qualify(schema, bare_name, payload.get("_backend"))
        if not generated:
            generated = list(cached.columns) if isinstance(cached, pd.DataFrame) else []

        response = {
            "is_error": False,
            "message": (
                f"Cache hit for {payload['class_name']}."
                f"{payload['method_name']}; "
                f"reused generated table '{bare_name}'"
            ),
            "error_message": None,
            **stored,
            "result": cached,
            "new_table": bare_name,
        }
        response.setdefault("involved_cols", [])
        response.setdefault("new_columns", generated)
        response["generated_cols"] = list(generated)
        response["result_metadata"] = {
            "from_cache": True,
            "cache_kind": "ref",
            "saved_table": qualified,
            "row_count": row_count,
            "column_count": column_count,
            "strict_args_kwargs_match": True,
        }
        return response

    # ── decorator ──────────────────────────────────────────────────
    def __call__(self, func: Optional[Callable] = None, deep_cache: Optional[bool] = None):
        if func is not None:
            return self._make_decorator(func, deep_cache)
        return lambda f: self._make_decorator(f, deep_cache)

    def _make_writer(self, mf):
        async def writer(payload, data_id, generated_table_name=None, is_deep_cache=False, schema=None,
                         table_scope=None, base_fingerprint=None, result_envelope=None,
                         cache_kind=None, ref_schema=None, ref_table=None,
                         involved_cols=None, generated_cols=None, result_cols=None,
                         owns_table=False, epoch=0, input_refs=None,
                         order_schema=None, order_table=None,
                         order_position_dtype=None, order_kind=None,
                         order_limit=None):
            return await mf._arecord_method_call(
                data_id=data_id,
                class_name=payload["class_name"],
                method_name=payload["method_name"],
                args_sig=payload["args_sig"],
                kwargs_sig=payload["kwargs_sig"],
                generated_table_name=generated_table_name,
                is_deep_cache=is_deep_cache,
                schema=schema,
                table_scope=table_scope,
                base_fingerprint=base_fingerprint,
                result_envelope=result_envelope,
                cache_kind=cache_kind,
                ref_schema=ref_schema,
                ref_table=ref_table,
                involved_cols=involved_cols,
                generated_cols=generated_cols,
                result_cols=result_cols,
                owns_table=owns_table,
                epoch=epoch,
                input_refs=input_refs,
                order_schema=order_schema,
                order_table=order_table,
                order_position_dtype=order_position_dtype,
                order_kind=order_kind,
                order_limit=order_limit,
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

                # Normalise positional/keyword/default differences so the
                # sync bridge, async callers, and direct calls share one key.
                state_key = manager._self_key(self)
                args_sig = manager._signature(args)
                kwargs_sig = manager._signature(kwargs)
                try:
                    bound = inspect.signature(func).bind(self, *args, **kwargs)
                    bound.apply_defaults()
                    arguments = dict(bound.arguments)
                    arguments.pop("self", None)
                    if state_key is not None:
                        arguments["__self__"] = state_key
                    args_sig = manager._signature(arguments)
                    kwargs_sig = ""
                except (TypeError, ValueError):
                    if state_key is not None:
                        kwargs_sig = manager._signature(
                            {"kwargs": kwargs, "__self__": state_key}
                        )

                payload = {
                    "class_name": self.__class__.__name__,
                    "method_name": func.__name__,
                    "args_sig": args_sig,
                    "kwargs_sig": kwargs_sig,
                    "_backend": backend,
                }

                # ponytail: hold the per-dataset lock across lookup+execute+store —
                # the MAX(opidx)+1 reads inside func() and the writer below must
                # not interleave with another op on this data_id (same opidx twice).
                async with manager._op_lock_for(backend, data_id):
                    # ponytail: merge/join outputs share the parent data_id — scope
                    # the key by override table or different results contaminate.
                    ops_parent = getattr(self, "_ops_parent", None)
                    scope = getattr(ops_parent, "_table_override", None) if ops_parent is not None else None
                    # --- Lookup: only for cached entries ---
                    if is_deep and backend:
                        method = f"{payload['class_name']}.{payload['method_name']}"
                        logger.debug(
                            "[cache] LOOKUP %s data_id=%s args=%s kwargs=%s",
                            method, data_id, payload["args_sig"], payload["kwargs_sig"],
                        )
                        lookup_start = time.perf_counter()
                        deep_cond = (
                            "CAST(is_deep_cache AS UInt8) = 1"
                            if backend.backend == "clickhouse"
                            else "is_deep_cache = TRUE"
                        )
                        row = await backend.fetch_row(
                            f"""
                            SELECT generated_table_name, schema, base_fingerprint, opidx,
                                   result_envelope, cache_kind, ref_schema, ref_table,
                                   involved_cols, generated_cols, result_cols, epoch,
                                   owns_table, order_schema, order_table,
                                   order_position_dtype, order_kind, order_limit
                            FROM {backend.cache_registry_table}
                            WHERE data_id = {backend.placeholder(1)}
                              AND operation_type = 'method_call'
                              AND class_name = {backend.placeholder(2)}
                              AND method_name = {backend.placeholder(3)}
                              AND args = {backend.placeholder(4)}
                              AND kwargs = {backend.placeholder(5)}
                              AND COALESCE(table_scope, '') = {backend.placeholder(6)}
                              AND ({deep_cond} OR cache_kind IS NOT NULL)
                            ORDER BY opidx DESC LIMIT 1
                            """,
                            data_id,
                            payload["class_name"],
                            payload["method_name"],
                            payload["args_sig"],
                            payload["kwargs_sig"],
                            scope or "",
                        )
                        if row:
                            bare, sch, stored_fp, opidx = row[0], row[1], row[2], row[3]
                            stored_envelope = row[4] if len(row) > 4 else None
                            stored_kind = row[5] if len(row) > 5 else None
                            ref_schema = row[6] if len(row) > 6 else None
                            ref_table = row[7] if len(row) > 7 else None
                            involved = manager._as_list(row[8] if len(row) > 8 else None)
                            generated = manager._as_list(row[9] if len(row) > 9 else None)
                            result_cols = manager._as_list(row[10] if len(row) > 10 else None)
                            stored_epoch = row[11] if len(row) > 11 else None
                            owns_table = bool(row[12]) if len(row) > 12 else False
                            order_schema = row[13] if len(row) > 13 else None
                            order_table = row[14] if len(row) > 14 else None
                            order_position_dtype = row[15] if len(row) > 15 else None
                            order_kind = row[16] if len(row) > 16 else None
                            order_limit = row[17] if len(row) > 17 else None
                            sch = sch or backend.cache_schema
                            current_fp = await manager._read_fingerprint(backend, data_id, ops_parent)
                            stale = False
                            if current_fp is None:
                                logger.info(
                                    "[cache] MISS %s data_id=%s — base unmeasurable",
                                    method, data_id,
                                )
                                row = None
                            elif stored_fp is not None and stored_fp != current_fp:
                                stale = True
                            elif stored_epoch is not None:
                                current_epoch = await manager._current_epoch(backend, data_id)
                                if current_epoch > int(stored_epoch):
                                    stale = True
                            if stale:
                                logger.info(
                                    "[cache] STALE %s data_id=%s — base changed; dropping entry",
                                    method, data_id,
                                )
                                await backend.execute(
                                    f"DELETE FROM {backend.cache_registry_table} "
                                    f"WHERE data_id = {backend.placeholder(1)} "
                                    f"AND opidx = {backend.placeholder(2)}",
                                    data_id, opidx,
                                )
                                await backend.execute(
                                    f"DELETE FROM {backend.cache_payloads_table} "
                                    f"WHERE data_id = {backend.placeholder(1)} "
                                    f"AND opidx = {backend.placeholder(2)}",
                                    data_id, opidx,
                                )
                                if order_table:
                                    try:
                                        await backend.drop_table(
                                            manager._qualify(
                                                order_schema or backend.cache_schema,
                                                order_table, backend,
                                            )
                                        )
                                    except Exception as exc:
                                        logger.warning(
                                            "[cache] stale order drop failed for %s: %s",
                                            order_table, exc,
                                        )
                                if stored_kind == "ref" and owns_table:
                                    try:
                                        await backend.drop_table(
                                            manager._qualify(
                                                ref_schema or sch, ref_table or bare, backend
                                            )
                                        )
                                    except Exception as exc:
                                        logger.warning(
                                            "[cache] stale table drop failed for %s: %s",
                                            ref_table or bare, exc,
                                        )
                                elif stored_kind is None and bare:
                                    try:
                                        await backend.drop_table(
                                            manager._qualify(sch, bare, backend)
                                        )
                                    except Exception as exc:
                                        logger.warning(
                                            "[cache] stale table drop failed for %s: %s",
                                            bare, exc,
                                        )
                                row = None
                        if row:
                            if stored_kind == "actual":
                                prow = await backend.fetch_row(
                                    f"SELECT format, payload FROM {backend.cache_payloads_table} "
                                    f"WHERE data_id = {backend.placeholder(1)} "
                                    f"AND opidx = {backend.placeholder(2)}",
                                    data_id, opidx,
                                )
                                if prow:
                                    try:
                                        cached = manager._decode_actual(
                                            prow[0],
                                            manager._payload_from_backend(prow[1], backend),
                                        )
                                    except Exception as exc:
                                        logger.warning(
                                            "[cache] actual payload decode failed for %s: %s",
                                            method, exc,
                                        )
                                        cached = None
                                    if cached is not None:
                                        logger.info(
                                            "[cache] HIT %s data_id=%s → actual payload (%.3fs)",
                                            method, data_id, time.perf_counter() - lookup_start,
                                        )
                                        return manager._make_hit_response(
                                            payload, cached, envelope=stored_envelope,
                                            kind="actual",
                                        )
                            elif stored_kind == "ref":
                                cols = result_cols or manager._ref_columns(
                                    {
                                        "involved_cols": involved,
                                        "generated_cols": generated,
                                    }
                                )
                                target_schema = ref_schema or sch
                                target_table = ref_table or bare
                                cached = await manager._load_ref_table(
                                    backend, target_schema, target_table, cols,
                                    order_schema=order_schema,
                                    order_table=order_table,
                                    order_limit=order_limit,
                                )
                                if cached is not None:
                                    logger.info(
                                        "[cache] HIT %s data_id=%s → %s.%s (%d rows, %.3fs)",
                                        method, data_id, target_schema, target_table,
                                        len(cached), time.perf_counter() - lookup_start,
                                    )
                                    return manager._make_hit_response(
                                        payload, cached, target_table, target_schema,
                                        stored_envelope, kind="ref",
                                    )
                            else:
                                bare, sch = row[0], row[1]
                                sch = sch or backend.cache_schema
                                cached = await manager._load_generated_table(
                                    backend, manager._qualify(sch, bare, backend)
                                )
                                if cached is not None:
                                    logger.info(
                                        "[cache] HIT %s data_id=%s → %s (%d rows, lookup+reload %.3fs)",
                                        method, data_id, bare, len(cached),
                                        time.perf_counter() - lookup_start,
                                    )
                                    return manager._make_hit_response(
                                        payload, cached, bare, sch, stored_envelope,
                                    )
                            logger.info(
                                "[cache] MISS %s data_id=%s — registry row found but reload failed",
                                method, data_id,
                            )
                        else:
                            logger.info(
                                "[cache] MISS %s data_id=%s — no usable registry row",
                                method, data_id,
                            )

                    # --- Execute ---
                    result = await func(self, *args, **kwargs)
                    generated_table_name = None
                    schema = None
                    cache_kind = None
                    ref_schema = None
                    ref_table = None
                    involved_cols = None
                    generated_cols = None
                    result_cols = None
                    owns_table = False
                    actual_format = None
                    actual_payload = None
                    epoch = 0
                    store_deep = is_deep
                    order_schema = None
                    order_table = None
                    order_position_dtype = None
                    order_kind = None
                    order_limit = None

                    is_error = isinstance(result, dict) and result.get("is_error", False)
                    annotated_kind = (
                        result.get("cache_kind") if isinstance(result, dict) else None
                    )
                    mutates = (
                        bool(result.get("mutates", False))
                        if isinstance(result, dict)
                        else False
                    )

                    # ponytail: one transaction — the stored table/payload and its
                    # registry row commit together, so a crash can't leave one
                    # without the other.
                    async with manager._txn_for(backend):
                        if backend:
                            if not is_error and annotated_kind is not None and mutates:
                                epoch = await manager._bump_epoch(backend, data_id)
                            else:
                                epoch = await manager._current_epoch(backend, data_id)

                        if is_deep and not is_error and isinstance(result, dict):
                            if annotated_kind == "actual":
                                try:
                                    actual_format, actual_payload = manager._encode_actual(
                                        result.get("result")
                                    )
                                    cache_kind = "actual"
                                except Exception as exc:
                                    logger.warning(
                                        "[cache] actual encode failed for %s: %s",
                                        payload["method_name"], exc,
                                    )
                                    store_deep = False
                                involved_cols = json.dumps(result.get("involved_cols") or [])
                                generated_cols = json.dumps(
                                    result.get("generated_cols")
                                    or result.get("new_columns")
                                    or []
                                )
                                result_cols = json.dumps(manager._ref_columns(result))
                            elif annotated_kind == "ref":
                                ref_schema = result.get("ref_schema")
                                ref_table = result.get("ref_table") or result.get("new_table")
                                if ref_table and backend:
                                    if not ref_schema:
                                        for cand in (backend.cache_schema, backend.upload_schema):
                                            if await backend.table_exists(
                                                manager._qualify(cand, ref_table, backend)
                                            ):
                                                ref_schema = cand
                                                break
                                    if ref_schema:
                                        cache_kind = "ref"
                                        involved_cols = json.dumps(
                                            result.get("involved_cols") or []
                                        )
                                        generated_cols = json.dumps(
                                            result.get("generated_cols")
                                            or result.get("new_columns")
                                            or []
                                        )
                                        result_cols = json.dumps(manager._ref_columns(result))
                                        owns_table = bool(result.get("owns_table", False))
                                        order_schema = result.get("order_schema")
                                        order_table = result.get("order_table")
                                        order_position_dtype = result.get("order_position_dtype")
                                        order_kind = result.get("order_kind")
                                        order_limit = result.get("order_limit")
                            else:
                                # Ops (datetime/transform/cumulative/window)
                                # that write into the active source table but do
                                # not yet declare cache_kind: infer a source ref.
                                bare_table = result.get("new_table") or result.get("generated_table_name")
                                src_table, src_schema = (
                                    await manager._active_context(backend, data_id, ops_parent)
                                    if backend else (None, None)
                                )
                                if bare_table and src_table and bare_table == src_table:
                                    cache_kind = "ref"
                                    ref_schema = src_schema
                                    ref_table = src_table
                                    involved_cols = json.dumps(result.get("involved_cols") or [])
                                    generated_cols = json.dumps(
                                        result.get("generated_cols")
                                        or result.get("new_columns")
                                        or []
                                    )
                                    result_cols = json.dumps(manager._ref_columns(result))
                                    owns_table = False
                                # Legacy unannotated path (removed once every op
                                # declares cache_kind).
                                elif bare_table and backend:
                                    if await backend.table_exists(manager._qualify(backend.cache_schema, bare_table, backend)):
                                        generated_table_name = bare_table
                                        schema = backend.cache_schema
                                    elif await backend.table_exists(manager._qualify(backend.upload_schema, bare_table, backend)):
                                        be = getattr(backend, "backend", None)
                                        if be == Backend.CLICKHOUSE:
                                            await backend.execute(
                                                f"RENAME TABLE {manager._qualify(backend.upload_schema, bare_table, backend)} "
                                                f"TO {manager._qualify(backend.cache_schema, bare_table, backend)}"
                                            )
                                        elif be == Backend.DUCKDB:
                                            await backend.execute(
                                                f"CREATE TABLE {manager._qualify(backend.cache_schema, bare_table, backend)} "
                                                f"AS SELECT * FROM {manager._qualify(backend.upload_schema, bare_table, backend)}"
                                            )
                                            await backend.execute(
                                                f"DROP TABLE {manager._qualify(backend.upload_schema, bare_table, backend)}"
                                            )
                                        else:
                                            await backend.execute(
                                                f"ALTER TABLE {manager._qualify(backend.upload_schema, bare_table, backend)} "
                                                f"SET SCHEMA {backend.cache_schema}"
                                            )
                                        generated_table_name = bare_table
                                        schema = backend.cache_schema
                                elif "result" in result:
                                    # No table holds this result → actual payload.
                                    try:
                                        actual_format, actual_payload = manager._encode_actual(
                                            result.get("result")
                                        )
                                        cache_kind = "actual"
                                        involved_cols = json.dumps(
                                            result.get("involved_cols") or []
                                        )
                                        generated_cols = json.dumps(
                                            result.get("generated_cols")
                                            or result.get("new_columns")
                                            or []
                                        )
                                        result_cols = json.dumps(manager._ref_columns(result))
                                    except Exception as exc:
                                        logger.warning(
                                            "[cache] actual encode failed for %s: %s",
                                            payload["method_name"], exc,
                                        )
                        elif (
                            not is_deep
                            and not is_error
                            and isinstance(result, dict)
                            and annotated_kind is None
                        ):
                            # ponytail: drop any table the method created — deep_cache=False means no tables
                            bare_table = result.get("new_table") or result.get("generated_table_name")
                            if bare_table and backend:
                                for sch in (backend.cache_schema, backend.upload_schema):
                                    q = manager._qualify(sch, bare_table, backend)
                                    if q and await backend.table_exists(q):
                                        await backend.drop_table(q)
                                        break

                    # ponytail: stamp the post-execute base state — a mutating op's
                    # repeat must see the mutated base, not the pre-state.
                    store_fp = None
                    if is_deep and backend:
                        store_fp = await manager._read_fingerprint(backend, data_id, ops_parent)

                    result_envelope = None
                    if is_deep and not is_error and isinstance(result, dict):
                        try:
                            result_envelope = json.dumps(
                                {
                                    k: v for k, v in result.items()
                                    if k not in (
                                        "result", "new_table", "result_metadata",
                                        "is_error", "message", "error_message",
                                        "cache_kind", "ref_schema", "ref_table",
                                        "owns_table", "result_cols",
                                        "order_schema", "order_table",
                                        "order_position_dtype", "order_kind",
                                        "order_limit",
                                    )
                                },
                                default=manager._json_default,
                            )
                        except Exception:
                            result_envelope = None

                    opidx = await writer(
                        payload, data_id, generated_table_name,
                        is_deep_cache=store_deep, schema=schema,
                        table_scope=scope, base_fingerprint=store_fp,
                        result_envelope=result_envelope,
                        cache_kind=cache_kind, ref_schema=ref_schema,
                        ref_table=ref_table, involved_cols=involved_cols,
                        generated_cols=generated_cols, result_cols=result_cols,
                        owns_table=owns_table, epoch=epoch,
                        order_schema=order_schema, order_table=order_table,
                        order_position_dtype=order_position_dtype,
                        order_kind=order_kind, order_limit=order_limit,
                    )
                    if cache_kind == "actual" and actual_payload is not None and backend:
                        try:
                            await backend.execute(
                                f"INSERT INTO {backend.cache_payloads_table} "
                                f"(data_id, opidx, format, payload) "
                                f"VALUES ({backend.placeholder(1)}, {backend.placeholder(2)}, "
                                f"{backend.placeholder(3)}, {backend.placeholder(4)})",
                                data_id, opidx, actual_format,
                                manager._payload_for_backend(actual_payload, backend),
                            )
                        except Exception as exc:
                            logger.warning(
                                "[cache] actual payload store failed for %s data_id=%s opidx=%s: %s",
                                payload["method_name"], data_id, opidx, exc,
                            )

                # Generated columns land on the physical base table; keep the
                # public view's column list in sync so raw SELECT * sees them.
                if (
                    backend
                    and not is_error
                    and isinstance(result, dict)
                    and not result.get("owns_table", False)
                    and (result.get("generated_cols") or result.get("new_columns"))
                ):
                    uploader = getattr(mf, "_uploader", None)
                    if uploader is not None:
                        try:
                            await uploader._refresh_public_view(data_id)
                        except Exception as exc:
                            logger.warning(
                                "[cache] public view refresh failed for data_id=%s: %s",
                                data_id, exc,
                            )

                if store_deep and cache_kind == "actual":
                    logger.info(
                        "[cache] STORE %s.%s data_id=%s (actual payload)",
                        payload["class_name"], payload["method_name"], data_id,
                    )
                elif store_deep and cache_kind == "ref":
                    logger.info(
                        "[cache] STORE %s.%s data_id=%s → %s.%s (ref)",
                        payload["class_name"], payload["method_name"],
                        data_id, ref_schema, ref_table,
                    )
                elif store_deep and generated_table_name:
                    logger.info(
                        "[cache] STORE %s.%s data_id=%s → %s.%s (deep)",
                        payload["class_name"], payload["method_name"],
                        data_id, schema, generated_table_name,
                    )
                elif not store_deep:
                    logger.debug(
                        "[cache] STORE %s.%s data_id=%s (signature-only)",
                        payload["class_name"], payload["method_name"], data_id,
                    )
                return result
            return async_wrapper
        else:
            logger.warning(
                "record_call applied to sync function %s; caching is only "
                "supported for coroutine functions",
                getattr(func, "__qualname__", func),
            )

            @wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                return func(self, *args, **kwargs)
            return sync_wrapper


record_call = CacheManager()
