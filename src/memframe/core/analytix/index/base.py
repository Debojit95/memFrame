from typing import Any, Dict, List, Optional, Tuple, Union
import bisect
import json
import traceback
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


class DataIndexOps:
    """Metadata-only logical index over database tables.

    SQL tables have no pandas-style row labels, so the "index" is tracked as
    a JSON list of key columns in ``memframe_csv_registry.index_cols`` —
    never DDL. ``set_index``/``reset_index`` only read/write that metadata;
    ``reindex``/``reindex_like`` are read-time LEFT JOINs against the key.

    MultiIndex = composite key (``index_cols`` length > 1; labels are tuples).
    ffill/bfill use the portable islands idiom (``COUNT(col)`` + ``MAX(col)``
    windows) — exact on all three backends. ``nearest`` maps gap labels to
    the closest present key in Python (one DISTINCT query) then reuses the
    plain join path.

    # ponytail: want-lists are inlined as UNION ALL bind params; a 1M-label
    # reindex builds 1M SELECT arms — chunk the labels or stage a temp table
    # if that ever matters. Multi-key `nearest`, `tolerance`, and
    # limit-with-nearest fail loudly; add them when a real query needs them.
    """

    _METHODS = (None, "ffill", "pad", "bfill", "backfill", "nearest")

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        return await self.db.get_column_types(table, schema)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    def _q(self, name: str) -> str:
        return self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(name))

    def _success_response(self, message, result=None, involved_cols=None, **extra):
        return ok(message, involved_cols or [], None, result, **extra)

    def _error_response(self, msg, involved_cols=None):
        return fail(msg, involved_cols or [])

    def _key_kind(self, col_types: Dict[str, str], key: str) -> str:
        kind = (col_types.get(key) or "").lower()
        if any(t in kind for t in ("int", "float", "double", "decimal", "numeric", "real")):
            return "numeric"
        if "date" in kind or "time" in kind:
            return "temporal"
        return "other"

    # ------------------------------------------------------------------
    # Registry metadata I/O (backend is the DatabaseBackend, duck-typed)
    # ------------------------------------------------------------------
    async def _read_index_cols(self, backend, data_id: str) -> List[str]:
        try:
            row = await backend.fetchval(
                f"SELECT index_cols FROM {backend.csv_registry_table} "
                f"WHERE data_id = {backend.placeholder(1)}",
                data_id,
            )
        except Exception:
            return []  # ponytail: pre-migration registry has no column yet
        if not row:
            return []
        try:
            cols = json.loads(row)
            return [str(c) for c in cols] if isinstance(cols, list) else []
        except (json.JSONDecodeError, TypeError):
            return []

    async def _write_index_cols(self, backend, data_id: str, cols: Optional[List[str]]) -> None:
        value = json.dumps(cols) if cols else None
        await backend.execute(
            f"UPDATE {backend.csv_registry_table} "
            f"SET index_cols = {backend.placeholder(1)} "
            f"WHERE data_id = {backend.placeholder(2)}",
            value,
            data_id,
        )

    # ------------------------------------------------------------------
    # Input normalization
    # ------------------------------------------------------------------
    def _normalize_keys(self, keys, columns_alias) -> Union[List[str], dict]:
        raw = keys if keys is not None else columns_alias
        if raw is None:
            return {"error": "Provide `keys` (column name or list of column names)."}
        names = [raw] if isinstance(raw, str) else list(raw)
        if not names or not all(isinstance(c, str) for c in names):
            return {"error": "`keys` must be a column name or list of column names."}
        return list(dict.fromkeys(names))  # dedup, keep order

    @staticmethod
    def _is_monotonic(labels: list) -> Optional[bool]:
        """True if asc/desc (None entries ignored); None if uncomparable."""
        vals = [v for v in labels if v is not None]
        if len(vals) < 2:
            return True
        try:
            asc = all(a <= b for a, b in zip(vals, vals[1:]))
            desc = all(a >= b for a, b in zip(vals, vals[1:]))
        except TypeError:
            return None
        return asc or desc

    # ------------------------------------------------------------------
    # set_index / reset_index / get_index
    # ------------------------------------------------------------------
    async def set_index(
        self,
        table: str,
        schema: str,
        keys=None,
        *,
        columns=None,
        backend=None,
        data_id: Optional[str] = None,
        append: bool = False,
        drop: bool = True,
        verify_integrity: bool = False,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            names = self._normalize_keys(keys, columns)
            if isinstance(names, dict):
                return self._error_response(names["error"])
            safe = [SQLIdentifierSanitizer.sanitize(c) for c in names]
            available = await self._get_column_types(table, schema)
            missing = [c for c in safe if c not in available]
            if missing:
                return self._error_response(f"Columns not found: {missing}", safe)
            existing = await self._read_index_cols(backend, data_id)
            final = existing + [c for c in safe if c not in existing] if append else safe
            if verify_integrity:
                qualified = self._qualified_table(table, schema)
                key_list = ", ".join(self._q(c) for c in final)
                total = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}")
                distinct = await self._fetchval(
                    f"SELECT COUNT(*) FROM (SELECT DISTINCT {key_list} FROM {qualified}) s"
                )
                if total != distinct:
                    return self._error_response(
                        f"Index has duplicates: {total} rows but {distinct} distinct keys.",
                        final,
                    )
            await self._write_index_cols(backend, data_id, final)
            # ponytail: `drop` is recorded, not executed — key columns never
            # leave the table; future SELECT projections hide them when True.
            return self._success_response(
                f"Index set on {final}",
                result={"index_cols": final, "drop": drop, "append": append},
                involved_cols=final,
                index_cols=final,
            )
        except Exception as e:
            return self._error_response(f"set_index error: {e}\n{traceback.format_exc()}")

    async def reset_index(
        self,
        table: str,
        schema: str,
        *,
        backend=None,
        data_id: Optional[str] = None,
        level=None,
        drop: bool = False,
        names=None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            existing = await self._read_index_cols(backend, data_id)
            if not existing:
                return self._success_response(
                    "No index set; nothing to reset.",
                    result={"index_cols": []},
                    index_cols=[],
                )
            if names is not None:
                # ponytail: dict {old: new} only — str/list positional rename
                # is a second spelling for one ALTER; ask if you need it.
                mapping = {}
                if isinstance(names, dict):
                    mapping = names
                elif isinstance(names, str) and len(existing) == 1:
                    mapping = {existing[0]: names}
                elif isinstance(names, list) and len(names) == len(existing):
                    mapping = dict(zip(existing, names))
                else:
                    return self._error_response(
                        "`names` must be a {old: new} mapping (or a list matching index width).",
                        existing,
                    )
                qualified = self._qualified_table(table, schema)
                for old, new in mapping.items():
                    old_s = SQLIdentifierSanitizer.sanitize(old)
                    new_s = SQLIdentifierSanitizer.sanitize(new)
                    if old_s not in existing:
                        return self._error_response(f"Index level '{old}' not in {existing}.", existing)
                    await self._exec(
                        f"ALTER TABLE {qualified} RENAME COLUMN {self._q(old_s)} TO {self._q(new_s)}"
                    )
                existing = [mapping.get(c, c) for c in existing]
            if level is None:
                remaining: List[str] = []
            else:
                drop_levels = [level] if isinstance(level, str) else list(level)
                unknown = [c for c in drop_levels if c not in existing]
                if unknown:
                    return self._error_response(f"Level(s) not in index {existing}: {unknown}.", existing)
                remaining = [c for c in existing if c not in drop_levels]
            await self._write_index_cols(backend, data_id, remaining or None)
            return self._success_response(
                "Index reset." if not remaining else f"Index levels removed; remaining: {remaining}.",
                result={"index_cols": remaining, "drop": drop},
                involved_cols=existing,
                index_cols=remaining,
            )
        except Exception as e:
            return self._error_response(f"reset_index error: {e}\n{traceback.format_exc()}")

    async def get_index(
        self,
        table: str,
        schema: str,
        *,
        backend=None,
        data_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            key_cols = await self._read_index_cols(backend, data_id)
            qualified = self._qualified_table(table, schema)
            lim = f" LIMIT {int(limit)}" if limit else ""
            if not key_cols:
                total = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0
                n = min(total, int(limit)) if limit else total
                return self._success_response(
                    "Synthetic RangeIndex (no index set).",
                    result={"index_cols": [], "synthetic": True, "values": list(range(n))},
                    index_cols=[],
                )
            key_list = ", ".join(self._q(c) for c in key_cols)
            rows = await self._fetch(
                f"SELECT DISTINCT {key_list} FROM {qualified} ORDER BY {key_list}{lim}"
            )
            if len(key_cols) == 1:
                values: Any = [dict(r)[key_cols[0]] for r in rows]
            else:
                values = [tuple(dict(r)[c] for c in key_cols) for r in rows]
            return self._success_response(
                f"Index on {key_cols} ({len(values)} labels).",
                result={"index_cols": key_cols, "synthetic": False, "values": values},
                involved_cols=key_cols,
                index_cols=key_cols,
            )
        except Exception as e:
            return self._error_response(f"index error: {e}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # reindex
    # ------------------------------------------------------------------
    def _resolve_axes(self, labels, index, columns, axis):
        """Return (row_labels_or_None, col_labels_or_None, error_or_None)."""
        if axis is None:
            axis_name = "index"
        elif axis in (0, "index"):
            axis_name = "index"
        elif axis in (1, "columns"):
            axis_name = "columns"
        else:
            return None, None, "axis must be 0/'index' or 1/'columns'."
        row_labels, col_labels = index, columns
        if labels is not None:
            if index is not None or columns is not None:
                return None, None, "Pass either `labels`+`axis` or `index=`/`columns=`, not both."
            if axis_name == "index":
                row_labels = labels
            else:
                col_labels = labels
        return row_labels, col_labels, None

    def _want_union_sql(self, key_cols: List[str], labels: list) -> Tuple[str, list]:
        """UNION ALL derived table of wanted labels with ordinal; portable SQL."""
        params: list = []
        arms = []
        ph_idx = 0

        def _ph():
            nonlocal ph_idx
            ph_idx += 1
            return self.db.placeholder(ph_idx)

        for ord_, lab in enumerate(labels):
            vals = list(lab) if isinstance(lab, (list, tuple)) else [lab]
            terms = " , ".join(f"{_ph()} AS {self._q(k)}" for k in key_cols)
            terms += f", {_ph()} AS {self._q('__ord')}"
            params.extend(vals)
            params.append(ord_)
            arms.append(f"SELECT {terms}")
        return " UNION ALL ".join(arms), params

    def _join_cond(self, key_cols: List[str]) -> str:
        parts = []
        for k in key_cols:
            qk = self._q(k)
            parts.append(f"(w.{qk} = t.{qk} OR (w.{qk} IS NULL AND t.{qk} IS NULL))")
        return " AND ".join(parts)

    async def reindex(
        self,
        table: str,
        schema: str,
        labels=None,
        *,
        index=None,
        columns=None,
        axis=None,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
        backend=None,
        data_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            row_labels, col_labels, err = self._resolve_axes(labels, index, columns, axis)
            if err:
                return self._error_response(err)
            if row_labels is None and col_labels is None:
                return self._error_response("Provide new labels via `labels`, `index=`, or `columns=`.")
            if method not in self._METHODS:
                return self._error_response(
                    f"method must be one of ffill/pad, bfill/backfill, nearest (got {method!r})."
                )
            method = {"pad": "ffill", "backfill": "bfill"}.get(method, method)
            if method == "nearest" and limit is not None:
                return self._error_response("limit with method='nearest' is not supported yet.")
            if row_labels is not None:
                return await self._reindex_rows(
                    table, schema, row_labels, col_labels,
                    backend=backend, data_id=data_id,
                    method=method, fill_value=fill_value, limit=limit,
                )
            return await self._reindex_columns(table, schema, col_labels, fill_value=fill_value)
        except Exception as e:
            return self._error_response(f"reindex error: {e}\n{traceback.format_exc()}")

    async def _reindex_rows(
        self,
        table: str,
        schema: str,
        row_labels,
        col_labels,
        *,
        backend,
        data_id: str,
        method: Optional[str],
        fill_value: Any,
        limit: Optional[int],
    ) -> Dict[str, Any]:
        key_cols = await self._read_index_cols(backend, data_id)
        if not key_cols:
            return self._error_response("No index set. Call set_index(keys) first.")
        labels = list(row_labels) if isinstance(row_labels, (list, tuple)) else [row_labels]
        if not labels:
            return self._error_response("Row labels must be a non-empty list.")
        for lab in labels:
            vals = list(lab) if isinstance(lab, (list, tuple)) else [lab]
            if len(key_cols) > 1 and not isinstance(lab, (list, tuple)):
                return self._error_response(
                    f"MultiIndex needs tuple labels of width {len(key_cols)} (got {lab!r}).", key_cols
                )
            if len(vals) != len(key_cols):
                return self._error_response(
                    f"Label width {len(vals)} != index width {len(key_cols)}.", key_cols
                )
        if method is not None:
            mono = self._is_monotonic(
                [tuple(lab) if isinstance(lab, (list, tuple)) else lab for lab in labels]
            )
            if not mono:
                return self._error_response(
                    f"method={method!r} needs monotonically increasing/decreasing labels.", key_cols
                )
        if method == "nearest":
            if len(key_cols) > 1:
                return self._error_response("method='nearest' supports a single key column only.", key_cols)
            col_types = await self._get_column_types(table, schema)
            if self._key_kind(col_types, key_cols[0]) == "other":
                return self._error_response(
                    "method='nearest' needs a numeric or datetime key column.", key_cols
                )
            labels = await self._nearest_labels(table, schema, key_cols[0], labels)
            method = None  # gaps resolved to present keys; plain join finishes

        col_types = await self._get_column_types(table, schema)
        value_cols = [c for c in col_types if c not in key_cols]
        if col_labels is not None:
            wanted = [col_labels] if isinstance(col_labels, str) else list(col_labels)
            unknown = [c for c in wanted if c not in col_types]
            if unknown:
                return self._error_response(f"Columns not found: {unknown}.", key_cols)
            value_cols = [c for c in wanted if c not in key_cols]

        qualified = self._qualified_table(table, schema)
        want_sql, params = self._want_union_sql(key_cols, labels)
        join_cond = self._join_cond(key_cols)

        if method in ("ffill", "bfill"):
            df = await self._filled_select(
                want_sql, params, qualified, join_cond, key_cols, value_cols,
                method=method, limit=limit, fill_value=fill_value,
            )
        else:
            if limit is not None:
                return self._error_response("`limit` needs a fill method (ffill/bfill).", key_cols)
            df = await self._plain_select(
                want_sql, params, qualified, join_cond, key_cols, value_cols,
                fill_value=fill_value,
            )

        if col_labels is not None:  # column projection in requested order
            wanted = [col_labels] if isinstance(col_labels, str) else list(col_labels)
            keep = [c for c in wanted if c in df.columns]
            df = df[keep]
        return self._success_response(
            f"Reindexed to {len(labels)} labels (method={method}).",
            result=df,
            involved_cols=key_cols,
            index_cols=key_cols,
            method=method,
        )

    async def _plain_select(
        self, want_sql, params, qualified, join_cond, key_cols, value_cols,
        *, fill_value: Any,
    ) -> pd.DataFrame:
        """LEFT JOIN against want-list; gaps → fill_value (or NULL). Read-only."""
        key_select = ", ".join(f"w.{self._q(k)} AS {self._q(k)}" for k in key_cols)
        if fill_value is None:
            val_select = ", ".join(f"t.{self._q(c)} AS {self._q(c)}" for c in value_cols)
        else:
            # ponytail: one bind param per filled column; same value repeated.
            val_terms, all_params = [], list(params)
            for c in value_cols:
                all_params.append(fill_value)
                val_terms.append(
                    f"COALESCE(t.{self._q(c)}, {self.db.placeholder(len(all_params))}) AS {self._q(c)}"
                )
            params = all_params
            val_select = ", ".join(val_terms) if val_terms else "1 AS __dummy"
        select_list = key_select + (", " + val_select if val_select else "")
        rows = await self._fetch(
            f"WITH want AS ({want_sql}) "
            f"SELECT {select_list} FROM want w LEFT JOIN {qualified} t ON {join_cond} "
            f"ORDER BY w.{self._q('__ord')}",
            *params,
        )
        return pd.DataFrame([dict(r) for r in rows])

    async def _filled_select(
        self, want_sql, params, qualified, join_cond, key_cols, value_cols,
        *, method: str, limit: Optional[int], fill_value: Any,
    ) -> pd.DataFrame:
        """LEFT JOIN + islands fill. Islands are computed on RAW join output so
        a fill_value never becomes an island anchor; fill_value (if any) is
        applied last to still-missing cells, matching pandas order."""
        order = self._q("__ord")
        desc = "DESC" if method == "bfill" else "ASC"
        params = list(params)
        raw_select = (
            ", ".join(f"w.{self._q(k)} AS {self._q(k)}" for k in key_cols)
            + "".join(f", t.{self._q(c)} AS {self._q(c)}" for c in value_cols)
            + f", w.{order} AS {order}"
        )
        isl_terms = [
            f"COUNT(j.{self._q(c)}) OVER (ORDER BY j.{order} {desc} "
            f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {self._q(f'__mf_isl_{i}')}"
            for i, c in enumerate(value_cols)
        ]
        pos_terms = [
            f"ROW_NUMBER() OVER (PARTITION BY s.{self._q(f'__mf_isl_{i}')} "
            f"ORDER BY s.{order} {desc}) AS {self._q(f'__mf_pos_{i}')}"
            for i in range(len(value_cols))
        ]
        fill_terms = [
            f"MAX(p.{self._q(c)}) OVER (PARTITION BY p.{self._q(f'__mf_isl_{i}')}) "
            f"AS {self._q(f'__mf_fill_{i}')}"
            for i, c in enumerate(value_cols)
        ]
        final_terms = []
        for i, c in enumerate(value_cols):
            q_fill = self._q(f"__mf_fill_{i}")
            expr = f"f.{q_fill}"
            if limit is not None:
                expr = (
                    f"CASE WHEN f.{self._q(f'__mf_pos_{i}')} <= {int(limit) + 1} "
                    f"THEN f.{q_fill} ELSE f.{self._q(c)} END"
                )
            if fill_value is not None:
                params.append(fill_value)
                expr = f"COALESCE({expr}, {self.db.placeholder(len(params))})"
            final_terms.append(f"{expr} AS {self._q(c)}")
        key_final = ", ".join(f"f.{self._q(k)} AS {self._q(k)}" for k in key_cols)
        val_final = (", " + ", ".join(final_terms)) if final_terms else ""
        sql = (
            f"WITH want AS ({want_sql}), "
            f"joined AS (SELECT {raw_select} FROM want w LEFT JOIN {qualified} t ON {join_cond}), "
            f"isl AS (SELECT j.*, {', '.join(isl_terms)} FROM joined j), "
            f"pos AS (SELECT s.*, {', '.join(pos_terms)} FROM isl s), "
            f"filled AS (SELECT p.*, {', '.join(fill_terms)} FROM pos p) "
            f"SELECT {key_final}{val_final} FROM filled f ORDER BY f.{order} ASC"
        )
        rows = await self._fetch(sql, *params)
        return pd.DataFrame([dict(r) for r in rows])

    async def _nearest_labels(
        self, table: str, schema: str, key: str, labels: list,
    ) -> list:
        """Rewrite each gap label to the closest present key (bisect, Python-side).

        One DISTINCT query; pandas-comparable picks; ties go to the predecessor
        (matches pandas 'nearest' tie-break on sorted input).
        """
        qualified = self._qualified_table(table, schema)
        rows = await self._fetch(
            f"SELECT DISTINCT {self._q(key)} AS k FROM {qualified} "
            f"WHERE {self._q(key)} IS NOT NULL ORDER BY k"
        )
        present = [dict(r)["k"] for r in rows]
        if not present:
            raise ValueError("method='nearest' needs at least one present key value.")
        try:
            probe = [float(v) if v is not None else None for v in present]
        except (TypeError, ValueError):
            import pandas as _pd

            probe = [(_pd.Timestamp(v).value if v is not None else None) for v in present]
        hits = {v for v in present}
        out = []
        for lab in labels:
            if lab in hits:
                out.append(lab)
                continue
            try:
                x = float(lab)
            except (TypeError, ValueError):
                import pandas as _pd

                x = _pd.Timestamp(lab).value
            vals = [p for p in probe if p is not None]
            i = bisect.bisect_left(vals, x)
            if i == 0:
                out.append(present[0])
            elif i >= len(vals):
                out.append(present[-1])
            else:
                out.append(present[i - 1] if abs(x - vals[i - 1]) <= abs(vals[i] - x) else present[i])
        return out

    async def _reindex_columns(
        self, table: str, schema: str, col_labels, *, fill_value: Any,
    ) -> Dict[str, Any]:
        try:
            wanted = [col_labels] if isinstance(col_labels, str) else list(col_labels)
            if not wanted:
                return self._error_response("Column labels must be non-empty.")
            col_types = await self._get_column_types(table, schema)
            qualified = self._qualified_table(table, schema)
            terms, params = [], []
            for c in wanted:
                cs = SQLIdentifierSanitizer.sanitize(c)
                if cs in col_types:
                    terms.append(f"t.{self._q(cs)} AS {self._q(cs)}")
                elif fill_value is None:
                    terms.append(f"CAST(NULL AS VARCHAR) AS {self._q(cs)}")
                else:
                    params.append(fill_value)
                    terms.append(f"{self.db.placeholder(len(params))} AS {self._q(cs)}")
            rows = await self._fetch(f"SELECT {', '.join(terms)} FROM {qualified} t", *params)
            df = pd.DataFrame([dict(r) for r in rows])
            return self._success_response(
                f"Reindexed columns to {wanted}.", result=df, involved_cols=wanted
            )
        except Exception as e:
            return self._error_response(f"reindex columns error: {e}\n{traceback.format_exc()}")

    async def reindex_like(
        self,
        table: str,
        schema: str,
        other: str,
        *,
        backend=None,
        data_id: Optional[str] = None,
        method: Optional[str] = None,
        fill_value: Any = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Conform to another dataset's index + columns (thin over reindex)."""
        try:
            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")
            if not isinstance(other, str) or not other:
                return self._error_response("`other` must be the other dataset's data_id string.")
            if method not in self._METHODS:
                return self._error_response(f"method must be one of ffill/pad, bfill/backfill, nearest (got {method!r}).")
            method = {"pad": "ffill", "backfill": "bfill"}.get(method, method)
            reg = backend.csv_registry_table
            ph = backend.placeholder
            rows = await backend.fetch(
                f"SELECT table_name, schema, index_cols FROM {reg} WHERE data_id = {ph(1)}",
                other,
            )
            row = rows[0] if rows else None
            if not row:
                return self._error_response(f"No registry entry for other data_id '{other}'.")
            other_table, other_schema, other_index_json = row[0], row[1], row[2]
            try:
                other_keys = [str(c) for c in json.loads(other_index_json)] if other_index_json else []
            except (json.JSONDecodeError, TypeError):
                other_keys = []
            if not other_keys:
                return self._error_response(f"Other dataset '{other}' has no index set.")
            other_schema = other_schema or getattr(backend, "upload_schema", None)
            my_keys = await self._read_index_cols(backend, data_id)
            if other_keys != my_keys:
                return self._error_response(
                    f"Index mismatch: self {my_keys} vs other {other_keys} "
                    f"(same key columns required).",
                    my_keys,
                )
            other_types = await self._get_column_types(other_table, other_schema)
            other_qualified = self._qualified_table(other_table, other_schema)
            key_list = ", ".join(self._q(c) for c in my_keys)
            key_rows = await self._fetch(
                f"SELECT DISTINCT {key_list} FROM {other_qualified} ORDER BY {key_list}"
            )
            if len(my_keys) == 1:
                other_labels = [dict(r)[my_keys[0]] for r in key_rows]
            else:
                other_labels = [tuple(dict(r)[c] for c in my_keys) for r in key_rows]
            return await self._reindex_rows(
                table, schema, other_labels, list(other_types.keys()),
                backend=backend, data_id=data_id, method=method,
                fill_value=fill_value, limit=limit,
            )
        except Exception as e:
            return self._error_response(f"reindex_like error: {e}\n{traceback.format_exc()}")
