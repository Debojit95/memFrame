from typing import Any, List, Optional, Tuple, Union
from core.analytix.selection import DataSelectionOps
from utils.method_call_logger import record_call


class SelectionOrchestrator:
    """
    Pandas-like selection API.
    Access: ctx.selection.asof(...), ctx.selection.loc(...), etc.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance          # ContextManager
        self._memframe = memframe_ops_instance.memframe   # MemFrame instance
        self._data_id = memframe_ops_instance._data_id
        self._selection_ops = None


    async def _ensure_ops(self) -> DataSelectionOps:
        if self._selection_ops is None:
            await self._ops_parent._ensure_adapter()
            self._selection_ops = DataSelectionOps(self._ops_parent._adapter)
        return self._selection_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    # ── Scalar / read‑only (no method call logging) ──────────────────
    async def asof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        return await ops.asof(
            table=table, schema=schema,
            where=where, on=on, subset=subset,
            backend=backend, data_id=data_id, chunk_size=chunk_size,
        )

    async def at(
        self,
        row_label,
        column_label: str,
        index_column: str = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.at(
            table=table, schema=schema,
            row_label=row_label,
            column_label=column_label,
            index_column=index_column,
        )

    async def iat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.iat(
            table=table, schema=schema,
            row_position=row_position,
            column_label=column_label,
            order_by=order_by,
        )

    async def get(
        self,
        keys: Union[str, List[str]],
        default: Any = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.get(
            table=table, schema=schema,
            keys=keys,
            default=default,
        )

    # ── DataFrame-returning methods ──────────────────────────────────
    async def loc(
        self,
        row_selector,
        columns=None,
        index_column: str = None,
        chunk_size: int = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if chunk_size is not None:
            return {
                "is_error": True,
                "message": "",
                "error_message": "chunk_size is not supported for loc with iloc-style row indexers.",
            }

        if isinstance(row_selector, tuple):
            if columns is not None:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "Pass either tuple row_selector=(rows, columns) or columns separately, not both.",
                }
            if len(row_selector) != 2:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "Tuple row_selector for loc must have exactly two items: (row_indexer, columns).",
                }
            row_selector, columns = row_selector

        row_indexer = self._parse_slice_text(row_selector)
        col_indexer = None
        if columns in (None, "*", ["*"], ("*",)):
            col_indexer = None
        elif isinstance(columns, str):
            return {
                "is_error": True,
                "message": "",
                "error_message": "columns must be a list of column names or '*'.",
            }
        elif isinstance(columns, (list, tuple)):
            if not columns:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "columns list cannot be empty.",
                }
            if not all(isinstance(c, str) for c in columns):
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "columns must be a list of column names or '*'.",
                }
            try:
                col_indexer = await self._column_names_to_positions(
                    ops=ops, table=table, schema=schema, columns=list(columns),
                )
            except ValueError as exc:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": str(exc),
                }
        else:
            return {
                "is_error": True,
                "message": "",
                "error_message": "columns must be a list of column names or '*'.",
            }

        result = await ops.iloc(
            table=table, schema=schema,
            row_indexer=row_indexer, col_indexer=col_indexer,
            backend=backend, data_id=data_id,
        )

        if index_column is not None and not result.get("is_error"):
            result["index_column_ignored"] = index_column

        return result

    @record_call
    async def where(
        self,
        cond: str,
        other: Optional[Any] = None,
        chunk_size: int = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        result = await ops.where(
            table=table, schema=schema,
            cond=cond, other=other,
            backend=backend, data_id=data_id, chunk_size=chunk_size,
        )
        return result

    @record_call
    async def select_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        result = await ops.select_dtypes(
            table=table, schema=schema,
            include=include, exclude=exclude,
            backend=backend, data_id=data_id, chunk_size=chunk_size,
        )
        return result

    async def iloc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = None,
        col_indexer: Union[int, List[int], slice, list, str] = None,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(row_indexer, tuple):
            if col_indexer is not None:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "Pass either tuple row_indexer=(rows, cols) or col_indexer separately, not both.",
                }
            if len(row_indexer) != 2:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "Tuple row_indexer for iloc must have exactly two items: (row_indexer, col_indexer).",
                }
            row_indexer, col_indexer = row_indexer

        row_indexer = self._parse_slice_text(row_indexer)
        col_indexer = self._parse_slice_text(col_indexer)

        if columns is not None and col_indexer is not None:
            return {
                "is_error": True,
                "message": "",
                "error_message": "Use either `col_indexer` or `columns`, not both.",
            }

        try:
            if columns is not None:
                col_indexer = await self._column_names_to_positions(
                    ops=ops, table=table, schema=schema, columns=columns,
                )
            elif isinstance(col_indexer, str) and ":" not in col_indexer:
                col_indexer = await self._column_names_to_positions(
                    ops=ops, table=table, schema=schema, columns=[col_indexer],
                )
            elif isinstance(col_indexer, (list, tuple)):
                has_str = any(isinstance(c, str) for c in col_indexer)
                if has_str:
                    if not all(isinstance(c, str) for c in col_indexer):
                        return {
                            "is_error": True,
                            "message": "",
                            "error_message": "When passing column names in col_indexer, all entries must be strings.",
                        }
                    col_indexer = await self._column_names_to_positions(
                        ops=ops, table=table, schema=schema, columns=list(col_indexer),
                    )
        except ValueError as exc:
            return {
                "is_error": True,
                "message": "",
                "error_message": str(exc),
            }

        result = await ops.iloc(
            table=table, schema=schema,
            row_indexer=row_indexer, col_indexer=col_indexer,
            backend=backend, data_id=data_id,
        )
        return result

    @record_call
    async def take(
        self,
        indices: List[int],
        axis: int = 0,
    ):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        result = await ops.take(
            table=table, schema=schema,
            indices=indices, axis=axis,
            backend=backend, data_id=data_id,
        )
        return result

    # ── helpers unchanged from original ────────────────────────────────
    @staticmethod
    def _parse_slice_text(indexer):
        if not isinstance(indexer, str) or ":" not in indexer:
            return indexer
        parts = indexer.split(":")
        if len(parts) > 3:
            raise ValueError(f"Invalid slice expression: '{indexer}'")
        parsed = []
        for p in parts:
            p = p.strip()
            parsed.append(None if p == "" else int(p))
        while len(parsed) < 3:
            parsed.append(None)
        return slice(parsed[0], parsed[1], parsed[2])

    async def _column_names_to_positions(
        self,
        ops: DataSelectionOps,
        table: str,
        schema: str,
        columns: Union[str, List[str], Tuple[str, ...]],
    ) -> List[int]:
        if isinstance(columns, str):
            columns = [columns]
        all_cols = await ops._get_all_columns(table, schema)
        pos_map = {c: i for i, c in enumerate(all_cols)}
        missing = [c for c in columns if c not in pos_map]
        if missing:
            raise ValueError(f"Unknown column(s): {missing}")
        return [pos_map[c] for c in columns]


# Alias for convenience
SelectionAccessor = SelectionOrchestrator
