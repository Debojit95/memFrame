import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from memframe.exceptions import DataNotFound
from memframe_ai.domain import build_domain_context

logger = logging.getLogger("memFrame")


@dataclass
class Session:
    """State for one chat conversation: adapter, active table, plots, settings.

    Bound to the caller's `ops` (ContextManager) and its parent MemFrame.
    The active table starts as the registered upload and advances as
    transform tools create transient tables.
    """

    session_id: str
    ops: Any
    memframe: Any
    settings: Any = None
    data_id: Optional[str] = None
    _adapter: Any = None
    _table: Optional[str] = None
    _schema: Optional[str] = None
    plots: dict = field(default_factory=dict)
    lock: Any = field(default_factory=asyncio.Lock)
    _context_cache: dict = field(default_factory=dict)
    _blocks: list = field(default_factory=list)
    _pinned_ctx: Optional[str] = None

    @property
    def blocks(self) -> list:
        return self._blocks

    def reset_blocks(self) -> None:
        self._blocks.clear()

    def record_block(self, block: dict) -> None:
        self._blocks.append(block)

    @property
    def backend(self) -> Any:
        return self.memframe._backend

    @property
    def adapter(self) -> Any:
        return self._adapter

    @property
    def table(self) -> Optional[str]:
        return self._table

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    async def ensure(self) -> "Session":
        """Resolve adapter + active table on first use."""
        if self._adapter is None:
            await self.ops._ensure_adapter()
            self._adapter = self.ops._adapter
        if self._table is None:
            self._table, self._schema = await self._resolve_active()
        return self

    async def _resolve_active(self) -> tuple[str, str]:
        backend = self.backend
        if backend is None:
            raise DataNotFound("Not connected. Call await mf.connect() first.")
        data_id = self.data_id or self.ops._data_id or self.memframe._active_id
        if not data_id:
            raise DataNotFound(
                "No active dataset. Upload data first (e.g. await mf.aupload_csv('file.csv'))."
            )
        rows = await backend.fetch(
            f"SELECT table_name FROM {backend.csv_registry_table} "
            f"WHERE data_id = {backend.placeholder(1)}",
            data_id,
        )
        if not rows:
            raise DataNotFound(f"No registered table for data_id {data_id}")
        return rows[0][0], backend.upload_schema

    def invalidate(self) -> None:
        self._table = None

    async def domain_context(self) -> str:
        """Return the domain context for the active table, computed once per chat.

        If a context was pinned for the current chat (:attr:`_pinned_ctx`), that
        precomputed string is returned so every agent in the chat sees the same
        context regardless of intermediate transient-table changes.
        """
        if self._pinned_ctx is not None:
            return self._pinned_ctx
        await self.ensure()
        key = (self._table, self._schema)
        ctx = self._context_cache.get(key)
        if ctx is None:
            ctx = await build_domain_context(self)
            self._context_cache[key] = ctx
            logger.info("domain_context built table=%s.%s chars=%d", self._schema, self._table, len(ctx))
        return ctx

    async def advance_table(self, new_table: str) -> None:
        """Move the active table to a transform result's transient table."""
        schema = self._schema or "transient"
        if self._adapter is not None and await self._adapter.table_exists(new_table, "transient"):
            schema = "transient"
        self._table, self._schema = new_table, schema

    def add_plot(self, plot_id: str, title: str, spec: dict, png: bytes) -> None:
        self.plots[plot_id] = {"id": plot_id, "title": title, "spec": spec, "png": png}

    def transform_kwargs(self) -> dict:
        data_id = self.data_id or self.ops._data_id or self.memframe._active_id
        return {"backend": self.backend, "data_id": data_id}


class SessionStore:
    def __init__(self):
        self._sessions: dict[str, Session] = {}

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def create(self, session_id: str, ops: Any, settings: Any = None) -> Session:
        session = Session(session_id=session_id, ops=ops, memframe=ops.memframe, settings=settings)
        self._sessions[session_id] = session
        return session

    def drop(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)


store = SessionStore()
