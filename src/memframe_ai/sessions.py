import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from memframe.exceptions import DataNotFound
from memframe_ai.domain import build_domain_context

logger = logging.getLogger("memframe.ai")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(_handler)


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
    _subquery_results: list = field(default_factory=list)
    # ponytail: full-df stash for the USER (all rows). The model still only
    # sees a capped sample via normalize's df_to_records payload.
    _results: list = field(default_factory=list)
    _context_cache: Optional[str] = None
    _context_version: int = 0
    _pinned: Optional[tuple] = None

    @property
    def subquery_results(self) -> list:
        return self._subquery_results

    def reset_subquery_results(self) -> None:
        self._subquery_results.clear()

    def record_subquery_result(self, label: str, payload: dict) -> None:
        self._subquery_results.append((label, payload))

    def reset_results(self) -> None:
        self._results.clear()

    @property
    def results(self) -> list:
        return self._results

    def add_result(self, df) -> None:
        self._results.append(df)

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

    @property
    def wrappers(self):
        """Public wrappers bound to this session, one per domain.

        Built lazily and cached on the session. AI tool files dispatch
        through these wrappers instead of reaching into core.analytix.
        """
        wrappers = getattr(self, "_wrappers", None)
        if wrappers is None:
            from memframe_ai.wrappers import SessionWrappers

            wrappers = SessionWrappers(self)
            self._wrappers = wrappers
        return wrappers

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
        self._context_cache = None
        self._context_version += 1

    async def domain_context(self, force_refresh: bool = False, lightweight: bool = False) -> str:
        """Return the domain context for the current active table.

        The context is cached and reused unless the table changes or force_refresh is True.
        This avoids regenerating the same context for unchanged tables.
        
        Args:
            force_refresh: If True, rebuilds the context even if cached.
            lightweight: If True, returns minimal context (column names + types only).
        """
        await self.ensure()
        
        # Check if we have a cached context and it's still valid
        cache_key = f"{'light' if lightweight else 'full'}_context"
        if not force_refresh and hasattr(self, cache_key) and getattr(self, cache_key) is not None:
            return getattr(self, cache_key)
        
        # Build fresh context
        ctx = await build_domain_context(self, lightweight=lightweight)
        setattr(self, cache_key, ctx)
        logger.info("domain_context built table=%s.%s chars=%d lightweight=%s", self._schema, self._table, len(ctx), lightweight)
        return ctx

    async def advance_table(self, new_table: str) -> None:
        """Move the active table to a transform result's transient table.

        When the session is pinned (during chat sub-query execution), the
        active table stays constant — the one from ctx.chat(). Transform
        results still land in persistent transient tables, but the session
        reference never drifts, so every specialist sees the same schema.
        """
        if self._pinned is not None:
            return
        schema = self._schema or "transient"
        if self._adapter is not None and await self._adapter.table_exists(new_table, "transient"):
            schema = "transient"
        self._table, self._schema = new_table, schema
        # Invalidate context cache when table changes
        self._context_cache = None

    def pin_table(self) -> None:
        """Pin the active table so advance_table becomes a no-op."""
        self._pinned = (self._table, self._schema)

    def unpin_table(self) -> None:
        """Restore advance_table behavior."""
        self._pinned = None

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
