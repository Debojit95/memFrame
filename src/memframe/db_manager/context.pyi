from __future__ import annotations

from typing import Any, Optional

from memframe.db_manager.adapters.base import DatabaseAdapter


class ContextManager:
    memframe: Any
    _data_id: Optional[str]
    _adapter: Optional[DatabaseAdapter]

    def __init__(self, memframe_instance: Any, data_id: Optional[str] = None) -> None: ...

    async def close(self) -> None: ...
    async def _ensure_adapter(self) -> None: ...
    async def _get_active_context(self) -> tuple: ...
