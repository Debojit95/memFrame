import logging
from typing import Any, Callable, Dict, Optional

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.adapters.factory import resolve_backend_config
from memframe.utils.async_sync import async_to_sync
from .pool import create_pool
from memframe.db_manager.setup import DatabaseBackend, create_backend

logger = logging.getLogger("memFrame")


class ConnectorManager:
    """Owns the database connection lifecycle: pool, backend and uploader.

    ``MemFrame`` holds one instance and delegates connect/close and the
    ``_backend``/``_pool``/``_uploader`` handles to it. ``context_factory``
    is the callback the uploader uses to build a ``ContextManager`` bound to
    the owning ``MemFrame``.
    """

    def __init__(
        self,
        connection_type: str,
        conn_params: Optional[Dict[str, Any]],
        context_factory: Callable[[str], Any],
    ):
        self.connection_type = connection_type
        self.conn_params = conn_params or {}
        self._backend: Optional[DatabaseBackend] = None
        self._pool = None
        self.__uploader = None
        self._context_factory = context_factory

    @property
    def backend(self) -> Optional[DatabaseBackend]:
        return self._backend

    @property
    def pool(self):
        return self._pool

    @property
    def _uploader(self):
        if self.__uploader is None:
            from memframe.core.ingestion.upload.base import Uploader

            u = Uploader()
            u._backend = self._backend
            u._type_detector = self._backend._type_detector if self._backend else None
            u._memframe_from_data_id = self._context_factory
            self.__uploader = u
        return self.__uploader

    def _placeholder(self, index: int) -> str:
        if self._backend is None:
            raise RuntimeError("Not connected.")
        return self._backend.placeholder(index)

    async def aconnect(self) -> None:
        backend_type, params = resolve_backend_config(self.connection_type, self.conn_params)
        self._pool = create_pool(backend_type, params)
        await self._pool.connect()
        self._backend = create_backend(backend_type, params)
        self._backend.pool = self._pool
        await self._backend.initialize()
        if self.__uploader:
            self.__uploader._backend = self._backend
            self.__uploader._type_detector = self._backend._type_detector

    async def aclose(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._backend = None

    @async_to_sync
    async def close(self) -> None:
        return await self.aclose()

    def is_duckdb(self) -> bool:
        return self._backend is not None and self._backend.backend == Backend.DUCKDB
