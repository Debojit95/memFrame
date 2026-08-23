import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import pandas as pd

from memframe.db_manager.connection import ConnectorManager
from memframe.db_manager.context import ContextManager
from memframe.db_manager.ops import OpsMixin
from memframe.db_manager.setup import DatabaseBackend
from memframe.exceptions import ConnectionNotReady, ConfigurationError
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class MemFrame(ContextManager, OpsMixin):
    def __init__(self, connection_type: str = "local", connection_params: Optional[Dict[str, Any]] = None, deep_cache: Optional[bool] = None):
        super().__init__(self)
        self.deep_cache = deep_cache
        self._active_id: Optional[str] = None
        self._connector = ConnectorManager(
            connection_type,
            connection_params,
            context_factory=lambda data_id: ContextManager(self, data_id=data_id),
        )

    @property
    def _backend(self) -> Optional[DatabaseBackend]:
        return self._connector._backend

    @property
    def _pool(self):
        return self._connector.pool

    @property
    def _uploader(self):
        return self._connector._uploader

    # ── connect ─────────────────────────────────────────────────────

    async def aconnect(self) -> None:
        await self._connector.aconnect()

    @async_to_sync
    async def connect(self) -> None:
        return await self.aconnect()

    # ── AI agent ─────────────────────────────────────────────────────

    async def aenable_agent(
        self,
        api_key: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        **overrides,
    ):
        """Configure the optional AI agent layer.

        Idempotent — calling again replaces the settings on this
        ``MemFrame`` and every dataset context bound to it. ``provider``
        and ``model`` default to the values in
        :class:`memframe_ai.config.AISettings`. ``api_key`` is required
        and can be passed positionally or by keyword.
        """
        from memframe_ai.config import AISettings

        kwargs: dict[str, Any] = {}
        if api_key is not None:
            kwargs["api_key"] = api_key
        if provider is not None:
            kwargs["provider"] = provider
        if model is not None:
            kwargs["model"] = model
        kwargs.update(overrides)
        self._ai_settings = AISettings(**kwargs)
        from memframe_ai.instrument import configure_logfire

        configure_logfire(self._ai_settings)
        return self._ai_settings

    @async_to_sync
    async def enable_agent(
        self,
        api_key: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        **overrides,
    ):
        """Synchronous form of :meth:`aenable_agent`."""
        return await self.aenable_agent(
            api_key=api_key, provider=provider, model=model, **overrides
        )

    # ── upload ──────────────────────────────────────────────────

    def _placeholder(self, index: int) -> str:
        return self._connector._placeholder(index)

    async def aupload_csv(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_csv(file_path, dtypes=dtypes)

    @async_to_sync
    async def upload_csv(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_csv(file_path, dtypes=dtypes)

    async def aupload_parquet(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_parquet(file_path, dtypes=dtypes)

    @async_to_sync
    async def upload_parquet(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_parquet(file_path, dtypes=dtypes)

    async def aupload_df(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_df(df, filename, dtypes=dtypes)

    @async_to_sync
    async def upload_df(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_df(df, filename=filename, dtypes=dtypes)

    # ── ops / context helpers ────────────────────────────────────

    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ):
        if data_id is not None and data is not None:
            raise ConfigurationError("Pass either `data_id` or `data`, not both.")
        if data is None and data_id is not None and not isinstance(data_id, str):
            data = data_id
            data_id = None
        if data is not None:
            try:
                import pandas as pd
            except ImportError as exc:
                raise ImportError(
                    "ops(data=...) requires pandas for DataFrame conversion."
                ) from exc
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)
            uploaded = self.upload_df(df)
            if isinstance(uploaded, ContextManager):
                return uploaded
            data_id = uploaded
        return ContextManager(self, data_id=data_id)

    def _local_db_path(self) -> Optional[Path]:
        if not self._connector.is_duckdb():
            raise ConnectionNotReady("Local DuckDB connection is not active.")
        db_path = self._backend.conn_params.get("db_path", "memframe_new.duckdb")
        if db_path == ":memory:":
            return None
        return Path(db_path)

    async def aclose(self) -> None:
        await super().aclose()
        await self._connector.aclose()

    @async_to_sync
    async def close(self) -> None:
        return await self.aclose()

    def memFrame(self, data_id: Optional[str] = None, data: Any = None, columns: Optional[List[str]] = None):
        return self._ops(data_id, data, columns)

    async def __aenter__(self):
        await self.aconnect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()


