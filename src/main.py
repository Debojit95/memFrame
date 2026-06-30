import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.ingestion.datatype_detector import Backend
from src.db_manager.setup import DatabaseBackend
from src.db_manager.context import ContextManager
from src.wrappers.base import BaseWrapper


import logging
from typing import  Any, Dict, List, Optional




logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)



class MemFrame(BaseWrapper):
    _CTX_PUBLIC_APIS = {name for name in ContextManager.__dict__ if not name.startswith("_")}

    def __init__(self, connection_type: str = "local", connection_params: Optional[Dict[str, Any]] = None,):
        super().__init__()
        self.connection_type = connection_type
        self.conn_params = connection_params or {}
        self._backend: Optional[DatabaseBackend] = None
        self._active_id: Optional[str] = None
        self._mcp_manager = None
        self._context_manager = ContextManager(self)


    def __getattr__(self, name: str):
        # Delegate ContextManager APIs on the default/global context.
        if name in self._CTX_PUBLIC_APIS:
            return getattr(self._context_manager, name)

        # Fallback for uploader internals that depend on backend helper APIs.
        if self._backend and hasattr(self._backend, name):
            return getattr(self._backend, name)

        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        return sorted(
            set(super().__dir__())
            | self._CTX_PUBLIC_APIS
        )


    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ):
        """
        Return an orchestrated operations interface for DataFrame operations.

        Use either:
        - `data_id` (str): operate on an existing uploaded dataset
        - `data` (dict/list/pandas-compatible): converted with
          `pd.DataFrame(data, columns=columns)`, uploaded, then used

        Returns:
            ContextManager bound to the resolved data_id (or active dataset).
        """
        if data_id is not None and data is not None:
            raise ValueError("Pass either `data_id` or `data`, not both.")

        # Backward compatibility: positional non-string input is treated as data.
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
    
    def _placeholder(self, index: int) -> str:
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        return self._backend.placeholder(index)

    def _local_db_path(self) -> Optional[Path]:
        
        if not self._backend or self._backend.backend != Backend.DUCKDB:
            raise RuntimeError("Local DuckDB connection is not active.")

        db_path = self._backend.conn_params.get("db_path", "memframe_new.duckdb")

        if db_path == ":memory:":
            return None

        return Path(db_path)
    
    
    async def connect(self) -> None:
        if self.connection_type == "local":
            backend = Backend.DUCKDB

            db_path = self.conn_params.get("db_path", "memframe_new.duckdb")
            if db_path == ":memory:":
                logger.warning("In-memory DuckDB is disabled for local mode; using 'memframe_new.duckdb' instead.")
                db_path = "memframe_new.duckdb"

            params = {"db_path": db_path}

        elif self.connection_type == "remote":
            backend_type = self.conn_params.get("backend")
            if backend_type == "postgres":
                backend = Backend.POSTGRES
                params = {
                    "host": self.conn_params["host"],
                    "port": self.conn_params.get("port", 5432),
                    "user": self.conn_params["user"],
                    "password": self.conn_params["password"],
                    "database": self.conn_params["database"],
                }
            else:
                raise ValueError("Remote backend must be 'postgres'")
        else:
            raise ValueError("connection_type must be 'local' or 'remote'")

        self._backend = DatabaseBackend(backend, params)
        await self._backend.connect()
        await self._backend.init_database()
    
    async def close(self) -> None:
        if self._backend:
            await self._backend.close()
        if self._mcp_manager:
            await self._mcp_manager.close()
            self._mcp_manager = None

    

    def memFrame(self, data_id: Optional[str] = None, data: Any = None, columns: Optional[List[str]] = None,  ):
        return self._ops(data_id,data,columns)
    
    
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


