from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from db_manager.context import ContextManager
from wrappers.base import BaseWrapper


class MemFrame(BaseWrapper):
    connection_type: str
    conn_params: Dict[str, Any]
    _active_id: Optional[str]

    def __init__(
        self,
        connection_type: str = "local",
        connection_params: Optional[Dict[str, Any]] = None,
    ) -> None: ...

    async def aconnect(self) -> None: ...
    def connect(self) -> None: ...
    async def close(self) -> None: ...

    async def aupload_csv(self, file_path: Union[str, Path]) -> ContextManager: ...
    def upload_csv(self, file_path: Union[str, Path]) -> ContextManager: ...

    async def aupload_parquet(self, file_path: Union[str, Path]) -> ContextManager: ...
    def upload_parquet(self, file_path: Union[str, Path]) -> ContextManager: ...

    async def aupload_df(self, df: Any, filename: Optional[str] = None) -> ContextManager: ...
    def upload_df(self, df: Any, filename: Optional[str] = None) -> ContextManager: ...

    def memFrame(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ) -> ContextManager: ...

    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ) -> ContextManager: ...

    async def __aenter__(self) -> MemFrame: ...
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...
