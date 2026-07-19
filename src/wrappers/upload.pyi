from __future__ import annotations

from pathlib import Path
from typing import Any

from core.ingestion.upload_manager import Uploader
from db_manager.context import ContextManager

import pandas as pd


class UploadWrapper(Uploader):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    async def aupload_csv(self, file_path: str | Path) -> ContextManager: ...
    def upload_csv(self, file_path: str | Path) -> ContextManager: ...

    async def aupload_parquet(self, file_path: str | Path) -> ContextManager: ...
    def upload_parquet(self, file_path: str | Path) -> ContextManager: ...

    async def aupload_df(
        self, df: pd.DataFrame, filename: str | None = None
    ) -> ContextManager: ...
    def upload_df(
        self, df: pd.DataFrame, filename: str | None = None
    ) -> ContextManager: ...
