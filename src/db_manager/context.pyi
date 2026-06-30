from __future__ import annotations

from typing import Any, Optional

from src.db_manager.adapters.base import DatabaseAdapter
from src.wrappers.analytix.inspect import TableOpsWrapper
from src.wrappers.analytix.selection import SelectionWrapper
from wrappers.analytix.cleaning import CleaningWrapper


class ContextManager(TableOpsWrapper, SelectionWrapper,CleaningWrapper):
    memframe: Any
    _data_id: Optional[str]
    _adapter: Optional[DatabaseAdapter]
    _inspect_wrapper: Optional[TableOpsWrapper]
    _selection_wrapper: Optional[SelectionWrapper]
    _clean_wrapper: Optional[CleaningWrapper]

    def __init__(self, memframe_instance: Any, data_id: Optional[str] = None) -> None: ...

    @property
    def inspect(self) -> TableOpsWrapper: ...

    @property
    def select(self) -> SelectionWrapper: ...
    
    @property
    def clean(self) -> CleaningWrapper: ...

    async def close(self) -> None: ...
