from __future__ import annotations

from typing import Any, Optional

from src.db_manager.adapters.base import DatabaseAdapter
from src.wrappers.analytix.inspect import TableOpsWrapper
from src.wrappers.analytix.selection import SelectionWrapper
from wrappers.analytix.cleaning import CleaningWrapper
from wrappers.analytix.stats import StatsWrapper


class ContextManager(TableOpsWrapper, SelectionWrapper,CleaningWrapper,StatsWrapper):
    memframe: Any
    _data_id: Optional[str]
    _adapter: Optional[DatabaseAdapter]
    _inspect_wrapper: Optional[TableOpsWrapper]
    _selection_wrapper: Optional[SelectionWrapper]
    _clean_wrapper: Optional[CleaningWrapper]
    _stats_wrapper: Optional[StatsWrapper]

    def __init__(self, memframe_instance: Any, data_id: Optional[str] = None) -> None: ...

    @property
    def inspect(self) -> TableOpsWrapper: ...

    @property
    def select(self) -> SelectionWrapper: ...
    
    @property
    def clean(self) -> CleaningWrapper: ...
    
    @property
    def stats(self) -> StatsWrapper: ...
    

    async def close(self) -> None: ...
