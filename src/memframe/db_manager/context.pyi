from __future__ import annotations

from typing import Any, Optional

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.wrappers.analytix.inspect import TableOpsWrapper
from memframe.wrappers.analytix.selection import SelectionWrapper
from memframe.wrappers.analytix.cleaning import CleaningWrapper
from memframe.wrappers.analytix.stats import StatsWrapper
from memframe.wrappers.plots.bar import BarWrapper
from memframe.wrappers.plots.bar_polar import BarPolarWrapper
from memframe.wrappers.plots.pie import PieWrapper
from memframe.wrappers.plots.line import LineWrapper
from memframe.wrappers.plots.scatter import ScatterWrapper
from memframe.wrappers.plots.scatter_3d import Scatter3DWrapper


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
    
    @property
    def bar(self) -> BarWrapper: ...
    
    @property
    def bar_polar(self) -> BarPolarWrapper: ...
        
    @property
    def line(self) -> LineWrapper: ...

    @property
    def pie(self) -> PieWrapper: ...

    @property
    def scatter(self) -> ScatterWrapper: ...
    
    @property
    def scatter3d(self) -> Scatter3DWrapper: ...    
    
    
    
    
    
    async def close(self) -> None: ...
