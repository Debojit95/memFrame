from __future__ import annotations

from typing import Any, Optional

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
from memframe.wrappers.analytix.cleaning import CleaningWrapper
from memframe.wrappers.analytix.datetime import DateTimeWrapper
from memframe.wrappers.analytix.inspection import TableOpsWrapper
from memframe.wrappers.analytix.selection import SelectionWrapper
from memframe.wrappers.analytix.stats import StatsWrapper
from memframe.wrappers.plots.bar import BarWrapper
from memframe.wrappers.plots.bar_polar import BarPolarWrapper
from memframe.wrappers.plots.line import LineWrapper
from memframe.wrappers.plots.pie import PieWrapper
from memframe.wrappers.plots.scatter import ScatterWrapper
from memframe.wrappers.plots.scatter3d import Scatter3DWrapper


class ContextManager(
    SelectionWrapper,
    TableOpsWrapper,
    CleaningWrapper,
    StatsWrapper,
    ArithmeticWrapper,
    BarWrapper,
    BarPolarWrapper,
    PieWrapper,
    LineWrapper,
    ScatterWrapper,
    Scatter3DWrapper,
):
    memframe: Any
    _data_id: Optional[str]
    _adapter: Optional[DatabaseAdapter]
    dt: DateTimeWrapper

    def __init__(self, memframe_instance: Any, data_id: Optional[str] = None) -> None: ...

    async def aclose(self) -> None: ...
    async def _ensure_adapter(self) -> None: ...
    async def _get_active_context(self) -> tuple: ...
    async def achat(self, prompt: str, session_id: Optional[str] = None) -> dict: ...
    def chat(self, prompt: str, session_id: Optional[str] = None) -> dict: ...
    async def aenable_agent(
        self,
        api_key: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        **overrides: Any,
    ) -> Any: ...
    def enable_agent(
        self,
        api_key: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        **overrides: Any,
    ) -> Any: ...
