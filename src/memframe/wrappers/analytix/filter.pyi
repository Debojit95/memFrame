from __future__ import annotations

from typing import Any

from memframe.core.analytix.filter_I import Predicate
from memframe.core.orchestrator.analytix.filter import FilteringOrchestrator


class FilteringWrapper(FilteringOrchestrator):
    def __call__(
        self,
        predicate: Predicate | str,
        columns: str | list[str] = "*",
        chunk_size: int | None = None,
    ) -> dict[str, Any]: ...

    async def afilter(
        self,
        predicate: Predicate | str,
        columns: str | list[str] = "*",
        chunk_size: int | None = None,
    ) -> dict[str, Any]: ...

    def filter(
        self,
        predicate: Predicate | str,
        columns: str | list[str] = "*",
        chunk_size: int | None = None,
    ) -> dict[str, Any]: ...
