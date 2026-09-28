from __future__ import annotations

from typing import Any, Literal, overload

from memframe.core.orchestrator.analytix.comparison import ComparisonOrchestrator


class ComparisonWrapper(ComparisonOrchestrator):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    @overload
    def __call__(self, expression: str, /) -> dict[str, Any]: ...
    @overload
    def __call__(
        self,
        col1: str,
        col2: str,
        operator: Literal["==", "!=", ">", "<", ">=", "<="],
        /,
    ) -> dict[str, Any]: ...

    @overload
    async def acompare(self, expression: str, /) -> dict[str, Any]: ...
    @overload
    async def acompare(
        self,
        col1: str,
        col2: str,
        operator: Literal["==", "!=", ">", "<", ">=", "<="],
        /,
    ) -> dict[str, Any]: ...

    @overload
    def compare(self, expression: str, /) -> dict[str, Any]: ...
    @overload
    def compare(
        self,
        col1: str,
        col2: str,
        operator: Literal["==", "!=", ">", "<", ">=", "<="],
        /,
    ) -> dict[str, Any]: ...
