from __future__ import annotations

from typing import Any

from memframe.core.orchestrator.analytix.cumulative import CumulativeOrchestrator


class CumulativeWrapper(CumulativeOrchestrator):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    # ------------------------------------------------------------------
    # cumsum
    # ------------------------------------------------------------------
    async def acumsum(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cumsum(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cumprod
    # ------------------------------------------------------------------
    async def acumprod(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cumprod(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cummax
    # ------------------------------------------------------------------
    async def acummax(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cummax(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cummin
    # ------------------------------------------------------------------
    async def acummin(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cummin(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cummean
    # ------------------------------------------------------------------
    async def acummean(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cummean(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cumcount
    # ------------------------------------------------------------------
    async def acumcount(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cumcount(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cumstd
    # ------------------------------------------------------------------
    async def acumstd(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cumstd(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    # ------------------------------------------------------------------
    # cumvar
    # ------------------------------------------------------------------
    async def acumvar(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...

    def cumvar(
        self,
        column: str,
        order_col: str | list[str] | None = None,
        target_col: str | None = None,
    ) -> dict[str, Any]: ...