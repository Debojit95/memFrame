from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from memframe.core.orchestrator.analytix.cumulative import CumulativeOrchestrator
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")


class CumulativeWrapper(CumulativeOrchestrator):
    """Wrapper around `CumulativeOrchestrator` with async/sync methods."""

    def __init__(self, *args, **kwargs):
        """Initialize the cumulative wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    # ------------------------------------------------------------------
    # cumsum
    # ------------------------------------------------------------------
    async def acumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative sum over a column."""
        return await super().cumsum(column, order_col, target_col)

    @async_to_sync
    async def cumsum(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative sum over a column."""
        return await self.acumsum(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cumprod
    # ------------------------------------------------------------------
    async def acumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative product over a column."""
        return await super().cumprod(column, order_col, target_col)

    @async_to_sync
    async def cumprod(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative product over a column."""
        return await self.acumprod(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cummax
    # ------------------------------------------------------------------
    async def acummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative maximum over a column."""
        return await super().cummax(column, order_col, target_col)

    @async_to_sync
    async def cummax(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative maximum over a column."""
        return await self.acummax(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cummin
    # ------------------------------------------------------------------
    async def acummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative minimum over a column."""
        return await super().cummin(column, order_col, target_col)

    @async_to_sync
    async def cummin(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative minimum over a column."""
        return await self.acummin(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cummean
    # ------------------------------------------------------------------
    async def acummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative mean over a column."""
        return await super().cummean(column, order_col, target_col)

    @async_to_sync
    async def cummean(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative mean over a column."""
        return await self.acummean(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cumcount
    # ------------------------------------------------------------------
    async def acumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative count over a column."""
        return await super().cumcount(column, order_col, target_col)

    @async_to_sync
    async def cumcount(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative count over a column."""
        return await self.acumcount(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cumstd
    # ------------------------------------------------------------------
    async def acumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative standard deviation."""
        return await super().cumstd(column, order_col, target_col)

    @async_to_sync
    async def cumstd(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative standard deviation."""
        return await self.acumstd(column, order_col, target_col)

    # ------------------------------------------------------------------
    # cumvar
    # ------------------------------------------------------------------
    async def acumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute cumulative variance."""
        return await super().cumvar(column, order_col, target_col)

    @async_to_sync
    async def cumvar(
        self,
        column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute cumulative variance."""
        return await self.acumvar(column, order_col, target_col)
