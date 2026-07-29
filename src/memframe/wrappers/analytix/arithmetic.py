"""Arithmetic wrapper exposing async and sync-friendly analytic operations.

This module bridges :class:`ArithmeticOrchestrator` methods into a wrapper API
that provides:
- `a*` coroutine methods for async usage.
- decorated sync-callable methods via `@async_to_sync`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from memframe.core.orchestrator.analytix.arithmetic import ArithmeticOrchestrator
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(handler)


class ArithmeticWrapper(ArithmeticOrchestrator):
    """Wrapper around `ArithmeticOrchestrator` with async/sync method pairs.

    Each operation is exposed as:
    - an async method prefixed with `a` (for example, `aadd`)
    - a sync-friendly counterpart (for example, `add`) decorated with
      `@async_to_sync`
    """

    def __init__(self, *args, **kwargs):
        """Initialize the arithmetic wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    # =========================================================
    # Binary Operations
    # =========================================================

    async def aadd(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously add two columns or scalar values."""
        return await super().add(col1, col2, target_col)

    @async_to_sync
    async def add(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously add two columns or scalar values."""
        return await self.aadd(col1, col2, target_col)

    async def asubtract(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously subtract the second operand from the first."""
        return await super().subtract(col1, col2, target_col)

    @async_to_sync
    async def subtract(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously subtract the second operand from the first."""
        return await self.asubtract(col1, col2, target_col)

    async def asub(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply alias subtraction (`sub`) operation."""
        return await super().subtract(col1, col2, target_col)

    @async_to_sync
    async def sub(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply alias subtraction (`sub`) operation."""
        return await self.asub(col1, col2, target_col)

    async def amul(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously multiply two operands."""
        return await super().mul(col1, col2, target_col)

    @async_to_sync
    async def mul(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously multiply two operands."""
        return await self.amul(col1, col2, target_col)

    async def adiv(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously divide the first operand by the second."""
        return await super().div(col1, col2, target_col)

    @async_to_sync
    async def div(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously divide the first operand by the second."""
        return await self.adiv(col1, col2, target_col)

    async def amod(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute modulo of the first operand by the second."""
        return await super().mod(col1, col2, target_col)

    @async_to_sync
    async def mod(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute modulo of the first operand by the second."""
        return await self.amod(col1, col2, target_col)

    async def apow(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously raise the first operand to the power of the second."""
        return await super().pow(col1, col2, target_col)

    @async_to_sync
    async def pow(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously raise the first operand to the power of the second."""
        return await self.apow(col1, col2, target_col)

    # =========================================================
    # Unary Operations
    # =========================================================

    async def aabs(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute absolute values for a column."""
        return await super().abs(column, target_col)

    @async_to_sync
    async def abs(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute absolute values for a column."""
        return await self.aabs(column, target_col)

    async def anegate(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously negate numeric values in a column."""
        return await super().negate(column, target_col)

    @async_to_sync
    async def negate(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously negate numeric values in a column."""
        return await self.anegate(column, target_col)

    async def around(
        self,
        column: str,
        digits: int = 0,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously round values to the given number of digits."""
        return await super().round(column, digits, target_col)

    @async_to_sync
    async def round(
        self,
        column: str,
        digits: int = 0,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously round values to the given number of digits."""
        return await self.around(column, digits, target_col)

    async def aceil(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply ceiling to column values."""
        return await super().ceil(column, target_col)

    @async_to_sync
    async def ceil(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply ceiling to column values."""
        return await self.aceil(column, target_col)

    async def afloor(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply floor to column values."""
        return await super().floor(column, target_col)

    @async_to_sync
    async def floor(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply floor to column values."""
        return await self.afloor(column, target_col)

    async def atruncate(
        self,
        column: str,
        digits: int = 0,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously truncate values to a fixed number of digits."""
        return await super().truncate(column, digits, target_col)

    @async_to_sync
    async def truncate(
        self,
        column: str,
        digits: int = 0,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously truncate values to a fixed number of digits."""
        return await self.atruncate(column, digits, target_col)

    

    # =========================================================
    # Exp / Log / Root
    # =========================================================

    async def aexp(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply exponential transform to a column."""
        return await super().exp(column, target_col)

    @async_to_sync
    async def exp(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply exponential transform to a column."""
        return await self.aexp(column, target_col)

    async def alog(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply natural logarithm to a column."""
        return await super().log(column, target_col)

    @async_to_sync
    async def log(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply natural logarithm to a column."""
        return await self.alog(column, target_col)

    async def alog10(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply base-10 logarithm to a column."""
        return await super().log10(column, target_col)

    @async_to_sync
    async def log10(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply base-10 logarithm to a column."""
        return await self.alog10(column, target_col)

    async def asqrt(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply square-root transform to a column."""
        return await super().sqrt(column, target_col)

    @async_to_sync
    async def sqrt(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply square-root transform to a column."""
        return await self.asqrt(column, target_col)

    # =========================================================
    # Trigonometric
    # =========================================================

    @async_to_sync
    async def sin(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply sine transform to a column."""
        return await super().sin(column, target_col)

    @async_to_sync
    async def cos(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply cosine transform to a column."""
        return await super().cos(column, target_col)

    @async_to_sync
    async def tan(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply tangent transform to a column."""
        return await super().tan(column, target_col)

    async def aasin(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply inverse-sine transform to a column."""
        return await super().asin(column, target_col)

    @async_to_sync
    async def asin(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply inverse-sine transform to a column."""
        return await self.aasin(column, target_col)

    async def aacos(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply inverse-cosine transform to a column."""
        return await super().acos(column, target_col)

    @async_to_sync
    async def acos(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply inverse-cosine transform to a column."""
        return await self.aacos(column, target_col)

    async def aatan(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply inverse-tangent transform to a column."""
        return await super().atan(column, target_col)

    @async_to_sync
    async def atan(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply inverse-tangent transform to a column."""
        return await self.aatan(column, target_col)

    async def aatan2(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously apply two-argument arctangent transform."""
        return await super().atan2(col1, col2, target_col)

    @async_to_sync
    async def atan2(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously apply two-argument arctangent transform."""
        return await self.aatan2(col1, col2, target_col)

    # =========================================================
    # Complex Operations
    # =========================================================

    async def aweighted_sum(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        weight1: float = 1,
        weight2: float = 1,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute weighted sum of two operands."""
        return await super().weighted_sum(
            col1,
            col2,
            weight1,
            weight2,
            target_col,
        )

    @async_to_sync
    async def weighted_sum(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        weight1: float = 1,
        weight2: float = 1,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute weighted sum of two operands."""
        return await self.aweighted_sum(
            col1,
            col2,
            weight1,
            weight2,
            target_col,
        )

    async def apercentage_change(
        self,
        old_col: str,
        new_col: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute percentage change from old to new column."""
        return await super().percentage_change(
            old_col,
            new_col,
            target_col,
        )

    @async_to_sync
    async def percentage_change(
        self,
        old_col: str,
        new_col: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute percentage change from old to new column."""
        return await self.apercentage_change(
            old_col,
            new_col,
            target_col,
        )

    async def anormalize_range(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously normalize values to the configured numeric range."""
        return await super().normalize_range(column, target_col)

    @async_to_sync
    async def normalize_range(
        self,
        column: str,
        target_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously normalize values to the configured numeric range."""
        return await self.anormalize_range(column, target_col)
