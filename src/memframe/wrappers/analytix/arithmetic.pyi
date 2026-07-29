# src/wrappers/arithmetic.pyi
from __future__ import annotations

from typing import Any, Dict, Union

from memframe.core.orchestrator.analytix.arithmetic import ArithmeticOrchestrator


class ArithmeticWrapper(ArithmeticOrchestrator):

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    # =========================================================
    # Binary Operations
    # =========================================================

    async def aadd(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def add(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def asubtract(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def subtract(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def asub(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def sub(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def amul(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def mul(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def adiv(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def div(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def amod(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def mod(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def apow(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def pow(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    # =========================================================
    # Unary Operations
    # =========================================================

    async def aabs(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def abs(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def anegate(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def negate(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def around(
        self,
        column: str,
        digits: int = 0,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def round(
        self,
        column: str,
        digits: int = 0,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def aceil(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def ceil(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def afloor(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def floor(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def atruncate(
        self,
        column: str,
        digits: int = 0,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def truncate(
        self,
        column: str,
        digits: int = 0,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    
    # =========================================================
    # Exp / Log / Root
    # =========================================================

    async def aexp(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def exp(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def alog(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def log(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def alog10(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def log10(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def asqrt(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def sqrt(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    # =========================================================
    # Trigonometric
    # =========================================================

    def sin(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def cos(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def tan(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def aasin(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def asin(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def aacos(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def acos(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def aatan(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def atan(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def aatan2(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def atan2(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    # =========================================================
    # Complex Operations
    # =========================================================

    async def aweighted_sum(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        weight1: float = 1,
        weight2: float = 1,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def weighted_sum(
        self,
        col1: Union[str, float, int],
        col2: Union[str, float, int],
        weight1: float = 1,
        weight2: float = 1,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def apercentage_change(
        self,
        old_col: str,
        new_col: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def percentage_change(
        self,
        old_col: str,
        new_col: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    async def anormalize_range(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...

    def normalize_range(
        self,
        column: str,
        target_col: str | None = None,
    ) -> Dict[str, Any]: ...
