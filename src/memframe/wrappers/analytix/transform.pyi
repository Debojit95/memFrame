# transform_wrapper.pyi

from typing import Any, Dict, List, Optional


class TransformWrapper:
    def __init__(self, memframe_ops_instance) -> None: ...

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
    async def ascale(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def scale(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aminmax(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def minmax(
        self,
        column: str,
    ) -> Dict[str, Any]: ...


    async def abin(
        self,
        column: str,
        bins: int = ...,
        strategy: str = ...,
    ) -> Dict[str, Any]: ...

    def bin(
        self,
        column: str,
        bins: int = ...,
        strategy: str = ...,
    ) -> Dict[str, Any]: ...

    async def apoly(
        self,
        column: str,
        degree: int = ...,
    ) -> Dict[str, Any]: ...

    def poly(
        self,
        column: str,
        degree: int = ...,
    ) -> Dict[str, Any]: ...

    async def ainteract(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...

    def interact(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]: ...

    # ------------------------------------------------------------------
    # Categorical
    # ------------------------------------------------------------------
    async def aonehot(
        self,
        column: str,
        max_categories: int = ...,
    ) -> Dict[str, Any]: ...

    def onehot(
        self,
        column: str,
        max_categories: int = ...,
    ) -> Dict[str, Any]: ...

    async def alabel_encode(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def label_encode(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def afrequency_encode(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def frequency_encode(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def atarget_encode(
        self,
        column: str,
        target_column: str,
    ) -> Dict[str, Any]: ...

    def target_encode(
        self,
        column: str,
        target_column: str,
    ) -> Dict[str, Any]: ...

    async def abinarize(
        self,
        column: str,
        value: Optional[Any] = ...,
        condition: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    def binarize(
        self,
        column: str,
        value: Optional[Any] = ...,
        condition: Optional[str] = ...,
    ) -> Dict[str, Any]: ...

    # ------------------------------------------------------------------
    # Datetime
    # ------------------------------------------------------------------
    async def acyclical_encode(
        self,
        column: str,
        features: List[str],
    ) -> Dict[str, Any]: ...

    def cyclical_encode(
        self,
        column: str,
        features: List[str],
    ) -> Dict[str, Any]: ...

    # ------------------------------------------------------------------
    # Tier1 scalers / transforms
    # ------------------------------------------------------------------
    async def arobust_scale(self, column: str, quantile_range: tuple = ...) -> Dict[str, Any]: ...
    def robust_scale(self, column: str, quantile_range: tuple = ...) -> Dict[str, Any]: ...
    async def arobust(self, column: str, quantile_range: tuple = ...) -> Dict[str, Any]: ...
    def robust(self, column: str, quantile_range: tuple = ...) -> Dict[str, Any]: ...
    async def amaxabs_scale(self, column: str) -> Dict[str, Any]: ...
    def maxabs_scale(self, column: str) -> Dict[str, Any]: ...
    async def amaxabs(self, column: str) -> Dict[str, Any]: ...
    def maxabs(self, column: str) -> Dict[str, Any]: ...
    async def anormalize(self, column: str, norm: str = ...) -> Dict[str, Any]: ...
    def normalize(self, column: str, norm: str = ...) -> Dict[str, Any]: ...
    async def alog(self, column: str, base: str = ..., epsilon: float = ...) -> Dict[str, Any]: ...
    def log(self, column: str, base: str = ..., epsilon: float = ...) -> Dict[str, Any]: ...
    async def alog_transform(self, column: str, base: str = ..., epsilon: float = ...) -> Dict[str, Any]: ...
    def log_transform(self, column: str, base: str = ..., epsilon: float = ...) -> Dict[str, Any]: ...
    async def aquantile_transform(self, column: str, output: str = ...) -> Dict[str, Any]: ...
    def quantile_transform(self, column: str, output: str = ...) -> Dict[str, Any]: ...
    async def aquantile(self, column: str, output: str = ...) -> Dict[str, Any]: ...
    def quantile(self, column: str, output: str = ...) -> Dict[str, Any]: ...
    async def apower_transform(self, column: str, method: str = ...) -> Dict[str, Any]: ...
    def power_transform(self, column: str, method: str = ...) -> Dict[str, Any]: ...
    async def apower(self, column: str, method: str = ...) -> Dict[str, Any]: ...
    def power(self, column: str, method: str = ...) -> Dict[str, Any]: ...
    async def aordinal_encode(self, column: str) -> Dict[str, Any]: ...
    def ordinal_encode(self, column: str) -> Dict[str, Any]: ...
    async def aordinal(self, column: str) -> Dict[str, Any]: ...
    def ordinal(self, column: str) -> Dict[str, Any]: ...

    # ------------------------------------------------------------------
    # Aliases
    # ------------------------------------------------------------------
    async def aget_dummies(
        self,
        column: str,
        max_categories: int = ...,
    ) -> Dict[str, Any]: ...

    def get_dummies(
        self,
        column: str,
        max_categories: int = ...,
    ) -> Dict[str, Any]: ...

    async def acut(
        self,
        column: str,
        bins: int = ...,
        strategy: str = ...,
    ) -> Dict[str, Any]: ...

    def cut(
        self,
        column: str,
        bins: int = ...,
        strategy: str = ...,
    ) -> Dict[str, Any]: ...

    async def aqcut(
        self,
        column: str,
        bins: int = ...,
    ) -> Dict[str, Any]: ...

    def qcut(
        self,
        column: str,
        bins: int = ...,
    ) -> Dict[str, Any]: ...


TransformAccessor = TransformWrapper