# preprocessing_wrapper.py

from typing import Any, Dict, List, Optional

from memframe.core.orchestrator.analytix.preprocessing import PreprocessingOrchestrator
from memframe.utils.async_sync import async_to_sync


class PreprocessingWrapper(PreprocessingOrchestrator):
    """
    Public sync + async wrapper for PreprocessingOrchestrator.
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the preprocessing wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
    async def ascale(self, column: str) -> Dict[str, Any]:
        """Asynchronously standard-scale a numeric column."""
        return await super().scale(column)

    @async_to_sync
    async def scale(self, column: str) -> Dict[str, Any]:
        """Synchronously standard-scale a numeric column."""
        return await self.ascale(column)

    async def aminmax(self, column: str) -> Dict[str, Any]:
        """Asynchronously min-max scale a numeric column."""
        return await super().minmax(column)

    @async_to_sync
    async def minmax(self, column: str) -> Dict[str, Any]:
        """Synchronously min-max scale a numeric column."""
        return await self.aminmax(column)


    async def abin(
        self,
        column: str,
        bins: int = 5,
        strategy: str = "uniform",
    ) -> Dict[str, Any]:
        """Asynchronously bin numeric values into discrete intervals."""
        return await super().bin(column, bins, strategy)

    @async_to_sync
    async def bin(
        self,
        column: str,
        bins: int = 5,
        strategy: str = "uniform",
    ) -> Dict[str, Any]:
        """Synchronously bin numeric values into discrete intervals."""
        return await self.abin(column, bins, strategy)

    async def apoly(
        self,
        column: str,
        degree: int = 2,
    ) -> Dict[str, Any]:
        """Asynchronously generate polynomial features for a column."""
        return await super().poly(column, degree)

    @async_to_sync
    async def poly(
        self,
        column: str,
        degree: int = 2,
    ) -> Dict[str, Any]:
        """Synchronously generate polynomial features for a column."""
        return await self.apoly(column, degree)

    async def ainteract(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Asynchronously create interaction feature between two columns."""
        return await super().interact(column1, column2)

    @async_to_sync
    async def interact(
        self,
        column1: str,
        column2: str,
    ) -> Dict[str, Any]:
        """Synchronously create interaction feature between two columns."""
        return await self.ainteract(column1, column2)

    # ------------------------------------------------------------------
    # Categorical
    # ------------------------------------------------------------------
    async def aonehot(
        self,
        column: str,
        max_categories: int = 10,
    ) -> Dict[str, Any]:
        """Asynchronously one-hot encode a categorical column."""
        return await super().onehot(column, max_categories)

    @async_to_sync
    async def onehot(
        self,
        column: str,
        max_categories: int = 10,
    ) -> Dict[str, Any]:
        """Synchronously one-hot encode a categorical column."""
        return await self.aonehot(column, max_categories)

    async def alabel_encode(self, column: str) -> Dict[str, Any]:
        """Asynchronously label-encode a categorical column."""
        return await super().label(column)

    @async_to_sync
    async def label_encode(self, column: str) -> Dict[str, Any]:
        """Synchronously label-encode a categorical column."""
        return await self.alabel_encode(column)

    async def afrequency_encode(self, column: str) -> Dict[str, Any]:
        """Asynchronously frequency-encode a categorical column."""
        return await super().frequency(column)

    @async_to_sync
    async def frequency_encode(self, column: str) -> Dict[str, Any]:
        """Synchronously frequency-encode a categorical column."""
        return await self.afrequency_encode(column)

    async def atarget_encode(
        self,
        column: str,
        target_column: str,
    ) -> Dict[str, Any]:
        """Asynchronously target-encode a categorical column."""
        return await super().target(column, target_column)

    @async_to_sync
    async def target_encode(
        self,
        column: str,
        target_column: str,
    ) -> Dict[str, Any]:
        """Synchronously target-encode a categorical column."""
        return await self.atarget_encode(column, target_column)

    async def abinarize(
        self,
        column: str,
        value: Optional[Any] = None,
        condition: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously binarize a column using value or condition."""
        return await super().binarize(column, value, condition)

    @async_to_sync
    async def binarize(
        self,
        column: str,
        value: Optional[Any] = None,
        condition: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously binarize a column using value or condition."""
        return await self.abinarize(column, value, condition)

    # ------------------------------------------------------------------
    # Datetime
    # ------------------------------------------------------------------
    async def acyclical_encode(
        self,
        column: str,
        features: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously generate cyclical datetime features."""
        return await super().cyclical(column, features)

    @async_to_sync
    async def cyclical_encode(
        self,
        column: str,
        features: List[str],
    ) -> Dict[str, Any]:
        """Synchronously generate cyclical datetime features."""
        return await self.acyclical_encode(column, features)

    # ------------------------------------------------------------------
    # Tier1 scalers / transforms
    # ------------------------------------------------------------------
    async def arobust_scale(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        return await super().robust_scale(column, quantile_range)

    @async_to_sync
    async def robust_scale(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        return await self.arobust_scale(column, quantile_range)

    async def arobust(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        return await self.arobust_scale(column, quantile_range)

    @async_to_sync
    async def robust(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        return await self.arobust(column, quantile_range)

    async def amaxabs_scale(self, column: str) -> Dict[str, Any]:
        return await super().maxabs_scale(column)

    @async_to_sync
    async def maxabs_scale(self, column: str) -> Dict[str, Any]:
        return await self.amaxabs_scale(column)

    async def amaxabs(self, column: str) -> Dict[str, Any]:
        return await self.amaxabs_scale(column)

    @async_to_sync
    async def maxabs(self, column: str) -> Dict[str, Any]:
        return await self.amaxabs(column)

    async def anormalize(self, column: str, norm: str = "l2") -> Dict[str, Any]:
        return await super().normalize(column, norm)

    @async_to_sync
    async def normalize(self, column: str, norm: str = "l2") -> Dict[str, Any]:
        return await self.anormalize(column, norm)

    async def alog(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        return await super().log(column, base, epsilon)

    @async_to_sync
    async def log(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        return await self.alog(column, base, epsilon)

    async def alog_transform(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        return await self.alog(column, base, epsilon)

    @async_to_sync
    async def log_transform(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        return await self.alog_transform(column, base, epsilon)

    # ------------------------------------------------------------------
    # Aliases
    # ------------------------------------------------------------------
    async def aget_dummies(
        self,
        column: str,
        max_categories: int = 10,
    ) -> Dict[str, Any]:
        """Asynchronously run `onehot` via alias method."""
        return await self.aonehot(column, max_categories)

    @async_to_sync
    async def get_dummies(
        self,
        column: str,
        max_categories: int = 10,
    ) -> Dict[str, Any]:
        """Synchronously run `onehot` via alias method."""
        return await self.aget_dummies(column, max_categories)


    async def acut(
        self,
        column: str,
        bins: int = 5,
        strategy: str = "uniform",
    ) -> Dict[str, Any]:
        """Asynchronously run `bin` via alias method."""
        return await self.abin(column, bins, strategy)

    @async_to_sync
    async def cut(
        self,
        column: str,
        bins: int = 5,
        strategy: str = "uniform",
    ) -> Dict[str, Any]:
        """Synchronously run `bin` via alias method."""
        return await self.acut(column, bins, strategy)

    async def aqcut(
        self,
        column: str,
        bins: int = 5,
    ) -> Dict[str, Any]:
        """Asynchronously quantile-bin numeric values."""
        return await self.abin(column, bins, strategy="quantile")

    @async_to_sync
    async def qcut(
        self,
        column: str,
        bins: int = 5,
    ) -> Dict[str, Any]:
        """Synchronously quantile-bin numeric values."""
        return await self.aqcut(column, bins)


PreprocessAccessor = PreprocessingWrapper
