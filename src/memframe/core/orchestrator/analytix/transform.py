from typing import Any, Dict, List, Optional
from memframe.core.analytix.transform import TransformOps
from memframe.cache import record_call


class TransformOrchestrator:
    """Orchestrator for feature engineering transform operations."""

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._Transform_ops = None

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)

    async def _ensure_ops(self) -> TransformOps:
        if self._Transform_ops is None:
            await self._ops_parent._ensure_adapter()
            self._Transform_ops = TransformOps(self._ops_parent._adapter)
        return self._Transform_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    def _persistence_context(self) -> Dict[str, Any]:
        backend = self._ops_parent.memframe._backend
        data_id = self._ops_parent._data_id or self._ops_parent.memframe._active_id
        return {"backend": backend, "data_id": data_id}

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def scale(self, column: str) -> Dict[str, Any]:
        """Standard scaling (z-score), similar to sklearn StandardScaler."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_standardize(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def minmax(self, column: str) -> Dict[str, Any]:
        """Min-max scaling, similar to sklearn MinMaxScaler."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_minmax_scale(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def bin(self, column: str, bins: int = 5, strategy: str = "uniform") -> Dict[str, Any]:
        """Binning (pd.cut / qcut-like behaviour)."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_bin(table, schema, column, bins, strategy, **self._persistence_context())

    @record_call(deep_cache=True)
    async def poly(self, column: str, degree: int = 2) -> Dict[str, Any]:
        """Polynomial feature expansion, similar to sklearn PolynomialFeatures."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_polynomial_features(table, schema, column, degree, **self._persistence_context())

    @record_call(deep_cache=True)
    async def interact(self, column1: str, column2: str) -> Dict[str, Any]:
        """Create interaction term between two columns."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_interaction_terms(table, schema, column1, column2, **self._persistence_context())

    # ------------------------------------------------------------------
    # Categorical
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def onehot(self, column: str, max_categories: int = 10) -> Dict[str, Any]:
        """One-hot encoding (pd.get_dummies / sklearn OneHotEncoder-style)."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_one_hot_encode(table, schema, column, max_categories, **self._persistence_context())

    @record_call(deep_cache=True)
    async def label(self, column: str) -> Dict[str, Any]:
        """Label encoding, similar to sklearn LabelEncoder."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_label_encode(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def frequency(self, column: str) -> Dict[str, Any]:
        """Frequency encoding."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_frequency_encode(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def target(self, column: str, target_column: str) -> Dict[str, Any]:
        """Target encoding."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_target_encode(table, schema, column, target_column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def binarize(
        self,
        column: str,
        value: Optional[Any] = None,
        condition: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Binarize column values, similar to sklearn binarize/Binarizer."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_binarize(table, schema, column, value, condition, **self._persistence_context())

    # ------------------------------------------------------------------
    # Datetime
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def cyclical(self, column: str, features: List[str]) -> Dict[str, Any]:
        """Cyclical datetime encoding (sin/cos features)."""
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.datetime_cyclical_encode(table, schema, column, features, **self._persistence_context())

    # ------------------------------------------------------------------
    # Tier1 scalers / transforms
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def robust_scale(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_robust_scale(table, schema, column, quantile_range, **self._persistence_context())

    @record_call(deep_cache=True)
    async def robust(self, column: str, quantile_range: tuple = (25, 75)) -> Dict[str, Any]:
        return await self.robust_scale(column, quantile_range)

    @record_call(deep_cache=True)
    async def maxabs_scale(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_maxabs_scale(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def maxabs(self, column: str) -> Dict[str, Any]:
        return await self.maxabs_scale(column)

    @record_call(deep_cache=True)
    async def normalize(self, column: str, norm: str = "l2") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_normalize(table, schema, column, norm, **self._persistence_context())

    @record_call(deep_cache=True)
    async def log(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_log_transform(table, schema, column, base, epsilon, **self._persistence_context())

    @record_call(deep_cache=True)
    async def log_transform(self, column: str, base: str = "e", epsilon: float = 0) -> Dict[str, Any]:
        return await self.log(column, base, epsilon)

    @record_call(deep_cache=True)
    async def quantile_transform(self, column: str, output: str = "uniform") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_quantile_transform(table, schema, column, output, **self._persistence_context())

    @record_call(deep_cache=True)
    async def quantile(self, column: str, output: str = "uniform") -> Dict[str, Any]:
        return await self.quantile_transform(column, output)

    @record_call(deep_cache=True)
    async def power_transform(self, column: str, method: str = "yeo-johnson") -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.numeric_power_transform(table, schema, column, method, **self._persistence_context())

    @record_call(deep_cache=True)
    async def power(self, column: str, method: str = "yeo-johnson") -> Dict[str, Any]:
        return await self.power_transform(column, method)

    @record_call(deep_cache=True)
    async def ordinal_encode(self, column: str) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.categorical_ordinal_encode(table, schema, column, **self._persistence_context())

    @record_call(deep_cache=True)
    async def ordinal(self, column: str) -> Dict[str, Any]:
        return await self.ordinal_encode(column)

    # ------------------------------------------------------------------
    # Pandas/sklearn aliases
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def get_dummies(self, column: str, max_categories: int = 10) -> Dict[str, Any]:
        return await self.onehot(column, max_categories)

    @record_call(deep_cache=True)
    async def cut(self, column: str, bins: int = 5, strategy: str = "uniform") -> Dict[str, Any]:
        return await self.bin(column, bins=bins, strategy=strategy)

    @record_call(deep_cache=True)
    async def qcut(self, column: str, bins: int = 5) -> Dict[str, Any]:
        """Quantile-based binning, similar to pandas qcut."""
        return await self.cut(column, bins=bins, strategy="quantile")


# Alias for convenience
TransformAccessor = TransformOrchestrator
