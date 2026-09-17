from typing import Any, Dict, List, Optional, Union

from memframe.core.analytix.cumulative import DataCumulativeOps
from memframe.cache import record_call


class CumulativeOrchestrator:
    """
    User‑facing cumulative operations (like pandas expanding()).
    Accessed via `ops.cumulative`.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe   # MemFrame
        self._data_id = memframe_ops_instance._data_id
        self._cumulative_ops = None

       
    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)
    
    
    async def _ensure_ops(self) -> DataCumulativeOps:
        if self._cumulative_ops is None:
            await self._ops_parent._ensure_adapter()
            self._cumulative_ops = DataCumulativeOps(
                self._ops_parent._adapter)
        return self._cumulative_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    # ------------------------------------------------------------------
    #  Public methods (order_col now optional and accepts list)
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def cumsum(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumsum(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumprod(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumprod(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummax(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cummax(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummin(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cummin(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummean(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cummean(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumcount(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumcount(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumstd(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumstd(
            table, schema, column, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumvar(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumvar(
            table, schema, column, order_col, target_col)