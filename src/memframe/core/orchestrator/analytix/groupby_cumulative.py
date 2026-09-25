"""
User‑facing group‑by cumulative orchestrator.
Now supports both a unified direct API and the builder pattern.
All operations record the generated table via @record_call.
"""

from typing import Any, Dict, List, Optional, Union


from memframe.core.analytix.groupby_cumulative import GroupbyCumulativeOps
from memframe.cache import record_call


class GroupByCumulativeOrchestrator:
    """
    Unified group‑by cumulative orchestrator.
    Direct usage:
        await ops.cumulative_groupby.cumsum("sales", group_cols="region")
    Builder usage:
        await ops.cumulative_groupby.groupby("region").cumsum("sales")
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._core_ops: Optional[GroupbyCumulativeOps] = None
        self._adapter = None

    
    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)
    
    
    async def _ensure_adapter(self):
        await self._ops_parent._ensure_adapter()
        self._adapter = self._ops_parent._adapter

    async def _get_active_context(self):
        return await self._ops_parent._get_active_context()

    async def _ensure_ops(self) -> GroupbyCumulativeOps:
        if self._core_ops is None:
            await self._ensure_adapter()
            self._core_ops = GroupbyCumulativeOps(self._adapter)
        return self._core_ops

    # ------------------------------------------------------------------
    #  UNIFIED DIRECT METHODS
    # ------------------------------------------------------------------
    @record_call
    async def cumsum(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,) -> Dict[str, Any]:
        
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cumsum(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cumprod(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cumprod(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cummax(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cummax(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cummin(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cummin(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cummean(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cummean(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cumcount(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cumcount(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cumstd(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cumstd(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    @record_call
    async def cumvar(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        return await ops.cumvar(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=map_feature,
        )

    # ------------------------------------------------------------------
    #  BUILDER PATTERN (returns a GroupByCumulative helper)
    # ------------------------------------------------------------------
    def groupby(self, *columns: str) -> "GroupByCumulative":
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupByCumulative(self, list(columns))


class GroupByCumulative:
    """
    Fluent object returned by `GroupByCumulativeOrchestrator.groupby(cols)`.
    Provides cumulative methods (cumsum, cummean, …) that operate per group.
    """

    def __init__(self, parent: GroupByCumulativeOrchestrator, group_cols: List[str]):
        self._parent = parent
        self.group_cols = group_cols

    async def cumsum(self, column: str,
                     order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cumsum(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cumprod(self, column: str,
                      order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cumprod(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cummax(self, column: str,
                     order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cummax(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    async def cummin(self, column: str,
                     order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cummin(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cummean(self, column: str,
                      order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cummean(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cumcount(self, column: str,
                       order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cumcount(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cumstd(self, column: str,
                     order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cumstd(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )

    
    async def cumvar(self, column: str,
                     order_col=None, target_col=None, map_feature: bool = False) -> Dict[str, Any]:
        return await self._parent.cumvar(
            column=column,
            group_cols=self.group_cols,
            order_col=order_col,
            target_col=target_col,
            map_feature=map_feature,
        )





