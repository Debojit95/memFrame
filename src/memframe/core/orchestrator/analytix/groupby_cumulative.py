"""
User‑facing group‑by cumulative orchestrator.
Now supports both a unified direct API and the builder pattern.
All operations record the generated table via @record_call.
"""

from typing import Any, Dict, List, Optional, Union


from memframe.core.analytix.groupby_cumulative import (
    GroupbyCumulativeOps,
    make_groupby_cumulative_ops,
)
from memframe.core.analytix._generated import make_sig, publish_row_aligned
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

    
    
    
    async def _ensure_adapter(self):
        await self._ops_parent._ensure_adapter()
        self._adapter = self._ops_parent._adapter

    async def _get_active_context(self):
        return await self._ops_parent._get_active_context()

    async def _ensure_ops(self) -> GroupbyCumulativeOps:
        if self._core_ops is None:
            await self._ensure_adapter()
            self._core_ops = make_groupby_cumulative_ops(self._adapter)
        return self._core_ops

    @staticmethod
    def _involved_cols(column, group_cols, order_col):
        cols = [column, *group_cols]
        if order_col:
            if isinstance(order_col, (list, tuple)):
                cols.extend(order_col)
            else:
                cols.append(order_col)
        return cols

    async def _run(self, operation, column, group_cols, order_col, target_col):
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id
        if isinstance(group_cols, str):
            group_cols = [group_cols]
        result = await getattr(ops, operation)(
            table, schema, column, group_cols,
            order_col=order_col, target_col=target_col,
            backend=backend, data_id=data_id, map_feature=False,
        )
        sig = make_sig(
            op=f"groupby_cumulative.{operation}", column=column,
            group_cols=group_cols, order_col=order_col,
            target_col=target_col,
        )
        return await publish_row_aligned(
            ops, table, schema, result,
            self._involved_cols(column, group_cols, order_col),
            sig=sig, op_type="groupby_cum_map",
            class_name=self.__class__.__name__, method_name=operation,
            backend=backend, data_id=data_id,
        )

    # ------------------------------------------------------------------
    #  UNIFIED DIRECT METHODS
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def cumsum(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,) -> Dict[str, Any]:
        
        return await self._run("cumsum", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumprod(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cumprod", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummax(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cummax", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummin(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cummin", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cummean(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cummean", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumcount(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cumcount", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumstd(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cumstd", column, group_cols, order_col, target_col)

    @record_call(deep_cache=True)
    async def cumvar(
        self,
        column: str,
        group_cols: Union[str, List[str]],
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None,
        map_feature: bool = False,
    ) -> Dict[str, Any]:
        return await self._run("cumvar", column, group_cols, order_col, target_col)

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





