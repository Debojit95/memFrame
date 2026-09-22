"""
User-facing group-by orchestrator.
Now supports both a unified direct API and the builder pattern.
All operations record the generated table via @record_call.
"""

from typing import Any, Dict, List, Optional, Union
from memframe.core.analytix.groupby_stats import GroupByStatsOps
from memframe.cache import record_call



class GroupByStatsOrchestrator:
    """
    Unified group-by orchestrator.
    Direct usage:
        await ops.groupby_stats.agg(group_cols=["region"], agg_dict={"sales": ["sum","mean"]})
    Builder usage:
        await ops.groupby_stats.groupby("region").agg({"sales": ["sum"]})
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe
        self._data_id = memframe_ops_instance._data_id
        self._core_ops: Optional[GroupByStatsOps] = None
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

    async def _ensure_ops(self) -> GroupByStatsOps:
        if self._core_ops is None:
            await self._ensure_adapter()
            self._core_ops = GroupByStatsOps(self._adapter)
        return self._core_ops

    # ------------------------------------------------------------------
    #  UNIFIED DIRECT AGGREGATION
    # ------------------------------------------------------------------
    @record_call
    async def agg(
        self,
        group_cols: Union[str, List[str]],
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,) -> Dict[str, Any]:
        """
        Directly perform group-by aggregation on the active dataset.
        group_cols: single column name or list of column names
        agg_dict: mapping of column -> list of stats (e.g. {"sales": ["sum","mean"]})
        """
        
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(group_cols, str):
            group_cols = [group_cols]

        return await ops.group_aggregate(
            table, schema, group_cols, agg_dict,
            backend=backend, data_id=data_id, new_table=new_table,
        )

    @record_call
    async def event_rate(
        self,
        group_cols: Union[str, List[str]],
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compute event rate per group.
        """
        ops = await self._ensure_ops()
        table, schema = await self._get_active_context()
        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        if isinstance(group_cols, str):
            group_cols = [group_cols]

        return await ops.group_event_rate(
            table, schema, group_cols, datetime_col, unit,
            backend=backend, data_id=data_id, new_table=new_table,
        )

    # ------------------------------------------------------------------
    #  BUILDER PATTERN (returns a GroupBy helper)
    # ------------------------------------------------------------------
    @record_call
    def groupby(self, *columns: str) -> "GroupBy":
        """
        Create a GroupBy object for the given column(s).

        Example:
            await ops.groupby_stats.groupby("region").agg({"sales": ["sum"]})
        """
        if not columns:
            raise ValueError("Must provide at least one group-by column.")
        return GroupBy(self, list(columns))
    
    
    
class GroupBy:
    """
    Returned by `groupby()`, holds group columns and provides
    individual stat methods + .agg().
    """

    def __init__(self, parent: GroupByStatsOrchestrator, group_cols: List[str]):
        self._parent = parent
        self._memframe = parent._memframe
        self._data_id = parent._data_id
        self.group_cols = group_cols

    async def _get_ops_and_context(self):
        ops = await self._parent._ensure_ops()
        table, schema = await self._parent._get_active_context()
        backend = self._parent._memframe._backend
        data_id = self._parent._data_id or self._parent._memframe._active_id
        return ops, table, schema, backend, data_id

    # ------------------------------------------------------------------
    # internal async aggregator – never overridden by wrappers
    # ------------------------------------------------------------------
    async def _agg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        ops, table, schema, backend, data_id = await self._get_ops_and_context()
        return await ops.group_aggregate(
            table, schema, self.group_cols, agg_dict,
            backend=backend, data_id=data_id, new_table=new_table,
        )

    # public async agg – can be overridden by wrappers
    @record_call
    async def agg(
        self,
        agg_dict: Dict[str, List[str]],
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._agg(agg_dict, new_table)

    # ------------------------------------------------------------------
    # convenience methods – always use the safe _agg
    # ------------------------------------------------------------------
    @record_call
    async def sum(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["sum"]})

    @record_call
    async def mean(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["mean"]})

    @record_call
    async def min(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["min"]})

    @record_call
    async def max(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["max"]})

    @record_call
    async def count(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["count"]})

    @record_call
    async def median(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["median"]})

    @record_call
    async def mode(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["mode"]})

    @record_call
    async def std(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["std"]})

    @record_call
    async def var(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["var"]})

    @record_call
    async def sem(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["sem"]})

    @record_call
    async def nunique(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["nunique"]})

    @record_call
    async def range(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["range"]})

    @record_call
    async def product(self, column: str) -> Dict[str, Any]:
        return await self._agg({column: ["product"]})

    @record_call
    async def event_rate(
        self,
        datetime_col: str,
        unit: str = "day",
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        ops, table, schema, backend, data_id = await self._get_ops_and_context()
        return await ops.group_event_rate(
            table, schema, self.group_cols, datetime_col, unit,
            backend=backend, data_id=data_id, new_table=new_table,
        )
