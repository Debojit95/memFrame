from functools import wraps
from typing import Any, Dict, List, Optional, Union

from memframe.core.analytix.cumulative import CumulativeOps, make_cumulative_ops
from memframe.core.analytix._generated import make_sig, publish_row_aligned
from memframe.cache import record_call


def _publish_wide(fn):
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        table, schema = await self._get_context()
        result = await fn(self, *args, **kwargs)
        if isinstance(result, dict) and not result.get("is_error"):
            wide = result.get("new_table")
            if wide and wide != table:
                ops = await self._ensure_ops()
                result = await publish_row_aligned(
                    ops, table, schema, result,
                    result.get("involved_cols") or [],
                    sig=make_sig(op=fn.__name__, args=args, kwargs=kwargs),
                    op_type=f"cumulative_{fn.__name__}",
                    class_name=self.__class__.__name__,
                    method_name=fn.__name__,
                    backend=self._memframe._backend,
                    data_id=self._data_id or self._memframe._active_id,
                )
        return result
    return wrapper


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

       
    
    
    async def _ensure_ops(self) -> CumulativeOps:
        if self._cumulative_ops is None:
            await self._ops_parent._ensure_adapter()
            self._cumulative_ops = make_cumulative_ops(
                self._ops_parent._adapter)
        return self._cumulative_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    # ------------------------------------------------------------------
    #  Public methods (order_col now optional and accepts list)
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
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
    @_publish_wide
    async def cumvar(
        self, column: str,
        order_col: Union[str, List[str], None] = None,
        target_col: Optional[str] = None
    ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cumvar(
            table, schema, column, order_col, target_col)
