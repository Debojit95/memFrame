from typing import Any, Dict, Optional, Union

from memframe.core.analytix.arithmetic import ArithmeticOps
from memframe.utils.method_call_logger import record_call


class ArithmeticOrchestrator:
    """
    User‑facing arithmetic methods with a pandas‑inspired API.
    Accessed via `ops.arithmetic`.
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._memframe = memframe_ops_instance.memframe   
        self._data_id = memframe_ops_instance._data_id
        self._arithmetic_ops = None

    
    async def _ensure_ops(self) -> ArithmeticOps:
        if self._arithmetic_ops is None:
            await self._ops_parent._ensure_adapter()
            self._arithmetic_ops = ArithmeticOps(self._ops_parent._adapter)
        return self._arithmetic_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    # ------------------------------------------------------------------
    #  Binary operations
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def add(self, col1: Union[str, float, int], col2: Union[str, float, int],
                  target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.add(table, schema, col1, col2, target_col)

    @record_call(deep_cache=True)
    async def subtract(self, col1: Union[str, float, int], col2: Union[str, float, int],
                       target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.subtract(table, schema, col1, col2, target_col)

    @record_call(deep_cache=True)
    async def sub(self, col1: Union[str, float, int], col2: Union[str, float, int],
                       target_col: Optional[str] = None) -> Dict[str, Any]:
        return await self.subtract(col1, col2, target_col) 
    
    @record_call(deep_cache=True)
    async def mul(self, col1: Union[str, float, int], col2: Union[str, float, int],
                       target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.multiply(table, schema, col1, col2, target_col)


    @record_call(deep_cache=True)
    async def div(self, col1: Union[str, float, int], col2: Union[str, float, int],
                     target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.divide(table, schema, col1, col2, target_col)

    @record_call(deep_cache=True)
    async def mod(self, col1: Union[str, float, int], col2: Union[str, float, int],
                     target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.modulo(table, schema, col1, col2, target_col)

    @record_call(deep_cache=True)
    async def pow(self, col1: Union[str, float, int], col2: Union[str, float, int],
                    target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.power(table, schema, col1, col2, target_col)
    # ------------------------------------------------------------------
    #  Unary operations
    # ------------------------------------------------------------------
    @record_call(deep_cache=True)
    async def abs(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.absolute(table, schema, column, target_col)
    
    @record_call(deep_cache=True)
    async def negate(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.negate(table, schema, column, target_col)

    @record_call(deep_cache=True)
    async def round(self, column: str, digits: int = 0,
                    target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.round(table, schema, column, digits, target_col)

    @record_call(deep_cache=True)
    async def ceil(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.ceil(table, schema, column, target_col)

    @record_call(deep_cache=True)
    async def floor(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.floor(table, schema, column, target_col)

    @record_call(deep_cache=True)
    async def truncate(self, column: str, digits: int = 0, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.truncate(table, schema, column, digits, target_col)

    
    # ------------------------------------------------------------------
    #  Exp / Log / Root
    # ------------------------------------------------------------------
    @record_call
    async def exp(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.exp(table, schema, column, target_col)

    @record_call
    async def log(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.log(table, schema, column, target_col)

   
    # ------------------------------------------------------------------
    #  Trigonometric
    # ------------------------------------------------------------------
    @record_call
    async def sin(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.sin(table, schema, column, target_col)

    @record_call
    async def cos(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.cos(table, schema, column, target_col)

    @record_call
    async def tan(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.tan(table, schema, column, target_col)

    @record_call
    async def asin(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.asin(table, schema, column, target_col)

    @record_call
    async def acos(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.acos(table, schema, column, target_col)

    @record_call
    async def atan(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.atan(table, schema, column, target_col)

    @record_call
    async def atan2(self, col1: Union[str, float, int], col2: Union[str, float, int],
                    target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.atan2(table, schema, col1, col2, target_col)

    # ------------------------------------------------------------------
    #  Complex operations
    # ------------------------------------------------------------------
    @record_call
    async def weighted_sum(self, col1: Union[str, float, int], col2: Union[str, float, int],
                               weight1: float = 1, weight2: float = 1, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.weighted_average(table, schema, col1, col2, weight1, weight2, target_col)

    @record_call
    async def percentage_change(self, old_col: str, new_col: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.percentage_change(table, schema, old_col, new_col, target_col)

    @record_call
    async def normalize_range(self, column: str, target_col: Optional[str] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.normalize_range(table, schema, column, target_col)
