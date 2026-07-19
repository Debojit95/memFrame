from typing import Optional, Dict, Any, List
from utils.method_call_logger import record_call
from core.analytix.table_ops import GeneralTableOps


class TableOpsOrchestrator:
    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance          # ContextManager
        self._memframe = memframe_ops_instance.memframe   # MemFrame
        self._data_id = memframe_ops_instance._data_id    # explicit data_id
        self._table_ops = None

          

    async def _ensure_ops(self) -> GeneralTableOps:
        if self._table_ops is None:
            await self._ops_parent._ensure_adapter()
            self._table_ops = GeneralTableOps(self._ops_parent._adapter)
        return self._table_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    async def head(self, n: int = 10, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_head(table, schema, n=n, columns=columns)

    async def tail(self, n: int = 10, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_tail(table, schema, n=n, columns=columns)

    async def sample(self, n: int = 10, columns: Optional[List[str]] = None, random_state: Optional[int] = None, ) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_sample(
            table,
            schema,
            n=n,
            columns=columns,
            random_state=random_state,
        )

    @record_call
    async def info(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_info(table, schema)

    @record_call
    async def describe(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_describe(table, schema, columns=columns)

    @record_call
    async def null_analysis(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if columns == "*" or columns == ["*"]:
            columns = None
        if columns is not None and not isinstance(columns, list):
            return {
                "is_error": True,
                "message": "",
                "error_message": "columns must be a list of column names or '*'",
                "involved_cols": [],
                "generated_cols": [],
            }

        return await ops.dataframe_null_analysis(table, schema, columns=columns)

    @record_call
    async def corr(self, columns: Optional[List[str]] = None, method: str = "pearson",) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_correlation_analysis(
            table,
            schema,
            columns=columns,
            method=method,
        )

    async def full_table(self,  columns: Optional[List[str]] = None,chunk_size: Optional[int] = None,) -> Dict[str, Any]:    
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_full_table(
            table,
            schema,
            columns=columns,
            chunk_size=chunk_size,
        )

    async def astype(self, columns: Optional[List[str]] = None, dtypes: Optional[List[str]] = None,  dtype_map: Optional[Dict[str, str]] = None,) -> Dict[str, Any]:
        
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        if dtype_map is None:
            if columns is None or dtypes is None:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "Provide either dtype_map OR (columns + dtypes)",
                    "involved_cols": [],
                    "generated_cols": [],
                }

            if len(columns) != len(dtypes):
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": "columns and dtypes length mismatch",
                    "involved_cols": [],
                    "generated_cols": [],
                }

            dtype_map = dict(zip(columns, dtypes))

        return await ops.dataframe_astype(table, schema, dtype_map=dtype_map)


    async def insert(self, column: str, value: Any) -> Dict[str, Any]:
        ops = await self._ensure_ops()

        if not isinstance(value, list):
            return {
                "is_error": True,
                "message": "",
                "error_message": "Value must be a list",
                "involved_cols": [],
                "generated_cols": [],
            }

        table, schema = await self._get_context()

        return await ops.dataframe_insert(
            table,
            schema,
            column=column,
            value=value,
        )

    @record_call
    async def map(self, func: str, na_action: Optional[str] = None,columns: Optional[List[str]] = None,datetime_action: str = "skip",):
        
        ops = await self._ensure_ops()

        if datetime_action not in ["skip", "cast_string", "extract_epoch", "keep", "error"]:
            return {
                "is_error": True,
                "message": "",
                "error_message": "Invalid datetime_action",
                "involved_cols": [],
                "generated_cols": [],
            }

        table, schema = await self._get_context()

        return await ops.dataframe_map(
            table,
            schema,
            func=func,
            na_action=na_action,
            columns=columns,
            datetime_action=datetime_action,
        )

    @record_call
    async def rename(self, columns: Dict[str, str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_rename(table, schema, columns=columns)

    @record_call
    async def set_index(self, columns: List[str]) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_set_index(table, schema, columns=columns)

    @record_call
    async def reset_index(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_reset_index(table, schema)

    @record_call
    async def update(self, on: str, other_table: str, other_schema: str = "upload",overwrite: bool = True, errors: str = "ignore",) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        if other_schema == "upload":
            other_schema = schema

        return await ops.dataframe_update(
            table,
            schema,
            other_table=other_table,
            other_schema=other_schema,
            on=on,
            overwrite=overwrite,
            errors=errors,
        )

    @record_call
    async def resample( self,time_column: str, rule: str,agg: str = "COUNT",  value_column: Optional[str] = None,label: str = "left", closed: str = "left",) -> Dict[str, Any]:
        
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        return await ops.dataframe_resample(
            table,
            schema,
            time_column=time_column,
            rule=rule,
            agg=agg,
            value_column=value_column,
            label=label,
            closed=closed,
        )

    async def axes(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_axes(table, schema)

    async def columns(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_columns(table, schema)

    async def dtypes(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_dtypes(table, schema)

    async def first_valid_index(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_first_valid_index(table, schema)

    async def memory_usage(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_memory_usage(table, schema)

    async def ndim(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_ndim(table, schema)

    async def shape(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_shape(table, schema)

    async def size(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_size(table, schema)

    async def values(self) -> Dict[str, Any]:
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_values(table, schema)

    async def items(self):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_items(table, schema)

    async def iterrows(self):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_iterrows(table, schema)

    async def itertuples(self, index: bool = True):
        ops = await self._ensure_ops()
        table, schema = await self._get_context()
        return await ops.dataframe_itertuples(table, schema, index=index)
