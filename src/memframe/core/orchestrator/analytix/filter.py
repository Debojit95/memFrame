from typing import Optional, Union
from memframe.core.analytix.filter.filter_II import DataFilteringOps, make_filtering_ops
from memframe.core.analytix.filter.filter_I import Predicate
from memframe.utils.str_filter_parser import parse_filter_string
from memframe.cache import record_call


class FilteringOrchestrator:
    """
    Pandas‑like filtering API.
    Access: ctx.filters.filter(predicate)
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance          # ContextManager
        self._memframe = memframe_ops_instance.memframe   # MemFrame instance
        self._data_id = memframe_ops_instance._data_id
        self._filtering_ops = None

    async def _ensure_ops(self) -> DataFilteringOps:
        if self._filtering_ops is None:
            await self._ops_parent._ensure_adapter()
            backend = self._memframe._backend.backend
            self._filtering_ops = make_filtering_ops(
                self._ops_parent._adapter, backend
            )
        return self._filtering_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    @record_call
    async def filter(
        self,
        predicate: Union["Predicate", str],
        columns: Union[str, list] = "*",
        chunk_size: Optional[int] = None,
        create_flag: bool = False,
    ):
        """
        Apply a filter and create a new transient table.

        Parameters
        ----------
        predicate : Predicate or str
            A predicate object built with F.num.gt(...), or a string expression
            like ``"col1 > col2 && col4 != col5"``.
        columns : str or list of str
            Columns to select (default ``*``).
        chunk_size : int, optional
            If given, returns an async iterator yielding DataFrames of that size.
        create_flag : bool
            If True and the match is a proper subset, write a boolean
            ``filter_flag`` column in place onto the source table (True for
            matching rows). Empty/full matches skip the flag.

        Returns
        -------
        dict
            Standard operation response with ``is_error``, ``result``/``iterator``,
            ``new_table``, and ``flag_column`` (name or None).
        """
        # Convert string to Predicate if needed
        if isinstance(predicate, str):
            predicate = parse_filter_string(predicate)

        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend
        data_id = self._data_id or self._memframe._active_id

        result = await ops.filter_table(
            table=table,
            schema=schema,
            predicate=predicate,
            columns=columns,
            backend=backend,
            data_id=data_id,
            chunk_size=chunk_size,
            create_flag=create_flag,
        )

        return result


# Alias for convenience
FilterAccessor = FilteringOrchestrator
