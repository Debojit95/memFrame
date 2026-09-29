from typing import Optional, Union
from memframe.core.analytix.filter_II import DataFilteringOps
from memframe.core.analytix.filter_I import Predicate
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

    @classmethod
    def replay_create(cls, memframe, data_id: str):
        from memframe.db_manager.context import ContextManager
        ctx = ContextManager(memframe, data_id=data_id)
        return cls(ctx)

    @classmethod
    def from_context(cls, memframe, data_id):
        return cls.replay_create(memframe, data_id)

    async def _ensure_ops(self) -> DataFilteringOps:
        if self._filtering_ops is None:
            await self._ops_parent._ensure_adapter()
            backend = self._memframe._backend.backend
            self._filtering_ops = DataFilteringOps(
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

        Returns
        -------
        dict
            Standard operation response with ``is_error``, ``result``/``iterator``,
            and ``new_table``.
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
        )

        return result


# Alias for convenience
FilterAccessor = FilteringOrchestrator
