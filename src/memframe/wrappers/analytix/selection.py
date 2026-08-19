# wrappers/selection_wrapper.py

from typing import Any, List, Optional, Tuple, Union

from memframe.core.orchestrator.analytix.selection import SelectionOrchestrator
from memframe.utils.async_sync import async_to_sync


class SelectionWrapper(SelectionOrchestrator):
    """
    Sync + async wrapper over SelectionOrchestrator.

    Naming convention:
        async -> aasof(), aloc(), ...
        sync  -> asof(), loc(), ...
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the selection wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # asof
    # ------------------------------------------------------------------

    async def aasof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        """Asynchronously perform as-of selection up to reference values."""
        return await super().asof(
            where=where,
            on=on,
            subset=subset,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def asof(
        self,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        """Synchronously perform as-of selection up to reference values."""
        return await self.aasof(
            where=where,
            on=on,
            subset=subset,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # at
    # ------------------------------------------------------------------

    async def aat(
        self,
        row_label,
        column_label: str,
        index_column: str = None,
    ):
        """Asynchronously access a scalar value by row/column labels."""
        return await super().at(
            row_label=row_label,
            column_label=column_label,
            index_column=index_column,
        )

    @async_to_sync
    async def at(
        self,
        row_label,
        column_label: str,
        index_column: str = None,
    ):
        """Synchronously access a scalar value by row/column labels."""
        return await self.aat(
            row_label=row_label,
            column_label=column_label,
            index_column=index_column,
        )

    # ------------------------------------------------------------------
    # iat
    # ------------------------------------------------------------------

    async def aiat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ):
        """Asynchronously access a scalar value by integer row position."""
        return await super().iat(
            row_position=row_position,
            column_label=column_label,
            order_by=order_by,
        )

    @async_to_sync
    async def iat(
        self,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ):
        """Synchronously access a scalar value by integer row position."""
        return await self.aiat(
            row_position=row_position,
            column_label=column_label,
            order_by=order_by,
        )

    # ------------------------------------------------------------------
    # get
    # ------------------------------------------------------------------

    async def aget(
        self,
        keys: Union[str, List[str]],
        default: Any = None,
    ):
        """Asynchronously retrieve one or more columns with default fallback."""
        return await super().get(
            keys=keys,
            default=default,
        )

    @async_to_sync
    async def get(
        self,
        keys: Union[str, List[str]],
        default: Any = None,
    ):
        """Synchronously retrieve one or more columns with default fallback."""
        return await self.aget(
            keys=keys,
            default=default,
        )

    # ------------------------------------------------------------------
    # select_dtypes
    # ------------------------------------------------------------------

    async def aselect_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        """Asynchronously select columns by included/excluded dtypes."""
        return await super().select_dtypes(
            include=include,
            exclude=exclude,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def select_dtypes(
        self,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        chunk_size: int = None,
    ):
        """Synchronously select columns by included/excluded dtypes."""
        return await self.aselect_dtypes(
            include=include,
            exclude=exclude,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # iloc
    # ------------------------------------------------------------------

    async def ailoc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = None,
        col_indexer: Union[int, List[int], slice, list, str] = None,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
        index_column: str = None,
    ):
        """Asynchronously select rows/columns by integer-location indexing."""
        return await super().iloc(
            row_indexer=row_indexer,
            col_indexer=col_indexer,
            columns=columns,
            index_column=index_column,
        )

    @async_to_sync
    async def iloc(
        self,
        row_indexer: Union[int, List[int], slice, list, str, tuple] = None,
        col_indexer: Union[int, List[int], slice, list, str] = None,
        columns: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
        index_column: str = None,
    ):
        """Synchronously select rows/columns by integer-location indexing."""
        return await self.ailoc(
            row_indexer=row_indexer,
            col_indexer=col_indexer,
            columns=columns,
            index_column=index_column,
        )

SelectionAccessor = SelectionWrapper
