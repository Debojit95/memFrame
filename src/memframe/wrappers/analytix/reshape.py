# wrappers/reshaping_wrapper.py

from typing import List, Optional, Union

from memframe.core.orchestrator.analytix.reshape import ReshapingOrchestrator
from memframe.utils.async_sync import async_to_sync


class ReshapingWrapper(ReshapingOrchestrator):
    """
    Sync + async wrapper over ReshapingOrchestrator.

    Naming convention:
        async -> aexplode(), amelt(), ...
        sync  -> explode(), melt(), ...
    """

    def __init__(self, memframe_ops_instance):
        """Initialize the reshaping wrapper."""
        super().__init__(memframe_ops_instance)

    # ------------------------------------------------------------------
    # explode
    # ------------------------------------------------------------------

    async def aexplode(
        self,
        column: Union[str, List[str]],
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously explode list-like values into separate rows."""
        return await super().explode(column, chunk_size)

    @async_to_sync
    async def explode(
        self,
        column: Union[str, List[str]],
        chunk_size: Optional[int] = None,
    ):
        """Synchronously explode list-like values into separate rows."""
        return await self.aexplode(column, chunk_size)

    # ------------------------------------------------------------------
    # melt
    # ------------------------------------------------------------------

    async def amelt(
        self,
        id_vars,
        value_vars,
        var_name: str = "variable",
        value_name: str = "value",
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously unpivot columns into long format."""
        return await super().melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def melt(
        self,
        id_vars,
        value_vars,
        var_name: str = "variable",
        value_name: str = "value",
        chunk_size: Optional[int] = None,
    ):
        """Synchronously unpivot columns into long format."""
        return await self.amelt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # pivot
    # ------------------------------------------------------------------

    async def apivot(
        self,
        index,
        columns,
        values,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously pivot data from long to wide format."""
        return await super().pivot(
            index=index,
            columns=columns,
            values=values,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def pivot(
        self,
        index,
        columns,
        values,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously pivot data from long to wide format."""
        return await self.apivot(
            index=index,
            columns=columns,
            values=values,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # pivot_table
    # ------------------------------------------------------------------

    async def apivot_table(
        self,
        index=None,
        columns=None,
        values=None,
        aggfunc: str = "mean",
        fill_value=None,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously create a pivot table with aggregation."""
        return await super().pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=fill_value,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def pivot_table(
        self,
        index=None,
        columns=None,
        values=None,
        aggfunc: str = "mean",
        fill_value=None,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously create a pivot table with aggregation."""
        return await self.apivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=fill_value,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # crosstab
    # ------------------------------------------------------------------

    async def acrosstab(
        self,
        index,
        columns,
        values=None,
        aggfunc=None,
        margins: bool = False,
        normalize: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously compute a cross-tabulation table."""
        return await super().crosstab(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            margins=margins,
            normalize=normalize,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def crosstab(
        self,
        index,
        columns,
        values=None,
        aggfunc=None,
        margins: bool = False,
        normalize: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously compute a cross-tabulation table."""
        return await self.acrosstab(
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            margins=margins,
            normalize=normalize,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # transpose
    # ------------------------------------------------------------------

    async def atranspose(
        self,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously transpose rows and columns."""
        return await super().transpose(chunk_size)

    @async_to_sync
    async def transpose(
        self,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously transpose rows and columns."""
        return await self.atranspose(chunk_size)

    # ------------------------------------------------------------------
    # rank
    # ------------------------------------------------------------------

    async def arank(
        self,
        columns,
        method: str = "average",
        na_option: str = "keep",
        ascending: bool = True,
        pct: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously rank values within selected columns."""
        return await super().rank(
            columns=columns,
            method=method,
            na_option=na_option,
            ascending=ascending,
            pct=pct,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def rank(
        self,
        columns,
        method: str = "average",
        na_option: str = "keep",
        ascending: bool = True,
        pct: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously rank values within selected columns."""
        return await self.arank(
            columns=columns,
            method=method,
            na_option=na_option,
            ascending=ascending,
            pct=pct,
            chunk_size=chunk_size,
        )

    # ------------------------------------------------------------------
    # groupby_rank
    # ------------------------------------------------------------------

    async def agroupby_rank(
        self,
        groupby,
        columns,
        method: str = "average",
        ascending: bool = True,
        na_option: str = "keep",
        pct: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Asynchronously rank values within groups."""
        return await super().groupby_rank(
            groupby=groupby,
            columns=columns,
            method=method,
            ascending=ascending,
            na_option=na_option,
            pct=pct,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def groupby_rank(
        self,
        groupby,
        columns,
        method: str = "average",
        ascending: bool = True,
        na_option: str = "keep",
        pct: bool = False,
        chunk_size: Optional[int] = None,
    ):
        """Synchronously rank values within groups."""
        return await self.agroupby_rank(
            groupby=groupby,
            columns=columns,
            method=method,
            ascending=ascending,
            na_option=na_option,
            pct=pct,
            chunk_size=chunk_size,
        )


ReshapeAccessor = ReshapingWrapper
