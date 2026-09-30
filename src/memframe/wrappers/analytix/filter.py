from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from memframe.core.orchestrator.analytix.filter import FilteringOrchestrator
from memframe.core.analytix.filter.filter_I import Predicate
from memframe.utils.async_sync import async_to_sync


class FilteringWrapper(
    FilteringOrchestrator,
):
    """
    Thin wrapper over FilteringOrchestrator.

    Purpose
    -------
    ✔ Stable public API layer
    ✔ Interface enforcement
    ✔ Future extension point
    ✔ Dependency injection friendly
    ✔ SDK-safe abstraction
    ✔ Replay compatible
    """

    async def afilter(
        self,
        predicate: Union[Predicate, str],
        columns: Union[str, List[str]] = "*",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Apply dataset filtering.

        Parameters
        ----------
        predicate:
            Predicate object OR expression string.

        columns:
            Columns to return.

        chunk_size:
            Optional chunk iterator size.

        Returns
        -------
        Dict[str, Any]
        """

        return await super().filter(
            predicate=predicate,
            columns=columns,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def filter(
        self,
        predicate: Union[Predicate, str],
        columns: Union[str, List[str]] = "*",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self.afilter(
            predicate=predicate,
            columns=columns,
            chunk_size=chunk_size,
        )

    def __call__(
        self,
        predicate: Union[Predicate, str],
        columns: Union[str, List[str]] = "*",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Allow direct call style:
            ops.filter("A > B")
        """
        return self.filter(
            predicate=predicate,
            columns=columns,
            chunk_size=chunk_size,
        )


# Alias
FilterAccessor = FilteringWrapper
