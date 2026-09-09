# sorting_wrapper.pyi

from typing import Any, Dict, List, Union

from memframe.core.orchestrator.analytix.sorting import SortingOrchestrator


class SortingWrapper(SortingOrchestrator):
    def __init__(self, memframe_ops_instance) -> None: ...

    async def asort_values(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = "last",
        columns: Union[str, List[str]] = "*",
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...

    def sort_values(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = "last",
        columns: Union[str, List[str]] = "*",
        chunk_size: int = None,
    ) -> Dict[str, Any]: ...


SortingAccessor = SortingWrapper
