from __future__ import annotations

from typing import Any, Dict, List, Optional

from memframe.core.orchestrator.analytix.inspection import TableOpsOrchestrator


class TableOpsWrapper(TableOpsOrchestrator):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    async def ahead(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    def head(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    async def atail(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    def tail(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    async def asample(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
        random_state: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def sample(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
        random_state: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    async def ainfo(self) -> Dict[str, Any]: ...
    def info(self) -> Dict[str, Any]: ...

    async def adescribe(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    def describe(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    async def anull_analysis(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    def null_analysis(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]: ...

    async def afull_table(
        self,
        columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def full_table(
        self,
        columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    async def aastype(
        self,
        columns: Optional[List[str]] = None,
        dtypes: Optional[List[str]] = None,
        dtype_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]: ...

    def astype(
        self,
        columns: Optional[List[str]] = None,
        dtypes: Optional[List[str]] = None,
        dtype_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]: ...

    async def ainsert(self, column: str, value: Any) -> Dict[str, Any]: ...
    def insert(self, column: str, value: Any) -> Dict[str, Any]: ...

    async def amap(
        self,
        func: str,
        na_action: Optional[str] = None,
        columns: Optional[List[str]] = None,
        datetime_action: str = "skip",
    ) -> Dict[str, Any]: ...

    def map(
        self,
        func: str,
        na_action: Optional[str] = None,
        columns: Optional[List[str]] = None,
        datetime_action: str = "skip",
    ) -> Dict[str, Any]: ...

    async def arename(self, columns: Dict[str, str]) -> Dict[str, Any]: ...
    def rename(self, columns: Dict[str, str]) -> Dict[str, Any]: ...

    async def aset_index(self, columns: List[str]) -> Dict[str, Any]: ...
    def set_index(self, columns: List[str]) -> Dict[str, Any]: ...

    async def aupdate(
        self,
        on: str,
        other_table: str,
        other_schema: str = "upload",
        overwrite: bool = True,
        errors: str = "ignore",
    ) -> Dict[str, Any]: ...

    def update(
        self,
        on: str,
        other_table: str,
        other_schema: str = "upload",
        overwrite: bool = True,
        errors: str = "ignore",
    ) -> Dict[str, Any]: ...

    async def aresample(
        self,
        time_column: str,
        rule: str,
        agg: str = "COUNT",
        value_column: Optional[str] = None,
        label: str = "left",
        closed: str = "left",
    ) -> Dict[str, Any]: ...

    def resample(
        self,
        time_column: str,
        rule: str,
        agg: str = "COUNT",
        value_column: Optional[str] = None,
        label: str = "left",
        closed: str = "left",
    ) -> Dict[str, Any]: ...

    async def adata_quality_missing_values(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    def data_quality_missing_values(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    async def adata_quality_completeness_score(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    def data_quality_completeness_score(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    async def acomprehensive_numeric_summary(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    def comprehensive_numeric_summary(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    async def astatistical_profile_report(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    def statistical_profile_report(
        self,
        columns: List[str],
    ) -> Dict[str, Any]: ...

    async def acolumns(self) -> Dict[str, Any]: ...
    def columns(self) -> Dict[str, Any]: ...

    async def adtypes(self) -> Dict[str, Any]: ...
    def dtypes(self) -> Dict[str, Any]: ...

    async def ashape(self) -> Dict[str, Any]: ...
    def shape(self) -> Dict[str, Any]: ...


TableOpsAccessor = TableOpsWrapper
