from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from core.orchestrator.analytix.cleaning import CleaningOrchestrator


class CleaningWrapper(CleaningOrchestrator):

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    async def afillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        mapping: Optional[Dict[Any, Any]] = None,
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def fillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        mapping: Optional[Dict[Any, Any]] = None,
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    async def aclip(
        self,
        column: str,
        lower: int | float = None,
        upper: int | float = None,
    ) -> Dict[str, Any]: ...

    def clip(
        self,
        column: str,
        lower: int | float = None,
        upper: int | float = None,
    ) -> Dict[str, Any]: ...

    async def adrop_outliers(
        self,
        column: str,
        z_thresh: float = 3.0,
    ) -> Dict[str, Any]: ...

    def drop_outliers(
        self,
        column: str,
        z_thresh: float = 3.0,
    ) -> Dict[str, Any]: ...

    async def ato_numeric(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def to_numeric(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def amap_values(
        self,
        column: str,
        mapping: Dict[Any, Any],
    ) -> Dict[str, Any]: ...

    def map_values(
        self,
        column: str,
        mapping: Dict[Any, Any],
    ) -> Dict[str, Any]: ...

    async def afilter_valid(
        self,
        column: str,
        valid_values: List[Any],
    ) -> Dict[str, Any]: ...

    def filter_valid(
        self,
        column: str,
        valid_values: List[Any],
    ) -> Dict[str, Any]: ...

    async def acompress_rare(
        self,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
    ) -> Dict[str, Any]: ...

    def compress_rare(
        self,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
    ) -> Dict[str, Any]: ...

    async def afix_dates(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    def fix_dates(
        self,
        column: str,
    ) -> Dict[str, Any]: ...

    async def aclip_dates(
        self,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def clip_dates(
        self,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    async def agroupby_fillna(
        self,
        column: str,
        group_cols: List[str],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def groupby_fillna(
        self,
        column: str,
        group_cols: List[str],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    async def adropna(
        self,
        axis: int = 0,
        how: str = "any",
        thresh: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def dropna(
        self,
        axis: int = 0,
        how: str = "any",
        thresh: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    async def adrop(
        self,
        columns: Optional[List[str]] = None,
        axis: int = 0,
        index: Optional[List[int]] = None,
    ) -> Dict[str, Any]: ...

    def drop(
        self,
        columns: Optional[List[str]] = None,
        axis: int = 0,
        index: Optional[List[int]] = None,
    ) -> Dict[str, Any]: ...

    async def aisna(self) -> Dict[str, Any]: ...
    def isna(self) -> Dict[str, Any]: ...

    async def anotna(self) -> Dict[str, Any]: ...
    def notna(self) -> Dict[str, Any]: ...

    async def adrop_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
    ) -> Dict[str, Any]: ...

    def drop_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
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