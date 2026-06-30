from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.async_sync import async_to_sync
from src.core.orchestrator.analytix.cleaning import CleaningOrchestrator

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(handler)


class CleaningWrapper(CleaningOrchestrator):
    """Wrapper around `CleaningOrchestrator` with async/sync method pairs.

    Each operation is exposed as:
    - an async method prefixed with `a` (for example, `afillna`)
    - a sync-friendly counterpart (for example, `fillna`) decorated with
      `@async_to_sync`
    """

    def __init__(self, *args, **kwargs):
        """Initialize the cleaning wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    # ==========================================================
    # FILLNA
    # ==========================================================

    async def afillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        mapping: Optional[Dict[Any, Any]] = None,
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously fill missing values in a column."""
        return await super().fillna(
            column=column,
            value=value,
            method=method,
            mapping=mapping,
            dtype=dtype,
        )

    @async_to_sync
    async def fillna(
        self,
        column: str,
        value: Optional[Any] = None,
        method: str = "mean",
        mapping: Optional[Dict[Any, Any]] = None,
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously fill missing values in a column."""
        return await self.afillna(
            column=column,
            value=value,
            method=method,
            mapping=mapping,
            dtype=dtype,
        )

    # ==========================================================
    # CLIP
    # ==========================================================

    async def aclip(
        self,
        column: str,
        lower: int | float = None,
        upper: int | float = None,
    ) -> Dict[str, Any]:
        """Asynchronously clip column values to lower/upper bounds."""
        return await super().clip(
            column=column,
            lower=lower,
            upper=upper,
        )

    @async_to_sync
    async def clip(
        self,
        column: str,
        lower: int | float = None,
        upper: int | float = None,
    ) -> Dict[str, Any]:
        """Synchronously clip column values to lower/upper bounds."""
        return await self.aclip(
            column=column,
            lower=lower,
            upper=upper,
        )

    # ==========================================================
    # DROP OUTLIERS
    # ==========================================================

    async def adrop_outliers(
        self,
        column: str,
        z_thresh: float = 3.0,
    ) -> Dict[str, Any]:
        """Asynchronously drop outlier rows using a z-score threshold."""
        return await super().drop_outliers(
            column=column,
            z_thresh=z_thresh,
        )

    @async_to_sync
    async def drop_outliers(
        self,
        column: str,
        z_thresh: float = 3.0,
    ) -> Dict[str, Any]:
        """Synchronously drop outlier rows using a z-score threshold."""
        return await self.adrop_outliers(
            column=column,
            z_thresh=z_thresh,
        )

    # ==========================================================
    # TO NUMERIC
    # ==========================================================

    async def ato_numeric(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Asynchronously coerce a column to numeric dtype."""
        return await super().to_numeric(column=column)

    @async_to_sync
    async def to_numeric(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Synchronously coerce a column to numeric dtype."""
        return await self.ato_numeric(column=column)

    # ==========================================================
    # MAP VALUES
    # ==========================================================

    async def amap_values(
        self,
        column: str,
        mapping: Dict[Any, Any],
    ) -> Dict[str, Any]:
        """Asynchronously map column values using a mapping dictionary."""
        return await super().map_values(
            column=column,
            mapping=mapping,
        )

    @async_to_sync
    async def map_values(
        self,
        column: str,
        mapping: Dict[Any, Any],
    ) -> Dict[str, Any]:
        """Synchronously map column values using a mapping dictionary."""
        return await self.amap_values(
            column=column,
            mapping=mapping,
        )

    # ==========================================================
    # FILTER VALID
    # ==========================================================

    async def afilter_valid(
        self,
        column: str,
        valid_values: List[Any],
    ) -> Dict[str, Any]:
        """Asynchronously keep rows whose values are in a valid set."""
        return await super().filter_valid(
            column=column,
            valid_values=valid_values,
        )

    @async_to_sync
    async def filter_valid(
        self,
        column: str,
        valid_values: List[Any],
    ) -> Dict[str, Any]:
        """Synchronously keep rows whose values are in a valid set."""
        return await self.afilter_valid(
            column=column,
            valid_values=valid_values,
        )

    # ==========================================================
    # COMPRESS RARE
    # ==========================================================

    async def acompress_rare(
        self,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
    ) -> Dict[str, Any]:
        """Asynchronously compress low-frequency categories into one label."""
        return await super().compress_rare(
            column=column,
            min_count=min_count,
            other_label=other_label,
        )

    @async_to_sync
    async def compress_rare(
        self,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
    ) -> Dict[str, Any]:
        """Synchronously compress low-frequency categories into one label."""
        return await self.acompress_rare(
            column=column,
            min_count=min_count,
            other_label=other_label,
        )

    # ==========================================================
    # FIX DATES
    # ==========================================================

    async def afix_dates(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Asynchronously parse and normalize date values in a column."""
        return await super().fix_dates(column=column)

    @async_to_sync
    async def fix_dates(
        self,
        column: str,
    ) -> Dict[str, Any]:
        """Synchronously parse and normalize date values in a column."""
        return await self.afix_dates(column=column)

    # ==========================================================
    # CLIP DATES
    # ==========================================================

    async def aclip_dates(
        self,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously clip date values to optional min/max bounds."""
        return await super().clip_dates(
            column=column,
            min_dt=min_dt,
            max_dt=max_dt,
        )

    @async_to_sync
    async def clip_dates(
        self,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously clip date values to optional min/max bounds."""
        return await self.aclip_dates(
            column=column,
            min_dt=min_dt,
            max_dt=max_dt,
        )

    # ==========================================================
    # GROUPBY FILLNA
    # ==========================================================

    async def agroupby_fillna(
        self,
        column: str,
        group_cols: List[str],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Asynchronously fill missing values using group-wise statistics."""
        return await super().groupby_fillna(
            column=column,
            group_cols=group_cols,
            value=value,
            method=method,
            dtype=dtype,
        )

    @async_to_sync
    async def groupby_fillna(
        self,
        column: str,
        group_cols: List[str],
        value: Optional[Any] = None,
        method: str = "mean",
        dtype: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Synchronously fill missing values using group-wise statistics."""
        return await self.agroupby_fillna(
            column=column,
            group_cols=group_cols,
            value=value,
            method=method,
            dtype=dtype,
        )

    # ==========================================================
    # DROPNA
    # ==========================================================

    async def adropna(
        self,
        axis: int = 0,
        how: str = "any",
        thresh: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously drop missing data by axis/how/thresh settings."""
        return await super().dropna(
            axis=axis,
            how=how,
            thresh=thresh,
        )

    @async_to_sync
    async def dropna(
        self,
        axis: int = 0,
        how: str = "any",
        thresh: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously drop missing data by axis/how/thresh settings."""
        return await self.adropna(
            axis=axis,
            how=how,
            thresh=thresh,
        )

    # ==========================================================
    # DROP
    # ==========================================================

    async def adrop(
        self,
        columns: Optional[List[str]] = None,
        axis: int = 0,
        index: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously drop rows or columns by labels/axis."""
        return await super().drop(
            axis=axis,
            index=index,
            columns=columns,
        )

    @async_to_sync
    async def drop(
        self,
        columns: Optional[List[str]] = None,
        axis: int = 0,
        index: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Synchronously drop rows or columns by labels/axis."""
        return await self.adrop(
            axis=axis,
            index=index,
            columns=columns,
        )

    # ==========================================================
    # ISNA
    # ==========================================================

    async def aisna(self) -> Dict[str, Any]:
        """Asynchronously return NA/null indicator mask."""
        return await super().isna()

    @async_to_sync
    async def isna(self) -> Dict[str, Any]:
        """Synchronously return NA/null indicator mask."""
        return await self.aisna()

    # ==========================================================
    # NOTNA
    # ==========================================================

    async def anotna(self) -> Dict[str, Any]:
        """Asynchronously return non-null indicator mask."""
        return await super().notna()

    @async_to_sync
    async def notna(self) -> Dict[str, Any]:
        """Synchronously return non-null indicator mask."""
        return await self.anotna()

    # ==========================================================
    # DROP DUPLICATES
    # ==========================================================

    async def adrop_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
    ) -> Dict[str, Any]:
        """Asynchronously remove duplicate rows with keep strategy."""
        return await super().drop_duplicates(
            subset=subset,
            keep=keep,
        )

    @async_to_sync
    async def drop_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
    ) -> Dict[str, Any]:
        """Synchronously remove duplicate rows with keep strategy."""
        return await self.adrop_duplicates(
            subset=subset,
            keep=keep,
        )

    # ==========================================================
    # DATA QUALITY
    # ==========================================================

    async def adata_quality_missing_values(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously compute missing-value data-quality metrics."""
        return await super().data_quality_missing_values(columns=columns)

    @async_to_sync
    async def data_quality_missing_values(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Synchronously compute missing-value data-quality metrics."""
        return await self.adata_quality_missing_values(columns=columns)

    async def adata_quality_completeness_score(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously compute completeness scores for columns."""
        return await super().data_quality_completeness_score(columns=columns)

    @async_to_sync
    async def data_quality_completeness_score(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Synchronously compute completeness scores for columns."""
        return await self.adata_quality_completeness_score(columns=columns)

    # ==========================================================
    # COMPREHENSIVE
    # ==========================================================

    async def acomprehensive_numeric_summary(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously generate a comprehensive numeric summary."""
        return await super().comprehensive_numeric_summary(columns=columns)

    @async_to_sync
    async def comprehensive_numeric_summary(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Synchronously generate a comprehensive numeric summary."""
        return await self.acomprehensive_numeric_summary(columns=columns)

    async def astatistical_profile_report(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously generate a statistical profile report."""
        return await super().statistical_profile_report(columns=columns)

    @async_to_sync
    async def statistical_profile_report(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Synchronously generate a statistical profile report."""
        return await self.astatistical_profile_report(columns=columns)
