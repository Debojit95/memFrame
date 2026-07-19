from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.async_sync import async_to_sync
from core.orchestrator.analytix.table_ops import TableOpsOrchestrator

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


class TableOpsWrapper(TableOpsOrchestrator):
    """Wrapper around `TableOpsOrchestrator` with async/sync methods."""

    def __init__(self, *args, **kwargs):
        """Initialize the table operations wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    # ==========================================================
    # HEAD
    # ==========================================================

    async def ahead(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously return the first `n` rows."""
        return await super().head(n=n, columns=columns)

    @async_to_sync
    async def head(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Synchronously return the first `n` rows."""
        return await self.ahead(n=n, columns=columns)

    # ==========================================================
    # TAIL
    # ==========================================================

    async def atail(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously return the last `n` rows."""
        return await super().tail(n=n, columns=columns)

    @async_to_sync
    async def tail(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Synchronously return the last `n` rows."""
        return await self.atail(n=n, columns=columns)

    # ==========================================================
    # SAMPLE
    # ==========================================================

    async def asample(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
        random_state: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously sample `n` rows from the table."""
        return await super().sample(
            n=n,
            columns=columns,
            random_state=random_state,
        )

    @async_to_sync
    async def sample(
        self,
        n: int = 10,
        columns: Optional[List[str]] = None,
        random_state: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously sample `n` rows from the table."""
        return await self.asample(
            n=n,
            columns=columns,
            random_state=random_state,
        )

    # ==========================================================
    # INFO
    # ==========================================================

    async def ainfo(self) -> Dict[str, Any]:
        """Asynchronously return dataset information summary."""
        return await super().info()

    @async_to_sync
    async def info(self) -> Dict[str, Any]:
        """Synchronously return dataset information summary."""
        return await self.ainfo()

    # ==========================================================
    # DESCRIBE
    # ==========================================================

    async def adescribe(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously compute descriptive statistics."""
        return await super().describe(columns=columns)

    @async_to_sync
    async def describe(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Synchronously compute descriptive statistics."""
        return await self.adescribe(columns=columns)

    # ==========================================================
    # NULL ANALYSIS
    # ==========================================================

    async def anull_analysis(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously analyze null distribution across columns."""
        return await super().null_analysis(columns=columns)

    @async_to_sync
    async def null_analysis(
        self,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Synchronously analyze null distribution across columns."""
        return await self.anull_analysis(columns=columns)

    # ==========================================================
    # CORR
    # ==========================================================

    async def acorr(
        self,
        columns: Optional[List[str]] = None,
        method: str = "pearson",
    ) -> Dict[str, Any]:
        """Asynchronously compute correlation matrix for selected columns."""
        return await super().corr(
            columns=columns,
            method=method,
        )

    @async_to_sync
    async def corr(
        self,
        columns: Optional[List[str]] = None,
        method: str = "pearson",
    ) -> Dict[str, Any]:
        """Synchronously compute correlation matrix for selected columns."""
        return await self.acorr(
            columns=columns,
            method=method,
        )

    # ==========================================================
    # FULL TABLE
    # ==========================================================

    async def afull_table(
        self,
        columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Asynchronously return full table data, optionally chunked."""
        return await super().full_table(
            columns=columns,
            chunk_size=chunk_size,
        )

    @async_to_sync
    async def full_table(
        self,
        columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Synchronously return full table data, optionally chunked."""
        return await self.afull_table(
            columns=columns,
            chunk_size=chunk_size,
        )

    # ==========================================================
    # ASTYPE
    # ==========================================================

    async def aastype(
        self,
        columns: Optional[List[str]] = None,
        dtypes: Optional[List[str]] = None,
        dtype_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Asynchronously cast columns to target dtypes."""
        return await super().astype(
            columns=columns,
            dtypes=dtypes,
            dtype_map=dtype_map,
        )

    @async_to_sync
    async def astype(
        self,
        columns: Optional[List[str]] = None,
        dtypes: Optional[List[str]] = None,
        dtype_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Synchronously cast columns to target dtypes."""
        return await self.aastype(
            columns=columns,
            dtypes=dtypes,
            dtype_map=dtype_map,
        )

    # ==========================================================
    # INSERT
    # ==========================================================

    async def ainsert(
        self,
        column: str,
        value: Any,
    ) -> Dict[str, Any]:
        """Asynchronously insert or assign a column value."""
        return await super().insert(
            column=column,
            value=value,
        )

    @async_to_sync
    async def insert(
        self,
        column: str,
        value: Any,
    ) -> Dict[str, Any]:
        """Synchronously insert or assign a column value."""
        return await self.ainsert(
            column=column,
            value=value,
        )

    # ==========================================================
    # MAP
    # ==========================================================

    async def amap(
        self,
        func: str,
        na_action: Optional[str] = None,
        columns: Optional[List[str]] = None,
        datetime_action: str = "skip",
    ):
        """Asynchronously apply a mapping function to values."""
        return await super().map(
            func=func,
            na_action=na_action,
            columns=columns,
            datetime_action=datetime_action,
        )

    @async_to_sync
    async def map(
        self,
        func: str,
        na_action: Optional[str] = None,
        columns: Optional[List[str]] = None,
        datetime_action: str = "skip",
    ):
        """Synchronously apply a mapping function to values."""
        return await self.amap(
            func=func,
            na_action=na_action,
            columns=columns,
            datetime_action=datetime_action,
        )

    # ==========================================================
    # RENAME
    # ==========================================================

    async def arename(
        self,
        columns: Dict[str, str],
    ) -> Dict[str, Any]:
        """Asynchronously rename columns using a mapping."""
        return await super().rename(columns=columns)

    @async_to_sync
    async def rename(
        self,
        columns: Dict[str, str],
    ) -> Dict[str, Any]:
        """Synchronously rename columns using a mapping."""
        return await self.arename(columns=columns)

    # ==========================================================
    # SET INDEX
    # ==========================================================

    async def aset_index(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Asynchronously set one or more columns as index."""
        return await super().set_index(columns=columns)

    @async_to_sync
    async def set_index(
        self,
        columns: List[str],
    ) -> Dict[str, Any]:
        """Synchronously set one or more columns as index."""
        return await self.aset_index(columns=columns)

    # ==========================================================
    # RESET INDEX
    # ==========================================================

    async def areset_index(self) -> Dict[str, Any]:
        """Asynchronously reset the table index."""
        return await super().reset_index()

    @async_to_sync
    async def reset_index(self) -> Dict[str, Any]:
        """Synchronously reset the table index."""
        return await self.areset_index()

    # ==========================================================
    # UPDATE
    # ==========================================================

    async def aupdate(
        self,
        on: str,
        other_table: str,
        other_schema: str = "upload",
        overwrite: bool = True,
        errors: str = "ignore",
    ) -> Dict[str, Any]:
        """Asynchronously update rows from another table using a key."""
        return await super().update(
            on=on,
            other_table=other_table,
            other_schema=other_schema,
            overwrite=overwrite,
            errors=errors,
        )

    @async_to_sync
    async def update(
        self,
        on: str,
        other_table: str,
        other_schema: str = "upload",
        overwrite: bool = True,
        errors: str = "ignore",
    ) -> Dict[str, Any]:
        """Synchronously update rows from another table using a key."""
        return await self.aupdate(
            on=on,
            other_table=other_table,
            other_schema=other_schema,
            overwrite=overwrite,
            errors=errors,
        )

    # ==========================================================
    # RESAMPLE
    # ==========================================================

    async def aresample(
        self,
        time_column: str,
        rule: str,
        agg: str = "COUNT",
        value_column: Optional[str] = None,
        label: str = "left",
        closed: str = "left",
    ) -> Dict[str, Any]:
        """Asynchronously resample time-series data with aggregation."""
        return await super().resample(
            time_column=time_column,
            rule=rule,
            agg=agg,
            value_column=value_column,
            label=label,
            closed=closed,
        )

    @async_to_sync
    async def resample(
        self,
        time_column: str,
        rule: str,
        agg: str = "COUNT",
        value_column: Optional[str] = None,
        label: str = "left",
        closed: str = "left",
    ) -> Dict[str, Any]:
        """Synchronously resample time-series data with aggregation."""
        return await self.aresample(
            time_column=time_column,
            rule=rule,
            agg=agg,
            value_column=value_column,
            label=label,
            closed=closed,
        )

    # ==========================================================
    # SIMPLE PROPERTY OPS
    # ==========================================================

    async def aaxes(self):
        """Asynchronously return table axis labels."""
        return await super().axes()

    @async_to_sync
    async def axes(self):
        """Synchronously return table axis labels."""
        return await self.aaxes()

    async def acolumns(self):
        """Asynchronously return column labels."""
        return await super().columns()

    @async_to_sync
    async def columns(self):
        """Synchronously return column labels."""
        return await self.acolumns()

    async def adtypes(self):
        """Asynchronously return column dtypes."""
        return await super().dtypes()

    @async_to_sync
    async def dtypes(self):
        """Synchronously return column dtypes."""
        return await self.adtypes()

    async def afirst_valid_index(self):
        """Asynchronously return the first valid index label."""
        return await super().first_valid_index()

    @async_to_sync
    async def first_valid_index(self):
        """Synchronously return the first valid index label."""
        return await self.afirst_valid_index()

    async def amemory_usage(self):
        """Asynchronously return per-column memory usage."""
        return await super().memory_usage()

    @async_to_sync
    async def memory_usage(self):
        """Synchronously return per-column memory usage."""
        return await self.amemory_usage()

    async def andim(self):
        """Asynchronously return number of dimensions."""
        return await super().ndim()

    @async_to_sync
    async def ndim(self):
        """Synchronously return number of dimensions."""
        return await self.andim()

    async def ashape(self):
        """Asynchronously return table shape."""
        return await super().shape()

    @async_to_sync
    async def shape(self):
        """Synchronously return table shape."""
        return await self.ashape()

    async def asize(self):
        """Asynchronously return total number of elements."""
        return await super().size()

    @async_to_sync
    async def size(self):
        """Synchronously return total number of elements."""
        return await self.asize()

    async def avalues(self):
        """Asynchronously return table values."""
        return await super().values()

    @async_to_sync
    async def values(self):
        """Synchronously return table values."""
        return await self.avalues()

    async def aitems(self):
        """Asynchronously iterate over column name/value pairs."""
        return await super().items()

    @async_to_sync
    async def items(self):
        """Synchronously iterate over column name/value pairs."""
        return await self.aitems()

    async def aiterrows(self):
        """Asynchronously iterate over rows as index/series pairs."""
        return await super().iterrows()

    @async_to_sync
    async def iterrows(self):
        """Synchronously iterate over rows as index/series pairs."""
        return await self.aiterrows()

    async def aitertuples(
        self,
        index: bool = True,
    ):
        """Asynchronously iterate rows as named tuples."""
        return await super().itertuples(index=index)

    @async_to_sync
    async def itertuples(
        self,
        index: bool = True,
    ):
        """Synchronously iterate rows as named tuples."""
        return await self.aitertuples(index=index)
