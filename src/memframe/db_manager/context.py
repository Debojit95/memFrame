import logging
from typing import Any, Optional

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter


logger = logging.getLogger("memFrame")


class ContextManager:
    def __init__(self, memframe_instance, data_id: Optional[str] = None):
        self.memframe = memframe_instance
        self._data_id = data_id
        self._adapter: Optional[DatabaseAdapter] = None
        self._wrappers = None

    def _lazy_init_wrappers(self):
        if self._wrappers is not None:
            return
        from memframe.wrappers.analytix.selection import SelectionWrapper
        from memframe.wrappers.analytix.inspection import TableOpsWrapper
        from memframe.wrappers.analytix.cleaning import CleaningWrapper
        from memframe.wrappers.analytix.stats import StatsWrapper
        from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
        from memframe.wrappers.plots.bar import BarWrapper
        from memframe.wrappers.plots.bar_polar import BarPolarWrapper
        from memframe.wrappers.plots.pie import PieWrapper
        from memframe.wrappers.plots.line import LineWrapper
        from memframe.wrappers.plots.scatter import ScatterWrapper
        from memframe.wrappers.plots.scatter_3d import Scatter3DWrapper

        self._wrappers = [
            SelectionWrapper(self),
            TableOpsWrapper(self),
            CleaningWrapper(self),
            StatsWrapper(self),
            ArithmeticWrapper(self),
            BarWrapper(self),
            BarPolarWrapper(self),
            PieWrapper(self),
            LineWrapper(self),
            ScatterWrapper(self),
            Scatter3DWrapper(self),
        ]

    def __getattr__(self, name: str) -> Any:
        self._lazy_init_wrappers()
        for w in self._wrappers:
            if hasattr(w, name):
                return getattr(w, name)
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        self._lazy_init_wrappers()
        names = set(super().__dir__())
        for w in self._wrappers:
            names.update(w.__dir__())
        return sorted(names)

    async def _ensure_adapter(self):
        if self._adapter is not None:
            return
        backend = self.memframe._backend
        pool = getattr(self.memframe, "_pool", None)
        if backend is None or pool is None:
            raise RuntimeError("Not connected. Call await connect() first.")
        if backend.backend == Backend.DUCKDB:
            self._adapter = DuckDBAdapter(pool)
        elif backend.backend == Backend.POSTGRES:
            self._adapter = PostgresAdapter(pool)
        elif backend.backend == Backend.CLICKHOUSE:
            self._adapter = ClickHouseAdapter(pool)
        else:
            raise RuntimeError("Unsupported backend")

    async def close(self):
        self._adapter = None

    async def _get_active_context(self):
        data_id = self._data_id or self.memframe._active_id
        if not data_id:
            raise ValueError("No active dataset and no explicit data_id provided.")
        backend = self.memframe._backend
        rows = await backend.fetch(
            f"SELECT table_name FROM {backend.csv_registry_table} WHERE data_id = {backend.placeholder(1)}",
            data_id,
        )
        if not rows:
            raise ValueError(f"No registry entry for {data_id}")
        table_name = rows[0][0]
        schema = backend.upload_schema
        return table_name, schema
