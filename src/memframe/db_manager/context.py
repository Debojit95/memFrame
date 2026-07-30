import logging
from pathlib import Path
from typing import Any, Optional

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter


logger = logging.getLogger("memFrame")


class ContextManager:
    """
    Orchestrates DataFrame operations using the active memframe connection.
    """

    
    def __init__(self, memframe_instance, data_id: Optional[str] = None):   
        self.memframe = memframe_instance
        self._data_id = data_id                                          
        self._adapter: Optional[DatabaseAdapter] = None
        
        self._selection_wrapper = None
        self._inspect_wrapper = None
        self._clean_wrapper = None
        self._stats_wrapper = None
        self._arithmetic_wrapper=None
        
        
        # PLOTS
        self._bar_wrapper = None
        self._bar_polar_wrapper = None
        self._pie_wrapper = None
        self._line_wrapper = None
        self._scatter_wrapper = None
        self._scatter3d_wrapper = None
        
        
    
    def __getattr__(self, name: str) -> Any:
        """
        Delegate wrapper APIs directly on ContextManager.

        Example:
            ops.head() / await ops.ahead()
            ops.fillna(...) / await ops.adropna(...)
            ops.pow(...) / ops.clip(...)
            ops.compare("A >= B")
            ops.cumsum(...) / await ops.acumsum(...)
            ops.filter("A > B")
            ops.cyclical(column="date", features=["month"])
            ops.rank(columns=["B", "C"])
            ops.select_dtypes(exclude="categorical")
            ops.sort_values(by="B")
            ops.mean("A")
        """
        
        for wrapper in (self.inspection, self.select, self.clean,self.stats,self.arithmetic,
                        self.bar,self.bar_polar, self.pie,self.line,self.scatter,self.scatter3d):
            
            if hasattr(wrapper, name):
                return getattr(wrapper, name)
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        return sorted(
            set(super().__dir__())
            | set(dir(self.select))
            | set(dir(self.inspection))
            | set(dir(self.clean))
            | set(dir(self.stats))
            | set(dir(self.arithmetic))
            
            | set(dir(self.bar))
            | set(dir(self.bar_polar))
            | set(dir(self.pie))
            | set(dir(self.line))
            | set(dir(self.scatter))
            | set(dir(self.scatter3d))
            
            
        )
    
    
    
    @property
    def select(self):
        from memframe.wrappers.analytix.selection import SelectionWrapper

        if self._selection_wrapper is None:
            self._selection_wrapper = SelectionWrapper(self)
        return self._selection_wrapper


     
    @property
    def inspection(self):
        from memframe.wrappers.analytix.inspection import TableOpsWrapper

        if self._inspect_wrapper is None:
            self._inspect_wrapper = TableOpsWrapper(self)
        return self._inspect_wrapper
    
    
    @property
    def clean(self):
        from memframe.wrappers.analytix.cleaning import CleaningWrapper

        if self._clean_wrapper is None:
            self._clean_wrapper = CleaningWrapper(self)
        return self._clean_wrapper
    
    
    @property
    def stats(self):
        from memframe.wrappers.analytix.stats import StatsWrapper
        if self._stats_wrapper is None:
            self._stats_wrapper = StatsWrapper(self)
        return self._stats_wrapper
    
    @property
    def arithmetic(self):
        from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
        if self._arithmetic_wrapper is None:
            self._arithmetic_wrapper = ArithmeticWrapper(self)
        return self._arithmetic_wrapper
    
    
    
    
    # PLOTTING-------
    
    @property
    def bar(self):
        from memframe.wrappers.plots.bar import BarWrapper
        if self._bar_wrapper is None:
            self._bar_wrapper = BarWrapper(self)
        return self._bar_wrapper
    
    
    @property
    def bar_polar(self):
        from memframe.wrappers.plots.bar_polar import BarPolarWrapper
        if self._bar_polar_wrapper is None:
            self._bar_polar_wrapper = BarPolarWrapper(self)
        return self._bar_polar_wrapper
    
    
    @property
    def pie(self):
        from memframe.wrappers.plots.pie import PieWrapper
        if self._pie_wrapper is None:
            self._pie_wrapper = PieWrapper(self)
        return self._pie_wrapper
    
    
    @property
    def line(self):
        from memframe.wrappers.plots.line import LineWrapper
        if self._line_wrapper is None:
            self._line_wrapper = LineWrapper(self)
        return self._line_wrapper
    
    
    @property
    def scatter(self):
        from memframe.wrappers.plots.scatter import ScatterWrapper
        if self._scatter_wrapper is None:
            self._scatter_wrapper = ScatterWrapper(self)
        return self._scatter_wrapper
    
    
    
    @property
    def scatter3d(self):
        from memframe.wrappers.plots.scatter_3d import Scatter3DWrapper
        if self._scatter3d_wrapper is None:
            self._scatter3d_wrapper = Scatter3DWrapper(self)
        return self._scatter3d_wrapper
    
    async def _ensure_adapter(self):
        """Create the appropriate adapter from memframe's backend and pool."""
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
        # Use explicit data_id if provided, otherwise fall back to global active
        data_id = self._data_id or self.memframe._active_id
        if not data_id:
            raise ValueError("No active dataset and no explicit data_id provided.")

        backend = self.memframe._backend
        rows = await backend.fetch(
            f"""
            SELECT table_name
            FROM {backend.csv_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        if not rows:
            raise ValueError(f"No registry entry for {data_id}")

        table_name = rows[0][0]   
        schema = backend.upload_schema

        return table_name, schema
