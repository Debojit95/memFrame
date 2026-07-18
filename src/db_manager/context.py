import sys
import logging
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent   # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from src.core.ingestion.datatype_detector import Backend
from src.db_manager.adapters.base import DatabaseAdapter
from src.db_manager.adapters.postgresql import PostgresAdapter
from src.db_manager.adapters.duckdb import DuckDBAdapter
from src.db_manager.adapters.clickhouse import ClickHouseAdapter  
    

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
        
        
        # PLOTS
        self._bar_wrapper = None
        self._bar_polar_wrapper = None
        self._pie_wrapper = None
        self._line_wrapper = None
        
        
    
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
        
        for wrapper in (self.inspect, self.select, self.clean,self.stats,
                        self.bar,self.bar_polar, self.pie,self.line):
            
            if hasattr(wrapper, name):
                return getattr(wrapper, name)
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        return sorted(
            set(super().__dir__())
            | set(dir(self.select))
            | set(dir(self.inspect))
            | set(dir(self.clean))
            | set(dir(self.stats))
            | set(dir(self.bar))
            | set(dir(self.bar_polar))
            | set(dir(self.pie))
            | set(dir(self.line))
            
            
        )
    
    
    
    @property
    def select(self):
        from wrappers.analytix.selection import SelectionWrapper

        if self._selection_wrapper is None:
            self._selection_wrapper = SelectionWrapper(self)
        return self._selection_wrapper


     
    @property
    def inspect(self):
        from wrappers.analytix.inspect import TableOpsWrapper

        if self._inspect_wrapper is None:
            self._inspect_wrapper = TableOpsWrapper(self)
        return self._inspect_wrapper
    
    
    @property
    def clean(self):
        from wrappers.analytix.cleaning import CleaningWrapper

        if self._clean_wrapper is None:
            self._clean_wrapper = CleaningWrapper(self)
        return self._clean_wrapper
    
    
    @property
    def stats(self):
        from wrappers.analytix.stats import StatsWrapper
        if self._stats_wrapper is None:
            self._stats_wrapper = StatsWrapper(self)
        return self._stats_wrapper
    
    # PLOTTING-------
    
    @property
    def bar(self):
        from wrappers.plots.bar import BarWrapper
        if self._bar_wrapper is None:
            self._bar_wrapper = BarWrapper(self)
        return self._bar_wrapper
    
    
    @property
    def bar_polar(self):
        from wrappers.plots.bar_polar import BarPolarWrapper
        if self._bar_polar_wrapper is None:
            self._bar_polar_wrapper = BarPolarWrapper(self)
        return self._bar_polar_wrapper
    
    
    @property
    def pie(self):
        from wrappers.plots.pie import PieWrapper
        if self._pie_wrapper is None:
            self._pie_wrapper = PieWrapper(self)
        return self._pie_wrapper
    
    
    @property
    def line(self):
        from wrappers.plots.line import LineWrapper
        if self._line_wrapper is None:
            self._line_wrapper = LineWrapper(self)
        return self._line_wrapper
    
    
    
    
    async def _ensure_adapter(self):
        """Create the appropriate adapter from memframe's backend."""
        if self._adapter is not None:
            return

        backend = self.memframe._backend
        if backend is None:
            raise RuntimeError("Not connected. Call await connect() first.")

        if backend.backend == Backend.DUCKDB:
            self._adapter = DuckDBAdapter(
                backend.conn_params.get("db_path", ":memory:"),
                existing_conn=getattr(backend, "_conn", None),
            )
        elif backend.backend == Backend.POSTGRES:
            params = backend.conn_params
            self._adapter = PostgresAdapter(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                database=params["database"],
            )
        elif backend.backend == Backend.CLICKHOUSE:           
            params = backend.conn_params
            self._adapter = ClickHouseAdapter(
                host=params["host"],
                port=params.get("port", 8123),
                user=params["user"],
                password=params["password"],
                database=params.get("database"),
                timeout=params.get("timeout", 10.0),
            )
        else:
            raise RuntimeError("Unsupported backend")

        await self._adapter.connect()

    async def close(self):
        if self._adapter:
            await self._adapter.close()
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
