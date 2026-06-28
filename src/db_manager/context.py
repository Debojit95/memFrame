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
        instead of:
            ops.inspect.head() / await ops.inspect.ahead()
            ops.clean.fillna(...) / await ops.clean.adropna(...)
            ops.arithmetic.pow(...) / ops.arithmetic.clip(...)
            ops.compare.compare("A >= B")
            ops.cumulative.cumsum(...) / await ops.cumulative.acumsum(...)
            ops.filters.filter("A > B")
            ops.preprocess.cyclical(column="date", features=["month"])
            ops.reshape.rank(columns=["B", "C"])
            ops.select.select_dtypes(exclude="categorical")
            ops.sort.sort_values(by="B")
            ops.stats.holiday_counts("A")
            ops.datetime.year(...) / await ops.datetime.ayear(...)
        """
        

        # Priority matters for overlapping method names (e.g. `clip`):
        # direct `ops.clip(...)` resolves to arithmetic clip.
        for wrapper in (self.select,):
            if hasattr(wrapper, name):
                return getattr(wrapper, name)
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        return sorted(
            set(super().__dir__())
            | set(dir(self.select))
        )
    
    
    
    @property
    def select(self):
        from wrappers.analytix.selection import SelectionWrapper

        if self._selection_wrapper is None:
            self._selection_wrapper = SelectionWrapper(self)
        return self._selection_wrapper


    
    
    
    
    
    
    
    
    
    
    
    
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
        schema = "upload"         # hardcoded schema for both backends

        return table_name, schema
