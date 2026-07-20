import asyncio
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

SOURCE_ROOT = Path(__file__).resolve().parent
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

if TYPE_CHECKING:
    import pandas as pd

from core.ingestion.datatype_detector import Backend
from db_manager.setup import DatabaseBackend
from db_manager.context import ContextManager
from wrappers.base import BaseWrapper

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)



class MemFrame(BaseWrapper):
    _CTX_PUBLIC_APIS = {name for name in ContextManager.__dict__ if not name.startswith("_")}

    def __init__(self, connection_type: str = "local", connection_params: Optional[Dict[str, Any]] = None,):
        super().__init__()
        self.connection_type = connection_type
        self.conn_params = connection_params or {}
        self._backend: Optional[DatabaseBackend] = None
        self._active_id: Optional[str] = None
        self._context_manager = ContextManager(self)


    def __getattr__(self, name: str):
        # Delegate ContextManager APIs on the default/global context.
        if name in self._CTX_PUBLIC_APIS:
            return getattr(self._context_manager, name)

        # Fallback for uploader internals that depend on backend helper APIs.
        if self._backend and hasattr(self._backend, name):
            return getattr(self._backend, name)

        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        return sorted(
            set(super().__dir__())
            | self._CTX_PUBLIC_APIS
        )


    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ):
        """
        Return an orchestrated operations interface for DataFrame operations.

        Use either:
        - `data_id` (str): operate on an existing uploaded dataset
        - `data` (dict/list/pandas-compatible): converted with
          `pd.DataFrame(data, columns=columns)`, uploaded, then used

        Returns:
            ContextManager bound to the resolved data_id (or active dataset).
        """
        if data_id is not None and data is not None:
            raise ValueError("Pass either `data_id` or `data`, not both.")

        # Backward compatibility: positional non-string input is treated as data.
        if data is None and data_id is not None and not isinstance(data_id, str):
            data = data_id
            data_id = None

        if data is not None:
            try:
                import pandas as pd
            except ImportError as exc:
                raise ImportError(
                    "ops(data=...) requires pandas for DataFrame conversion."
                ) from exc

            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)

            uploaded = self.upload_df(df)
            if isinstance(uploaded, ContextManager):
                return uploaded
            data_id = uploaded

        return ContextManager(self, data_id=data_id)
    
    def _placeholder(self, index: int) -> str:
        if not self._backend:
            raise RuntimeError("Not connected. Call await connect() first.")
        return self._backend.placeholder(index)

    def _local_db_path(self) -> Optional[Path]:
        
        if not self._backend or self._backend.backend != Backend.DUCKDB:
            raise RuntimeError("Local DuckDB connection is not active.")

        db_path = self._backend.conn_params.get("db_path", "memframe_new.duckdb")

        if db_path == ":memory:":
            return None

        return Path(db_path)
    
    
    async def close(self) -> None:
        if self._backend:
            await self._backend.close()

    def memFrame(self, data_id: Optional[str] = None, data: Any = None, columns: Optional[List[str]] = None,  ):
        return self._ops(data_id,data,columns)
    
    
    async def __aenter__(self):
        await self.aconnect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()








def log_result(result:Dict):
    
    if not result["is_error"]:
        print(result["result"])   
    else:
        print(result["error_message"])


def _postgres_demo_params(database: str) -> Dict[str, Any]:
    """Return PostgreSQL params for either Docker Compose or a host terminal."""
    return {
        "backend": "postgres",
        "host": os.getenv("PGHOST") or os.getenv("DB_HOST") or "127.0.0.1",
        "port": int(os.getenv("PGPORT") or os.getenv("DB_PORT") or "5723"),
        "user": os.getenv("PGUSER") or os.getenv("DB_USER") or "postgres",
        "password": (
            os.getenv("PGPASSWORD")
            or os.getenv("DB_PASSWORD")
            or "1daa7b94de72ed5e958797469df6bbeb3f14e0f6daa862b8442bc63a4da3b7c3"
        ),
        "database": os.getenv("PGDATABASE") or database,
    }
    
async def test():
    import numpy as np
    import pandas as pd
    # from src.utils.plot_renderer import smart_show
   
    
    pd.set_option('display.max_columns', 100) 

    
    pg_params={
        "backend": "postgres",
        "host": "localhost",
        "port": 5723,
        "user":"postgres",
        "password":  "1daa7b94de72ed5e958797469df6bbeb3f14e0f6daa862b8442bc63a4da3b7c3",
        "database": "testA"}
    
    clickhouse_params={
            "backend": "clickhouse",
            "host": "localhost",
            "port": 8123,
            "user":"default",
            "password": "your_clickhouse_password"
        }
    
        
    
    np.random.seed(42)

    df = pd.DataFrame(
        [
            [pd.to_datetime("2023-09-19"), 2, 2, "zoom", 0, "er","holiday"],
            [pd.to_datetime("2024-09-01"), 10, 3, "zoom", 4.54532, "zoom","Halfday"],
            [pd.to_datetime("2023-04-01"), 7, np.nan, "zoom", 2.567, "rt","work"],
            [pd.to_datetime("2023-05-12"), 8, np.nan, "meet", np.nan, "meet","work"],
            [pd.to_datetime("2022-12-25"), 17, 9, "zoom", 6.1, "er","work"],
            [pd.to_datetime("2024-03-31"), 32, 5, np.nan, np.nan, "er","Halfday"],
            [pd.to_datetime("2023-07-07"), 4, np.nan, "meet", 3, "rt","Halfday"],
            [pd.to_datetime("2022-11-11"), 1, 1, "zoom", 1.12, "rt","holiday"],
            [pd.to_datetime("2023-03-03"), 7, 7, np.nan, 12.675, "er","workday"],
            [pd.to_datetime("2024-01-01"), 1, 4, "meet", 5.345, "meet","Halfday"],
        ],
        columns=list("ABCDEFG")
    )

    # random time for column A
    hours = np.random.randint(0, 24, len(df))
    minutes = np.random.randint(0, 60, len(df))
    seconds = np.random.randint(0, 60, len(df))

    df["A"] = df["A"] + pd.to_timedelta(hours, unit="h") \
                        + pd.to_timedelta(minutes, unit="m") \
                        + pd.to_timedelta(seconds, unit="s")

    # ✅ Create 8th column (H) as datetime with timezone
    # Step 1: create another datetime (could be random offset from A)
    df["H"] = df["A"] + pd.to_timedelta(np.random.randint(1, 100, len(df)), unit="h")
    # df["H"] = df["H"].dt.tz_localize("Asia/Kolkata")


    df["I"] = [1672531200, 1675209600,1677628800, 1680307200,1682899200, 1685577600,1688169600,1690848000,1693526400, 1699866400]
    df['J'] = pd.to_datetime([
            "2023-01-01",
            "2024-07-31",
            "2021-11-01",
            "2024-02-01",
            "2022-03-01",
            "2023-04-01",
            "2024-05-31",
            "2022-12-01",
            "2025-03-11",
            "2023-04-01",
        ])
    
    # mf = MemFrame(connection_type="local", connection_params={"db_path": "memFrame_new.duckdb"})
    mf = MemFrame(connection_type="remote", connection_params=pg_params)
    # mf = MemFrame(connection_type="remote", connection_params=clickhouse_params)
    
    
    
    mf.connect()
    print(mf)

    df1 = pd.DataFrame(
    {
        "A": [[[0, 1, 2], "foo", [], [3, 4]],[5,6],[["hor","mhor"],[3,"j8"],8]],
        "B": [1,5,7],
        "C": [[["a", "b", "c"], np.nan, [], ["d", "e"]],np.nan,[6,"uiu",'*',np.nan]]
    }
)
    
    try:
        # ops1 = await mf.aupload_csv('demo/earthquake.csv')
        ops1 = await mf.aupload_df(df)
        # ops1 = mf._ops(data_id='Vip5bj')
            
        print(await mf.alist_tables())
        print("*"*100)
        

        
        # result1 = ops1.groupby("F","D").agg({"B": ["sum", "mean", "std"],"C":["var","min"]})
        # log_result(result1)
        # print("*"*100)
        
        
        result1 =  ops1.fillna("C")
        log_result(result1)
        print("*"*100)
        
        # result1 = ops1.groupby("G").median("B")
        # log_result(result1)
        # print("*"*100)
        
        # result1 =  ops1.groupby("D").sum("C")
        # log_result(result1)
        # print("*"*100)
        
        
        # result1 = ops1.groupby("F").std("B")
        # log_result(result1)
        # print("*"*100)
        
        
        # result1 = ops1.groupby("G").var("E")
        # log_result(result1)
        # print("*"*100)
        

    finally:
        await mf.close()
    
    return


if __name__ == "__main__":
    _ = asyncio.run(test())






