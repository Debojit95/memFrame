import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import pandas as pd

from memframe.db_manager.connection import ConnectorManager
from memframe.db_manager.context import ContextManager
from memframe.db_manager.ops import OpsMixin
from memframe.db_manager.setup import DatabaseBackend
from memframe.exceptions import ConnectionNotReady, ConfigurationError
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class MemFrame(ContextManager, OpsMixin):
    def __init__(self, connection_type: str = "local", connection_params: Optional[Dict[str, Any]] = None, deep_cache: Optional[bool] = None):
        super().__init__(self)
        self.deep_cache = deep_cache
        self._active_id: Optional[str] = None
        self._connector = ConnectorManager(
            connection_type,
            connection_params,
            context_factory=lambda data_id: ContextManager(self, data_id=data_id),
        )

    @property
    def _backend(self) -> Optional[DatabaseBackend]:
        return self._connector._backend

    @property
    def _pool(self):
        return self._connector.pool

    @property
    def _uploader(self):
        return self._connector._uploader

    # ── connect ─────────────────────────────────────────────────────

    async def aconnect(self) -> None:
        await self._connector.aconnect()

    @async_to_sync
    async def connect(self) -> None:
        return await self.aconnect()

    # ── AI agent ─────────────────────────────────────────────────────

    async def aenable_agent(
        self,
        api_key: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        **overrides,
    ):
        """Configure the optional AI agent layer.

        Idempotent — calling again replaces the settings on this
        ``MemFrame`` and every dataset context bound to it. ``provider``
        and ``model`` default to the values in
        :class:`memframe_ai.config.AISettings`. ``api_key`` is required
        and can be passed positionally or by keyword.
        """
        from memframe_ai.config import AISettings

        kwargs: dict[str, Any] = {}
        if api_key is not None:
            kwargs["api_key"] = api_key
        if provider is not None:
            kwargs["provider"] = provider
        if model is not None:
            kwargs["model"] = model
        kwargs.update(overrides)
        self._ai_settings = AISettings(**kwargs)
        return self._ai_settings

    @async_to_sync
    async def enable_agent(
        self,
        api_key: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        **overrides,
    ):
        """Synchronous form of :meth:`aenable_agent`."""
        return await self.aenable_agent(
            api_key=api_key, provider=provider, model=model, **overrides
        )

    # ── upload ──────────────────────────────────────────────────

    def _placeholder(self, index: int) -> str:
        return self._connector._placeholder(index)

    async def aupload_csv(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_csv(file_path, dtypes=dtypes)

    @async_to_sync
    async def upload_csv(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_csv(file_path, dtypes=dtypes)

    async def aupload_parquet(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_parquet(file_path, dtypes=dtypes)

    @async_to_sync
    async def upload_parquet(
        self,
        file_path: str | Path,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_parquet(file_path, dtypes=dtypes)

    async def aupload_df(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self._uploader._aupload_df(df, filename, dtypes=dtypes)

    @async_to_sync
    async def upload_df(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
    ) -> ContextManager:
        return await self.aupload_df(df, filename=filename, dtypes=dtypes)

    # ── ops / context helpers ────────────────────────────────────

    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ):
        if data_id is not None and data is not None:
            raise ConfigurationError("Pass either `data_id` or `data`, not both.")
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

    def _local_db_path(self) -> Optional[Path]:
        if not self._connector.is_duckdb():
            raise ConnectionNotReady("Local DuckDB connection is not active.")
        db_path = self._backend.conn_params.get("db_path", "memframe_new.duckdb")
        if db_path == ":memory:":
            return None
        return Path(db_path)

    async def aclose(self) -> None:
        await super().aclose()
        await self._connector.aclose()

    @async_to_sync
    async def close(self) -> None:
        return await self.aclose()

    def memFrame(self, data_id: Optional[str] = None, data: Any = None, columns: Optional[List[str]] = None):
        return self._ops(data_id, data, columns)

    async def __aenter__(self):
        await self.aconnect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()



def log_result(result:Dict):
    
    if not result["is_error"]:
        print(result["result"])   
    else:
        print(result["error_message"])


    
async def test():
    import numpy as np
    import pandas as pd
    import time
    
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
    
    # mf = MemFrame(connection_type="local", connection_params={"db_path": "/mnt/c/Users/ASUS/Documents/Open_Source/memFrame/memFrame_new.duckdb"})
    mf = MemFrame(connection_type="remote", connection_params=pg_params,deep_cache=True)
    # mf = MemFrame(connection_type="remote", connection_params=clickhouse_params, deep_cache=True)
    
    
    
    await mf.aconnect()
    print(mf)
    
    # df = pd.DataFrame({
    #         "id": [1, 2, 3, 4, 4],
    #         "salary": [1000, None, 3000, 1000000, 1000000],
    #         "bonus": [100, 200, None, 400, 400],
    #         "department": ["HR", "IT", "IT", "Finance", "Finance"],
    #         "category": ["A", "B", "B", "X", "X"],
    #         "numeric_str": ["10", "20", "30", "40", "50"],
    #         "date_col": [
    #             "2024-01-01",
    #             "2024-02-15",
    #             "2024-03-20",
    #             "2024-04-25",
    #             "2024-04-25",
    #         ],
    #         "mixed_nulls": [1, None, None, 4, 4],
    #     })
        
    # df.to_csv("/mnt/c/Users/ASUS/Documents/Open_Source/memFrame/tests/result/fails.csv")

    df1 = pd.DataFrame(
    {
        "A": [[[0, 1, 2], "foo", [], [3, 4]],[5,6],[["hor","mhor"],[3,"j8"],8]],
        "B": [1,5,7],
        "C": [[["a", "b", "c"], np.nan, [], ["d", "e"]],np.nan,[6,"uiu",'*',np.nan]]
    }
)
    
    try:
        start_time = time.perf_counter()
        ops1 = await mf.aupload_csv('/mnt/c/Users/ASUS/Documents/BAAS_KUBE/Neoanalytix/test_datasets/success/covid19.csv')
        # ops1 = await mf.aupload_csv('/mnt/c/Users/ASUS/Documents/Open_Source/memFrame/tests/result/fails.csv')
        # ops1 = await mf.aupload_parquet('tests/datasets/sample.parquet')
        # ops1 = await mf.aupload_df(df)
        # ops1 = mf._ops(data_id='Vip5bj')
        print(f"time taken : {time.perf_counter()-start_time} seconds")
        print(await mf.alist_tables())
        
        print("*"*100)
        

        
        # result1 = ops1.head()
        # result1 = ops1.sub('C','B')
        # print(result1)
        print("*"*100)
        # _ = ops1.clip(column="date", lower="2021-01-05", upper="2023-10-15")
        _ = ops1.corr()
        print(_)


        # result1 = ops1.fillna('C')
        # log_result(result1)
        # print("*"*100)

        # n = 1000000
        # df = pd.DataFrame({
        #     "A": np.random.uniform(1, 99, size=n),
        #     "B":  np.random.uniform(1, 99, size=n),
        # })
        # print(df.sample())
        # ctx = mf.upload_df(df)
        # data_id = ctx._data_id
        # print(f"Uploaded {n} rows → data_id: {data_id}")

        # # ---- 1st call (cold cache) ----
        # t0 = time.perf_counter()
        # res_cold = await ctx.amul('A','B')
        # t1 = time.perf_counter()
        # cold_time = t1 - t0
        # print(f"Cold call took {cold_time:.4f} seconds")

        # # ---- 2nd call (identical) ----
        # t0 = time.perf_counter()
        # res_cached = await ctx.amul('A','B')
        # t1 = time.perf_counter()
        # cached_time = t1 - t0
        # print(f"Cached call took {cached_time:.4f} seconds")
        
        # speedup = cold_time / cached_time
        # print(f"Speedup: {speedup:.2f}x faster with cache")
        
        
        
        
        # result1 =  ops1.groupby("D").mean("B")
        # log_result(result1)
        # print("*"*100)
        
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
        await mf.aclose()
    
    return



if __name__ == "__main__":
    import asyncio
    _ = asyncio.run(test())



