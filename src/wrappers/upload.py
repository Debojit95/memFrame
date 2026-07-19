
import logging
import sys
from pathlib import Path
from typing import  Optional,Union,TYPE_CHECKING


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from db_manager.context import ContextManager
from utils.async_sync import async_to_sync
from core.ingestion.upload_manager import Uploader

if TYPE_CHECKING:
    import pandas as pd


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

class UploadWrapper(Uploader):
    """
    Public upload interface for memFrame.

    This wrapper exposes synchronous and asynchronous APIs
    for uploading datasets into the memFrame execution engine.

    Supported upload formats:
        - CSV
        - Parquet
        - pandas DataFrame

    Notes
    -----
    All upload methods return a `ContextManager` instance
    which acts as the dataframe-like query context used
    throughout memFrame operations.

    The synchronous APIs internally execute the asynchronous
    implementations using the `async_to_sync` adapter.
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize UploadWrapper.

        Parameters
        ----------
        *args :
            Positional arguments forwarded to `Uploader`.

        **kwargs :
            Keyword arguments forwarded to `Uploader`.
        """
        super().__init__(*args, **kwargs)
    
    # ====================== SYNC APIS ===================
    async def aupload_csv(self, file_path: Union[str, Path]) -> ContextManager:
        """
        Asynchronously upload a CSV file.

        Parameters
        ----------
        file_path : str or Path
            Path to the CSV file.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.

        Raises
        ------
        FileNotFoundError
            If the CSV file does not exist.

        ValueError
            If the CSV file is invalid or unreadable.
        """

        return await super()._aupload_csv(file_path)
    
    @async_to_sync
    async def upload_csv(self, file_path: Union[str, Path]) -> ContextManager:
        """
        Synchronously upload a CSV file.

        This method internally calls `aupload_csv`
        using sync-to-async execution bridging.

        Parameters
        ----------
        file_path : str or Path
            Path to the CSV file.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.
        """
        return await self.aupload_csv(file_path)

    
    async def aupload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
        """
        Asynchronously upload a Parquet file.

        Parameters
        ----------
        file_path : str or Path
            Path to the Parquet file.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.
        """
        return await super()._aupload_parquet(file_path)

    @async_to_sync
    async def upload_parquet(self, file_path: Union[str, Path]) -> ContextManager:
        """
        Synchronously upload a Parquet file.

        Parameters
        ----------
        file_path : str or Path
            Path to the Parquet file.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.
        """
        return await self.aupload_parquet(file_path)
    
    async def aupload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        """
        Asynchronously upload a pandas DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            Input dataframe to upload.

        filename : str, optional
            Optional virtual filename used internally
            for table naming and metadata tracking.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.
        """
        return await super()._aupload_df(df, filename)
        
    @async_to_sync
    async def upload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        """
        Synchronously upload a pandas DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            Input dataframe to upload.

        filename : str, optional
            Optional virtual filename used internally
            for table naming and metadata tracking.

        Returns
        -------
        ContextManager
            Query execution context for the uploaded dataset.
        """
        return await self.aupload_df(df, filename=filename)




