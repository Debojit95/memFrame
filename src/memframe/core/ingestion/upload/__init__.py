"""Upload package for memFrame."""

from .base import Uploader
from .duckdb import DuckDBUploader
from .postgres import PostgresUploader
from .clickhouse import ClickHouseUploader
from .strategies.base import UploadStrategy
from .strategies.csv import CsvUploadStrategy
from .strategies.parquet import ParquetUploadStrategy
from .strategies.df import DfUploadStrategy

__all__ = [
    "Uploader",
    "DuckDBUploader",
    "PostgresUploader",
    "ClickHouseUploader",
    "UploadStrategy",
    "CsvUploadStrategy",
    "ParquetUploadStrategy",
    "DfUploadStrategy",
]