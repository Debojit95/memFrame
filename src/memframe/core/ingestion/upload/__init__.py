"""Upload package for memFrame."""

from .base import Uploader
from .duckdb_impl import DuckDBUploadImpl
from .postgres_impl import PostgresUploadImpl
from .clickhouse_impl import ClickHouseUploadImpl
from .strategies.base import UploadStrategy
from .strategies.csv import CsvUploadStrategy
from .strategies.parquet import ParquetUploadStrategy
from .strategies.df import DfUploadStrategy

__all__ = [
    "Uploader",
    "DuckDBUploadImpl",
    "PostgresUploadImpl",
    "ClickHouseUploadImpl",
    "UploadStrategy",
    "CsvUploadStrategy",
    "ParquetUploadStrategy",
    "DfUploadStrategy",
]
