import asyncio

import pandas as pd
import pytest

from memframe.exceptions import (
    ConfigurationError,
    ConnectionNotReady,
    DataNotFound,
    OperationError,
)
from memframe.main import MemFrame
from memframe.utils.helper import SQLIdentifierSanitizer


@pytest.fixture
def mf():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    try:
        yield m
    finally:
        asyncio.run(m.aclose())


@pytest.fixture
def uploaded(mf):
    ctx = mf.upload_df(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}), filename="err_dataset")
    return ctx


class TestUploadErrorPaths:
    def test_missing_csv_raises_file_not_found(self, mf):
        with pytest.raises(FileNotFoundError):
            mf.upload_csv("/no/such/file.csv")

    def test_dtypes_unknown_column_raises(self, mf):
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(ConfigurationError, match="unknown column"):
            mf.upload_df(df, dtypes={"bogus": "int64"})

    def test_dtypes_empty_value_raises(self, mf):
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(ConfigurationError, match="non-empty string"):
            mf.upload_df(df, dtypes={"a": "  "})

    def test_dtypes_unknown_type_raises(self, mf):
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(ConfigurationError, match="Unknown dtype"):
            mf.upload_df(df, dtypes={"a": "BOGUS"})

    def test_normalize_dtype_override_rejects_bogus(self):
        from memframe.core.ingestion.upload.base import Uploader

        with pytest.raises(ConfigurationError, match="Unknown dtype"):
            Uploader()._normalize_dtype_override({"a": "BOGUS"})


class TestSelectionErrorResponses:
    def test_iloc_tuple_and_columns_conflict(self, uploaded):
        with pytest.raises(OperationError, match="not both"):
            uploaded.iloc(("1:2", "a"), columns=["a"])

    def test_iloc_tuple_bad_length(self, uploaded):
        with pytest.raises(OperationError, match="exactly two items"):
            uploaded.iloc(("1:2", "a", "extra"))

    def test_iloc_columns_not_str_list(self, uploaded):
        with pytest.raises(OperationError, match="list of column names"):
            uploaded.iloc("1:2", columns=42)

    def test_iloc_columns_empty_list(self, uploaded):
        with pytest.raises(OperationError, match="cannot be empty"):
            uploaded.iloc("1:2", columns=[])

    def test_iloc_columns_non_str_elements(self, uploaded):
        with pytest.raises(OperationError, match="list of column names"):
            uploaded.iloc("1:2", columns=[1, 2])

    def test_iloc_unknown_column_raises(self, uploaded):
        with pytest.raises(OperationError, match="Unknown column"):
            uploaded.iloc("1:2", columns=["nope"])

    def test_iloc_col_indexer_and_columns_conflict(self, uploaded):
        with pytest.raises(OperationError, match="not both"):
            uploaded.iloc(1, col_indexer=[0], columns=["a"])

    def test_select_dtypes_no_match(self, uploaded):
        with pytest.raises(OperationError, match="No columns match"):
            uploaded.select_dtypes(include=["float64"])

    def test_map_invalid_datetime_action(self, uploaded):
        with pytest.raises(OperationError, match="Invalid datetime_action"):
            uploaded.map("uppercase", datetime_action="bogus")


class TestOpsConfigErrorPaths:
    def test_ops_both_args_raises(self, mf):
        with pytest.raises(ConfigurationError, match="not both"):
            mf._ops(data_id="x", data=pd.DataFrame({"a": [1]}))

    def test_local_db_path_non_duckdb_raises(self):
        from unittest.mock import MagicMock

        m = MemFrame.__new__(MemFrame)
        m._connector = MagicMock()
        m._connector.is_duckdb.return_value = False
        with pytest.raises(ConnectionNotReady, match="Local DuckDB"):
            m._local_db_path()

    def test_retrieve_missing_op_raises(self, mf, uploaded):
        with pytest.raises(DataNotFound, match="Operation 99"):
            asyncio.run(mf.aretrieve_operation(uploaded._data_id, 99))

    def test_upload_df_before_connect_raises(self):
        m = MemFrame()
        with pytest.raises(ConnectionNotReady, match="Not connected"):
            m.upload_df(pd.DataFrame({"a": [1]}))


class TestSanitizerNegatives:
    def test_is_valid_rejects_quotes(self):
        assert SQLIdentifierSanitizer.is_valid('inject"me') is False

    def test_is_valid_rejects_semicolon(self):
        assert SQLIdentifierSanitizer.is_valid("a;DROP") is False

    def test_is_valid_rejects_whitespace(self):
        assert SQLIdentifierSanitizer.is_valid("has space") is False

    def test_is_valid_rejects_empty(self):
        assert SQLIdentifierSanitizer.is_valid("") is False

    def test_is_valid_accepts_plain(self):
        assert SQLIdentifierSanitizer.is_valid("valid_col_1") is True
