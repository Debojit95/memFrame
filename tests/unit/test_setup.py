import pytest

from memframe.db_manager.setup import create_backend
from memframe.core.ingestion.datatype_detector import Backend


@pytest.fixture
def duckdb_backend():
    backend = create_backend(Backend.DUCKDB, {"db_path": ":memory:"})
    return backend


class TestSchemaNaming:
    def test_default_schemas(self, duckdb_backend):
        assert duckdb_backend.upload_schema == "upload"
        assert duckdb_backend.transient_schema == "transient"
        assert duckdb_backend.registry_schema == "registry"


class TestTableNaming:
    def test_get_upload_table_name(self, duckdb_backend):
        assert duckdb_backend.get_upload_table_name("abc123") == "abc123"

    def test_get_transient_table_name(self, duckdb_backend):
        assert duckdb_backend.get_transient_table_name("abc123", 5) == 'transient."abc123_5"'

    def test_registry_table_properties(self, duckdb_backend):
        assert duckdb_backend.transient_registry_table == "registry.transient_registry"
        assert duckdb_backend.csv_registry_table == "registry.csv_registry"

    def test_placeholder(self, duckdb_backend):
        assert duckdb_backend.placeholder(1) == "?"


class TestSplitQualifiedName:
    def test_split(self, duckdb_backend):
        assert duckdb_backend._split_qualified_table_name('schema."tbl"') == ("schema", "tbl")

    def test_split_no_schema(self, duckdb_backend):
        assert duckdb_backend._split_qualified_table_name("tbl") == (None, "tbl")

    def test_strip_quotes(self, duckdb_backend):
        assert duckdb_backend._strip_identifier_quotes('"a`') == "a"


class TestPlaceholder:
    def test_postgres_placeholder(self):
        backend = create_backend(Backend.POSTGRES, {
            "host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"
        })
        assert backend.placeholder(2) == "$2"
