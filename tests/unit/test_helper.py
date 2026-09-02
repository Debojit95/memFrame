import pytest

from memframe.exceptions import ConfigurationError
from memframe.utils.helper import (
    SQLIdentifierSanitizer,
    sanitize_sql_identifier,
    validate_sql_identifier,
)


class TestSQLIdentifierSanitizer:
    def test_validate_simple(self):
        assert SQLIdentifierSanitizer.validate("col1") == "col1"

    def test_validate_qualified(self):
        assert SQLIdentifierSanitizer.validate("schema.table") == "schema.table"

    def test_validate_disallows_dots_when_not_qualified(self):
        with pytest.raises(ConfigurationError):
            SQLIdentifierSanitizer.validate("schema.table", allow_qualified=False)

    def test_validate_rejects_empty(self):
        with pytest.raises(ConfigurationError):
            SQLIdentifierSanitizer.validate("")

    def test_validate_rejects_non_string(self):
        with pytest.raises(ConfigurationError):
            SQLIdentifierSanitizer.validate(123)

    def test_validate_rejects_dangerous_chars(self):
        for bad in ("col; DROP TABLE", "col-name", "col name", "col()"):
            with pytest.raises(ConfigurationError):
                SQLIdentifierSanitizer.validate(bad)

    def test_sanitize_dangerous_chars(self):
        assert SQLIdentifierSanitizer.sanitize("col name") == "col_name"
        assert SQLIdentifierSanitizer.sanitize("col-name") == "col_name"
        assert SQLIdentifierSanitizer.sanitize("col;DROP") == "col_DROP"

    def test_sanitize_qualified_preserves_dots(self):
        assert SQLIdentifierSanitizer.sanitize("sch.col") == "sch.col"

    def test_sanitize_empty_returns_underscore(self):
        assert SQLIdentifierSanitizer.sanitize("") == "_"

    def test_sanitize_strips_quotes(self):
        assert SQLIdentifierSanitizer.sanitize('"col"') == "col"

    def test_is_valid(self):
        assert SQLIdentifierSanitizer.is_valid("ok_col")
        assert not SQLIdentifierSanitizer.is_valid("not ok")

    def test_sanitize_many(self):
        assert SQLIdentifierSanitizer.sanitize_many(["a b", "c-d"]) == ["a_b", "c_d"]


def test_convenience_functions():
    assert sanitize_sql_identifier("a b") == "a_b"
    assert validate_sql_identifier("ok_col") == "ok_col"
