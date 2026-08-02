import pyarrow as pa
import pytest

from memframe.core.ingestion.datatype_detector import (
    DatatypeDetector,
    _generate_6char_id,
)


@pytest.fixture
def detector():
    return DatatypeDetector()


def _infer(detector, values):
    arr = pa.array(values)
    return detector._infer_column(pa.chunked_array([arr]))


class TestIDGeneration:
    def test_length(self):
        assert len(_generate_6char_id()) == 6

    def test_charset(self):
        import string
        allowed = set(string.ascii_letters + string.digits)
        assert all(c in allowed for c in _generate_6char_id())


class TestEncodingDetection:
    def test_ascii_file(self, tmp_path):
        p = tmp_path / "a.csv"
        p.write_text("hello,world\n1,2\n", encoding="ascii")
        assert DatatypeDetector()._detect_encoding(str(p)) in ("ascii", "utf-8")

    def test_to_postgres_encoding(self):
        assert DatatypeDetector.to_postgres_encoding("utf-8") == "UTF8"
        assert DatatypeDetector.to_postgres_encoding("latin-1") == "LATIN1"
        assert DatatypeDetector.to_postgres_encoding("unknown") == "UTF8"


class TestNormalize:
    def test_strips_and_removes_empty(self):
        d = DatatypeDetector()
        assert d._normalize_string_list(["  a  ", "", None, "b"]) == ["a", "b"]


class TestDetectBoolean:
    def test_all_boolean(self):
        assert DatatypeDetector()._detect_boolean(["true", "false", "1", "0"]) == 1.0

    def test_mixed_returns_zero(self):
        assert DatatypeDetector()._detect_boolean(["true", "maybe"]) == 0.0

    def test_empty_returns_zero(self):
        assert DatatypeDetector()._detect_boolean([]) == 0.0


class TestDetectInteger:
    def test_small_int(self):
        score, sql = DatatypeDetector()._detect_integer(["1", "2", "3"])
        assert score == 1.0
        assert sql == "SMALLINT"

    def test_integer_range(self):
        score, sql = DatatypeDetector()._detect_integer(["100000", "-50000"])
        assert score == 1.0
        assert sql == "INTEGER"

    def test_bigint_range(self):
        score, sql = DatatypeDetector()._detect_integer(["5000000000"])
        assert score == 1.0
        assert sql == "BIGINT"

    def test_non_integer_returns_zero(self):
        score, sql = DatatypeDetector()._detect_integer(["abc", "def"])
        assert score == 0.0
        assert sql is None


class TestDetectFloat:
    def test_all_float(self):
        assert DatatypeDetector()._detect_float(["1.5", "2.0"]) == 1.0

    def test_non_float_returns_zero(self):
        assert DatatypeDetector()._detect_float(["abc"]) == 0.0


class TestDetectDatetime:
    def test_date_only(self):
        score, sql = DatatypeDetector()._detect_datetime(["2024-01-01", "2024-02-01"])
        assert score == 1.0
        assert sql == "DATE"

    def test_timestamp(self):
        score, sql = DatatypeDetector()._detect_datetime(["2024-01-01 12:00:00"])
        assert score == 1.0
        assert sql == "TIMESTAMP"

    def test_non_date_returns_zero(self):
        score, sql = DatatypeDetector()._detect_datetime(["hello"])
        assert score == 0.0
        assert sql is None


class TestDetectCategorical:
    def test_low_cardinality_high_score(self):
        assert DatatypeDetector()._detect_categorical(["a", "a", "a", "b"]) == pytest.approx(1 - 2 / 4)

    def test_high_cardinality_zero(self):
        assert DatatypeDetector()._detect_categorical(["a", "b", "c"]) == 0.0

    def test_ratio_over_default_threshold_returns_zero(self):
        assert DatatypeDetector()._detect_categorical(["a", "a", "b"]) == 0.0


class TestInferColumn:
    def test_integer_column(self):
        result = _infer(DatatypeDetector(), [1, 2, 3, 4])
        assert result["type"] == "integer"
        assert result["postgres_type"] in ("SMALLINT", "INTEGER", "BIGINT")

    def test_float_column(self):
        result = _infer(DatatypeDetector(), [1.5, 2.5, 3.5])
        assert result["type"] == "float"

    def test_datetime_column(self):
        result = _infer(DatatypeDetector(), ["2024-01-01", "2024-02-01"])
        assert result["type"] == "datetime"
        assert result["postgres_type"] == "DATE"

    def test_text_column(self):
        result = _infer(DatatypeDetector(), ["alpha", "beta", "gamma", "delta"])
        assert result["type"] == "text"

    def test_boolean_column(self):
        result = _infer(DatatypeDetector(), ["true", "false", "true"])
        assert result["type"] == "boolean"

    def test_infer_column_series_wrapper(self):
        import pandas as pd
        series = pd.Series([1, 2, 3, 4])
        result = DatatypeDetector()._infer_column_(series)
        assert result["type"] == "integer"

    def test_null_values_ignored(self):
        result = _infer(DatatypeDetector(), [1, None, 3])
        assert result["type"] == "integer"
