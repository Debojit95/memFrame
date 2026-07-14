import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import chardet
import numpy as np
import pyarrow as pa
import pandas as pd
from pyarrow import ChunkedArray

logger = logging.getLogger("memFrame")

# ----------------------------------------------------------------------
#  Compiled regex patterns for performance
# ----------------------------------------------------------------------
_INTEGER_PATTERN = re.compile(r"[+-]?\d+")
_FLOAT_PATTERN = re.compile(r"[+-]?(?:\d+\.\d+|\d+\.\d*|\.\d+|\d+)")
_BOOLEAN_SET = frozenset(
    {"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f",
     "True", "False", "T", "F", "TRUE", "FALSE", 'Y', 'N', "YES", "NO"}
)

class Backend:
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    CLICKHOUSE = "clickhouse"


def _generate_6char_id() -> str:
    import random
    import string
    return "".join(random.choices(string.ascii_letters + string.digits, k=6))


class DatatypeDetector:
    """
    Heuristic column‑type detection working on PyArrow arrays.
    No pandas dependency.
    """
    def __init__(
        self,
        threshold: float = 0.8,
        sample_size: int = 5000,
        chunk_size: int = 100000,
        max_categorical_unique_ratio: float = 0.5,
        max_varchar_length: int = 65535,
        use_varchar_with_length: bool = False,
        varchar_buffer_factor: float = 1.5,
    ):
        self.threshold = threshold
        self.sample_size = sample_size
        self.chunk_size = chunk_size
        self.max_categorical_unique_ratio = max_categorical_unique_ratio
        self.max_varchar_length = max_varchar_length
        self.use_varchar_with_length = use_varchar_with_length
        self.varchar_buffer_factor = varchar_buffer_factor

    # ------------------------------------------------------------------
    #  Encoding detection (unchanged)
    # ------------------------------------------------------------------
    def _detect_encoding(self, file_path: str) -> str:
        try:
            with open(file_path, "rb") as f:
                raw = f.read(128 * 1024)
                result = chardet.detect(raw)
                enc = result.get("encoding")
                if enc and result.get("confidence", 0) > 0.7:
                    try:
                        with open(file_path, "r", encoding=enc) as test:
                            for _ in range(10):
                                test.readline()
                        return enc
                    except UnicodeDecodeError:
                        pass
        except Exception:
            pass

        for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"]:
            try:
                with open(file_path, "r", encoding=enc) as f:
                    for _ in range(100):
                        f.readline()
                return enc
            except UnicodeDecodeError:
                continue
        return "latin-1"

    @staticmethod
    def to_postgres_encoding(python_encoding: str) -> str:
        mapping = {
            "ascii": "SQL_ASCII",
            "utf-8": "UTF8",
            "utf8": "UTF8",
            "latin-1": "LATIN1",
            "iso-8859-1": "LATIN1",
            "cp1252": "WIN1252",
        }
        return mapping.get(python_encoding.lower(), "UTF8")

    # ---------- Type detection helpers (now operate on lists) ----------
    def _normalize_string_list(self, values: List[Optional[str]]) -> List[str]:
        """Strip whitespace and remove empty/None strings."""
        return [s.strip() for s in values if s is not None and s.strip() != ""]

    def _detect_boolean(self, values: List[str]) -> float:
        if not values:
            return 0.0
        lower = [v.lower() for v in values]
        return 1.0 if all(v in _BOOLEAN_SET for v in lower) else 0.0

    def _detect_integer(self, values: List[str]) -> Tuple[float, Optional[str]]:
        if not values:
            return 0.0, None

        valid = [v for v in values if _INTEGER_PATTERN.fullmatch(v)]
        if not valid:
            return 0.0, None

        try:
            int_values = [int(v) for v in valid]
        except ValueError:
            return 0.0, None

        score = len(valid) / len(values)
        min_val = min(int_values)
        max_val = max(int_values)

        if min_val >= -32768 and max_val <= 32767:
            sql_type = "SMALLINT"
        elif min_val >= -2147483648 and max_val <= 2147483647:
            sql_type = "INTEGER"
        else:
            sql_type = "BIGINT"

        return score, sql_type

    def _detect_float(self, values: List[str]) -> float:
        if not values:
            return 0.0

        valid = [v for v in values if _FLOAT_PATTERN.fullmatch(v)]
        if not valid:
            return 0.0

        try:
            [float(v) for v in valid]
        except ValueError:
            return 0.0

        return len(valid) / len(values)

    def _detect_datetime(self, values: List[str]) -> Tuple[float, Optional[str]]:
        """Detect datetime using a list of string values."""
        import datetime

        if not values:
            return 0.0, None

        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y/%m/%d %H:%M:%S%z", "%d/%m/%Y %H:%M:%S%z", "%m/%d/%Y %H:%M:%S%z",
            "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
            "%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y%m%d",
        ]

        def _infer_subtype(converted_dates: List[datetime.datetime]) -> str:
            # Determine if time component is present and if timezone aware
            has_time = any(
                d.hour != 0 or d.minute != 0 or d.second != 0 or d.microsecond != 0
                for d in converted_dates
            )
            tz_aware = any(d.tzinfo is not None for d in converted_dates)
            if tz_aware:
                return "TIMESTAMPTZ"
            if has_time:
                return "TIMESTAMP"
            return "DATE"

        best_score = 0.0
        best_subtype = None

        for fmt in formats:
            converted = []
            valid_count = 0
            for v in values:
                try:
                    dt = datetime.datetime.strptime(v, fmt)
                    converted.append(dt)
                    valid_count += 1
                except ValueError:
                    converted.append(None)
            score = valid_count / len(values)
            if score > best_score:
                best_score = score
                best_subtype = _infer_subtype([d for d in converted if d is not None])
            if best_score >= self.threshold:
                return best_score, best_subtype

        return best_score, best_subtype if best_score > 0 else None

    def _detect_categorical(self, values: List[str]) -> float:
        if not values:
            return 0.0
        unique_ratio = len(set(values)) / len(values)
        if unique_ratio > self.max_categorical_unique_ratio:
            return 0.0
        return 1.0 - unique_ratio

    def _get_string_type(self, values: List[str]) -> str:
        if not self.use_varchar_with_length:
            return "TEXT"
        max_len = max((len(v) for v in values), default=0)
        if max_len == 0:
            max_len = 255
        else:
            max_len = min(int(max_len * self.varchar_buffer_factor), self.max_varchar_length)
        return f"VARCHAR({max_len})"

    def _infer_column(self, chunked_array: ChunkedArray) -> Dict[str, Any]:
        """
        Receive a PyArrow ChunkedArray and return type detection result.
        """
        series = chunked_array
        

        # Take a sample
        if len(series) > self.sample_size:
            # random sample: combine all chunks, sample, then rebuild
            table = pa.table({"col": series})
            sampled_table = table.take(pa.array(np.random.choice(len(table), self.sample_size, replace=False)))
            series = sampled_table.column("col")

        # Convert to Python list 
        raw_values = series.to_pylist()

        # Filter null-like values properly
        cleaned = []
        for v in raw_values:
            if v is None:
                continue
            if isinstance(v, float) and np.isnan(v):
                continue

            s = str(v).strip()

            # Remove empty / null-like strings
            if s == "" or s.lower() in {"nan", "none", "null"}:
                continue

            cleaned.append(s)

        str_values = cleaned
        
        
        # DateTime detection
        dt_conf, dt_sql_type = self._detect_datetime(str_values)
        if dt_conf >= self.threshold:
            return {"type": "datetime", "confidence": dt_conf, "postgres_type": dt_sql_type}

        # Other types
        bool_conf = self._detect_boolean(str_values)
        int_conf, int_sql_type = self._detect_integer(str_values)
        float_conf = self._detect_float(str_values)
        cat_conf = self._detect_categorical(str_values)

        scores = {
            "boolean": bool_conf,
            "integer": int_conf,
            "float": float_conf,
            "datetime": dt_conf,
            "categorical": cat_conf,
        }

        best_type = max(scores, key=scores.get)
        best_score = scores[best_type]

        # If nothing meets threshold, fallback to TEXT
        typed_scores = {k: scores[k] for k in ("boolean", "integer", "float", "datetime")}
        has_partial_typed_signal = any(0.0 < v < 1.0 for v in typed_scores.values())

        if best_score < self.threshold or (best_type in typed_scores and best_score < 1.0):
            return {"type": "text", "confidence": best_score, "postgres_type": "TEXT"}

        if best_type == "boolean":
            return {"type": "boolean", "confidence": best_score, "postgres_type": "BOOLEAN"}
        elif best_type == "integer":
            return {"type": "integer", "confidence": best_score, "postgres_type": int_sql_type}
        elif best_type == "float":
            return {"type": "float", "confidence": best_score, "postgres_type": "FLOAT"}  # or NUMERIC
        elif best_type == "datetime":
            return {"type": "datetime", "confidence": best_score, "postgres_type": dt_sql_type}
        elif has_partial_typed_signal:
            return {"type": "text", "confidence": best_score, "postgres_type": "TEXT"}
        else:
            return {"type": "categorical", "confidence": best_score, "postgres_type": self._get_string_type(str_values)}


    def _infer_column_(self, series: "pd.Series") -> Dict[str, Any]:
        """
        Wrap a pandas Series into a PyArrow ChunkedArray and delegate to _infer_column.
        This keeps all detection logic, thresholds, and sampling exactly as in the
        original PyArrow path.
        """
        # Convert to PyArrow array (drop NaN → None so that pyarrow nulls are used)
        arrow_array = pa.array(series.where(series.notna(), None))

        # Wrap in a ChunkedArray for _infer_column
        chunked = pa.chunked_array([arrow_array])

        # Delegate to the existing, proven method
        return self._infer_column(chunked)
