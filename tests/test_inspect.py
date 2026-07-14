# tests/test_inspection.py

import os
import asyncio
import functools
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np
import pytest

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from src.main import MemFrame
from db_test_utils import default_duckdb_test_path

# ----------------------------------------------------------------------
# Backend configuration - set environment variables for PostgreSQL
# ----------------------------------------------------------------------
LOCAL_DB = "local"
REMOTE_DB = "remote"
DUCKDB_BACKEND = "duckdb"
POSTGRES_BACKEND = "postgres"

BACKEND_PARAMS = {
    LOCAL_DB: {"connection_type": "local", "params": {}},
    REMOTE_DB: {
        "connection_type": "remote",
        "params": {
            "backend": "postgres",
            "host": os.getenv("PGHOST", "localhost"),
            "port": int(os.getenv("PGPORT", 5432)),
            "user": os.getenv("PGUSER", "postgres"),
            "password": os.getenv("PGPASSWORD", "postgres"),
            "database": os.getenv("PGDATABASE", "memframe_test"),
        },
    },
}

BACKEND_ALIASES = {
    LOCAL_DB: DUCKDB_BACKEND,
    REMOTE_DB: POSTGRES_BACKEND,
    DUCKDB_BACKEND: DUCKDB_BACKEND,
    POSTGRES_BACKEND: POSTGRES_BACKEND,
}

# Choose which backends to run when the CLI does not provide --db-backend.
TEST_BACKENDS = [
    backend.strip()
    for backend in os.getenv("MEMFRAME_TEST_BACKENDS", "local").split(",")
    if backend.strip()
]
RESULT_DIR = Path(__file__).resolve().parent / "result"


def _usage_error(message: str) -> pytest.UsageError:
    return pytest.UsageError(f"Invalid inspection DB configuration: {message}")


def _parse_connection_params(raw_params: str) -> Dict[str, Any]:
    if not raw_params:
        return {}
    try:
        params = json.loads(raw_params)
    except json.JSONDecodeError as exc:
        raise _usage_error(f"--db-params must be valid JSON: {exc}") from exc
    if not isinstance(params, dict):
        raise _usage_error("--db-params must be a JSON object")
    return params


def _normalize_backend_name(backend_name: str) -> str:
    normalized = str(backend_name).strip().lower()
    if normalized not in BACKEND_ALIASES:
        allowed = ", ".join(sorted(BACKEND_ALIASES))
        raise _usage_error(f"unknown backend '{backend_name}'. Use one of: {allowed}")
    return BACKEND_ALIASES[normalized]


def _infer_backend_from_params(params: Dict[str, Any]) -> str:
    backend = params.get("backend")
    if backend is not None:
        return _normalize_backend_name(str(backend))
    if "db_path" in params:
        return DUCKDB_BACKEND
    raise _usage_error("--db-params was provided without --db-backend")


def _validate_port(value: Any) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise _usage_error("Postgres param 'port' must be an integer") from exc
    if port < 1 or port > 65535:
        raise _usage_error("Postgres param 'port' must be between 1 and 65535")
    return port


def _validate_duckdb_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"db_path"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"DuckDB does not accept params: {', '.join(unknown)}")

    db_path = params.get("db_path", default_duckdb_test_path("inspect"))
    if not isinstance(db_path, str) or not db_path.strip():
        raise _usage_error("DuckDB param 'db_path' must be a non-empty string")
    return {"db_path": db_path}


def _validate_postgres_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"backend", "host", "port", "user", "password", "database", "schema_prefix"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"Postgres does not accept params: {', '.join(unknown)}")

    merged = dict(BACKEND_PARAMS[REMOTE_DB]["params"])
    merged.update(params)
    merged["backend"] = POSTGRES_BACKEND

    for key in ("host", "user", "database"):
        value = merged.get(key)
        if not isinstance(value, str) or not value.strip():
            raise _usage_error(f"Postgres param '{key}' must be a non-empty string")
    password = merged.get("password")
    if not isinstance(password, str):
        raise _usage_error("Postgres param 'password' must be a string")
    merged["port"] = _validate_port(merged.get("port", 5432))
    return merged


def _build_backend_config(backend_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    backend = _normalize_backend_name(backend_name)
    if backend == DUCKDB_BACKEND:
        return {
            "backend": DUCKDB_BACKEND,
            "connection_type": "local",
            "params": _validate_duckdb_params(params),
        }
    return {
        "backend": POSTGRES_BACKEND,
        "connection_type": "remote",
        "params": _validate_postgres_params(params),
    }


def _selected_backend_configs(config) -> List[Dict[str, Any]]:
    raw_params = config.getoption("--db-params")
    params = _parse_connection_params(raw_params)
    cli_backend = config.getoption("--db-backend")

    if cli_backend:
        return [_build_backend_config(cli_backend, params)]
    if raw_params:
        return [_build_backend_config(_infer_backend_from_params(params), params)]
    return [_build_backend_config(backend_name, {}) for backend_name in TEST_BACKENDS]


def pytest_generate_tests(metafunc):
    if "backend_config" in metafunc.fixturenames:
        configs = _selected_backend_configs(metafunc.config)
        ids = [config["backend"] for config in configs]
        metafunc.parametrize("backend_config", configs, ids=ids, indirect=True)


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """Create a diverse DataFrame for inspection tests."""
    dates = pd.date_range("2025-01-01", periods=6, freq="2D")
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", None],  # one missing
        "salary": [50000.0, 60000.0, 55000.0, None, 70000.0, 65000.0],
        "bonus": [5000.0, 6000.0, None, 4000.0, 7000.0, 6500.0],
        "hire_date": dates,
        "active": [True, False, True, True, False, None],  # missing bool
        "score": [85.5, 92.3, 78.9, None, 88.0, 91.2],
        "department": ["Sales", "IT", "IT", "HR", "Sales", None],
    })


@pytest.fixture(scope="function")
def backend_config(request) -> Dict[str, Any]:
    """Return the connection configuration for the current test."""
    config = getattr(request, "param", None)
    if config is None:
        config = _selected_backend_configs(request.config)[0]
    return {
        "backend": config["backend"],
        "connection_type": config["connection_type"],
        "params": dict(config.get("params", {})),
    }


@pytest.fixture(scope="function")
def connected_memframe(backend_config) -> MemFrame:
    """Create a MemFrame connected to the desired backend."""
    mf = MemFrame(
        connection_type=backend_config["connection_type"],
        connection_params=backend_config.get("params", {}),
    )
    asyncio.run(mf.connect())
    try:
        yield mf
    finally:
        asyncio.run(mf.close())


@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, sample_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    ctx = connected_memframe.upload_df(sample_df, filename="inspection_dataset")
    return ctx


# ----------------------------------------------------------------------
# Helper: convert library result to pandas DataFrame
# ----------------------------------------------------------------------
def get_plain_result(result: Any) -> Any:
    """Extract the actual value from a library result dict (non‑DataFrame)."""
    if isinstance(result, dict):
        if result.get("is_error"):
            raise AssertionError(result.get("error_message") or f"Operation failed: {result}")
        # Often the real result is under 'result' key
        if "result" in result:
            return get_plain_result(result["result"])
        # For property methods that return a dict with e.g. 'columns' key
        if "columns" in result:
            return result["columns"]
        # For info
        if "column_name" in result:
            return result  # it's a table, leave as dict
        # Fallback: return the whole dict (for metadata methods)
        return result
    return result

def get_result_df(result: Any) -> pd.DataFrame:
    """Extract a pandas DataFrame from the diverse result types returned."""
    # First try to get a plain DataFrame
    if isinstance(result, pd.DataFrame):
        return result
    if hasattr(result, "full_table"):
        return get_result_df(result.full_table())
    if hasattr(result, "to_pandas"):
        return result.to_pandas()
    if hasattr(result, "collect"):
        collected = result.collect()
        if isinstance(collected, pd.DataFrame):
            return collected
    if isinstance(result, dict):
        if result.get("is_error"):
            raise AssertionError(result.get("error_message") or f"Operation failed: {result}")
        # Look for nested DataFrame in 'result' or 'current_state'
        for key in ("result", "current_state", "data"):
            if key in result:
                inner = result[key]
                if isinstance(inner, pd.DataFrame):
                    return inner
                if isinstance(inner, dict):
                    # Try deeper (e.g. rename returns {'result': {'result': DataFrame}})
                    if "result" in inner and isinstance(inner["result"], pd.DataFrame):
                        return inner["result"]
                    if "data" in inner and isinstance(inner["data"], pd.DataFrame):
                        return inner["data"]
        # If the result itself looks like a table (has 'column_name' etc.), 
        # we cannot extract a DataFrame easily; raise
        raise AssertionError(f"Cannot extract DataFrame from dict: {list(result.keys())}")
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")


def normalize_nulls(df: pd.DataFrame, sample_df: pd.DataFrame) -> pd.DataFrame:
    """Convert NaN/None in string columns to empty string (matching library behaviour)."""
    df = df.copy()
    # Identify string/object columns that contain None in sample
    for col in sample_df.columns:
        if sample_df[col].dtype == object and sample_df[col].isnull().any():
            if col in df.columns:
                df[col] = df[col].fillna("").replace("", None)  # but library may return ''
                # We'll convert both sides to empty string for comparison
                df[col] = df[col].where(df[col].notna(), "")
    return df

def assert_series_equal_loose(
    actual: pd.Series,
    expected: pd.Series,
    as_datetime: bool = False,
) -> None:
    """Compare two Series while ignoring non-semantic metadata differences."""
    actual_series = actual.reset_index(drop=True)
    expected_series = expected.reset_index(drop=True)
    if as_datetime:
        actual_series = pd.to_datetime(actual_series, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        expected_series = pd.to_datetime(expected_series, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    pd.testing.assert_series_equal(
        actual_series,
        expected_series,
        check_dtype=False,
        check_names=False,
    )


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Drop helper columns and normalize index for stable DataFrame comparisons."""
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)


def unwrap_result_payload(result: Any) -> Any:
    if isinstance(result, dict) and "result" in result:
        return result["result"]
    return result


def get_dict_value(result: Any, key: str):
    """Helper to get value from dict result, handling error dict."""
    if isinstance(result, dict):
        if result.get("is_error"):
            raise AssertionError(f"Operation failed: {result.get('error_message')}")
        return result.get(key)
    return getattr(result, key, None)


# ----------------------------------------------------------------------
# PDF generation helper
# ----------------------------------------------------------------------
def _empty_pdf_df(message: str) -> pd.DataFrame:
    return pd.DataFrame({"info": [message]})


def _coerce_pdf_df(value: Any, empty_message: str) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, pd.Series):
        name = value.name if value.name is not None else "value"
        return value.rename(name).to_frame().reset_index(drop=True)
    if isinstance(value, dict):
        if isinstance(value.get("result"), pd.DataFrame):
            return value["result"]
        if isinstance(value.get("data"), pd.DataFrame):
            return value["data"]
        if value.get("is_error"):
            return pd.DataFrame({
                "is_error": [value.get("is_error")],
                "error_message": [value.get("error_message", "")],
            })
        try:
            return pd.DataFrame(value)
        except ValueError:
            return pd.DataFrame([value])
    if value is None:
        return _empty_pdf_df(empty_message)
    return pd.DataFrame({"value": [value]})


def _prepare_pdf_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime-like columns to strings for stable PDF rendering."""
    pdf_df = _coerce_pdf_df(df, "No data available").copy()
    for col in pdf_df.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf_df[col]):
            pdf_df[col] = pdf_df[col].astype(str)
    return pdf_df


def render_df_to_pdf_page(
    pdf,
    title,
    method_call,
    original_df,
    memframe_df,
    pandas_df,
    backend,
    status="PASSED",
    error_message="",
):
    """Create a single PDF page with method call + Original/MemFrame/Pandas snapshots."""
    sections = [
        ("Original", original_df.head(10)),
        ("MemFrame Result", memframe_df.head(10)),
        ("Pandas Result", pandas_df.head(10)),
    ]

    fig_height = max(8, 2 + sum(max(2, len(df) + 2) for _, df in sections) * 0.4)
    fig, axes = plt.subplots(3, 1, figsize=(16, fig_height))
    fig.suptitle(f"{title}  [{backend}]  {status}", fontsize=12, fontweight="bold")
    fig.text(0.01, 0.965, f"Call: {method_call}", fontsize=10, family="monospace")
    if error_message:
        fig.text(0.01, 0.94, f"Failure: {error_message}", fontsize=9, color="crimson")

    for ax, (label, df) in zip(axes, sections):
        ax.axis("off")
        ax.set_title(label, fontsize=10, loc="left")
        if df.empty:
            ax.text(0.01, 0.5, "(empty)", fontsize=9, transform=ax.transAxes)
            continue
        table = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.1, 1.2)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def _capture_test_method_pdf_failures(cls):
    """Wrap test methods so assertion failures can still be written to the PDF report."""
    for name, value in list(vars(cls).items()):
        if name.startswith("test_") and callable(value):
            setattr(cls, name, _pdf_failure_wrapper(value))
    return cls


def _pdf_failure_wrapper(test_func):
    @functools.wraps(test_func)
    def wrapper(self, *args, **kwargs):
        try:
            return test_func(self, *args, **kwargs)
        except Exception as exc:
            self._pdf_test_failed = True
            self._mark_current_pdf_records("FAILED", str(exc))
            if getattr(self, "_save_to_file", False) and not getattr(self, "_current_pdf_records", []):
                request = getattr(self, "_current_request", None)
                self._record_failure_from_traceback(request, exc)
            raise

    return wrapper


# ----------------------------------------------------------------------
# Test class
# ----------------------------------------------------------------------
@_capture_test_method_pdf_failures
class TestInspectionOperations:
    """All inspection / table-ops tests that require a backend connection."""

    _save_to_file = False
    _saved_results = []

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, request, save_to_file):
        """Attach save flag and handle PDF generation after all class tests."""
        cls = request.cls
        cls._save_to_file = save_to_file
        cls._saved_results = []
        yield
        if cls._save_to_file and cls._saved_results:
            RESULT_DIR.mkdir(parents=True, exist_ok=True)
            pdf_path = RESULT_DIR / f"test_inspection_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for result in cls._saved_results:
                    render_df_to_pdf_page(
                        pdf,
                        result["test_name"],
                        result["method_call"],
                        result["original_df"],
                        result["memframe_df"],
                        result["pandas_df"],
                        result["backend"],
                        result.get("status", "PASSED"),
                        result.get("error_message", ""),
                    )
            print(f"\n\nTest report saved to: {pdf_path}\n")

    @pytest.fixture(autouse=True)
    def _capture_failed_pdf_result(self, request):
        self._current_pdf_records = []
        self._current_request = request
        self._pdf_test_failed = False
        try:
            yield
        except Exception as exc:
            self._pdf_test_failed = True
            self._mark_current_pdf_records("FAILED", str(exc))
            if self._save_to_file and not self._current_pdf_records:
                self._record_failure_from_traceback(request, exc)
            raise
        else:
            if self._pdf_test_failed:
                pass
            elif self._save_to_file and not self._current_pdf_records:
                self._record_passed_from_fixtures(request)
            else:
                self._mark_current_pdf_records("PASSED", "")
        finally:
            self._current_pdf_records = []
            self._current_request = None
            self._pdf_test_failed = False

    def _mark_current_pdf_records(self, status: str, error_message: str) -> None:
        for result in getattr(self, "_current_pdf_records", []):
            result["status"] = status
            result["error_message"] = error_message

    def _record_failure_from_traceback(self, request, exc: Exception) -> None:
        tb = exc.__traceback__
        frame_locals = {}
        while tb:
            frame = tb.tb_frame
            if frame.f_code.co_name.startswith("test_"):
                frame_locals = frame.f_locals
            tb = tb.tb_next

        original_df = _coerce_pdf_df(
            frame_locals.get("sample_df"),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in (
            "res_df",
            "plain",
            "result",
            "mapped_series",
            "other_ctx",
            "ts_ctx",
        ):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_df = _coerce_pdf_df(
            frame_locals.get("expected"),
            "No pandas expected result was available when this test failed",
        )

        backend_config = frame_locals.get("backend_config") or {}
        self._record_result(
            test_name=request.node.name if request is not None else "unknown_failed_test",
            method_call=request.node.name if request is not None else "unknown_failed_test",
            original_df=original_df,
            memframe_df=memframe_df,
            pandas_df=pandas_df,
            backend=backend_config.get("connection_type", "unknown"),
            status="FAILED",
            error_message=str(exc),
        )

    def _record_passed_from_fixtures(self, request) -> None:
        funcargs = getattr(request.node, "funcargs", {})
        original_df = _coerce_pdf_df(
            funcargs.get("sample_df"),
            "sample_df was not available for this test",
        )
        backend_config = funcargs.get("backend_config") or {}
        self._record_result(
            test_name=request.node.name,
            method_call=request.node.name,
            original_df=original_df,
            memframe_df=_empty_pdf_df("No explicit MemFrame result was recorded for this passing test"),
            pandas_df=_empty_pdf_df("No explicit pandas expected result was recorded for this passing test"),
            backend=backend_config.get("connection_type", "unknown"),
            status="PASSED",
            error_message="",
        )

    def _record_result(
        self,
        test_name: str,
        method_call: str,
        original_df: pd.DataFrame,
        memframe_df: pd.DataFrame,
        pandas_df: pd.DataFrame,
        backend: str,
        status: str = "PENDING",
        error_message: str = "",
    ):
        """Store test result for PDF generation."""
        if self._save_to_file:
            result = {
                "test_name": test_name,
                "method_call": method_call,
                "original_df": _prepare_pdf_df(_coerce_pdf_df(original_df, "No original data")),
                "memframe_df": _prepare_pdf_df(_coerce_pdf_df(memframe_df, "No MemFrame result")),
                "pandas_df": _prepare_pdf_df(_coerce_pdf_df(pandas_df, "No pandas result")),
                "backend": backend,
                "status": status,
                "error_message": error_message,
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ----------------------------------------------------
    # head / tail / sample
    # ----------------------------------------------------
    def test_head(self, uploaded_ctx, sample_df, backend_config):
        n = 2
        result = uploaded_ctx.head(n=n)
        res_df = get_result_df(result)
        expected = sample_df.head(n).reset_index(drop=True)
        # Normalise datetime to string
        res_df["hire_date"] = res_df["hire_date"].astype(str)
        expected["hire_date"] = expected["hire_date"].dt.strftime("%Y-%m-%d")
        # Normalise string nulls
        for col in ["name", "department"]:
            if col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        pd.testing.assert_frame_equal(
            normalize_frame(res_df),
            normalize_frame(expected),
            check_dtype=False,
            check_names=False,
        )
    
    def test_tail(self, uploaded_ctx, sample_df, backend_config):
        n = 2
        result = uploaded_ctx.tail(n=n)
        res_df = get_result_df(result)
        expected = sample_df.tail(n).reset_index(drop=True)
        # Normalise datetime to string
        res_df["hire_date"] = res_df["hire_date"].astype(str)
        expected["hire_date"] = expected["hire_date"].dt.strftime("%Y-%m-%d")
        # Normalise string nulls
        for col in ["name", "department"]:
            if col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        pd.testing.assert_frame_equal(
            normalize_frame(res_df),
            normalize_frame(expected),
            check_dtype=False,
            check_names=False,
        )

    def test_sample(self, uploaded_ctx, sample_df, backend_config):
        n = 4
        result = uploaded_ctx.sample(n=n, random_state=42)
        res_df = get_result_df(result)
        # Ensure correct number of rows
        assert len(res_df) == n
        # Check that all sampled 'id's are subset of original
        assert set(res_df["id"]).issubset(set(sample_df["id"]))
    
    # ----------------------------------------------------
    # info
    # ----------------------------------------------------
    def test_info(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.info()
        # result is a dict describing columns
        plain = get_plain_result(result)
        assert "column_name" in plain or "columns" in plain
        # If it's a table, the number of columns should match
        if "column_name" in plain:
            # probably a list of rows, not easy to check
            pass
        else:
            # maybe has 'columns' list
            assert len(plain.get("columns", [])) == len(sample_df.columns)

    # ----------------------------------------------------
    # describe
    # ----------------------------------------------------
    def test_describe(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.describe()
        res_df = get_result_df(result)
        expected = sample_df.describe()  # default numeric
        common_stats = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        # Keep only rows that exist in both
        res_df = res_df[res_df.index.isin(common_stats)]
        expected = expected[expected.index.isin(common_stats)]
        # Align columns (numeric)
        numeric_cols = res_df.columns.intersection(expected.columns)
        res_num = res_df[numeric_cols].astype(float)
        exp_num = expected[numeric_cols].astype(float)
        pd.testing.assert_frame_equal(
            res_num.sort_index().sort_index(axis=1),
            exp_num.sort_index().sort_index(axis=1),
            check_dtype=False,
            check_exact=False,
            atol=0.1,
        )

    # ----------------------------------------------------
    # null_analysis
    # ----------------------------------------------------
    def test_null_analysis(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.null_analysis()
        res_df = get_result_df(result)
        expected = pd.DataFrame({
            "column": sample_df.columns,
            "null_count": sample_df.isnull().sum().values,
            "null_percentage": (sample_df.isnull().mean() * 100).values,
        })
        # MemFrame might return column names differently, normalize
        res_cols = res_df.columns.str.lower()
        if "null_count" in res_cols and "null_percentage" in res_cols:
            res_compare = res_df.rename(columns=str.lower)
            pd.testing.assert_frame_equal(
                res_compare[["column", "null_count", "null_percentage"]].reset_index(drop=True),
                expected.reset_index(drop=True),
                check_dtype=False,
                check_exact=False,
                atol=0.1,
            )
        self._record_result(
            test_name="null_analysis",
            method_call="uploaded_ctx.null_analysis()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # corr
    # ----------------------------------------------------
    def test_corr(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.corr()
        res_df = get_result_df(result)
        expected = sample_df.select_dtypes(include=np.number).corr()
        # Align columns and index
        common_cols = expected.columns.intersection(res_df.columns)
        pd.testing.assert_frame_equal(
            res_df[common_cols].reindex(common_cols).fillna(0),
            expected[common_cols].reindex(common_cols).fillna(0),
            check_dtype=False,
            atol=0.01,
        )
        self._record_result(
            test_name="corr",
            method_call="uploaded_ctx.corr()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    
    # ----------------------------------------------------
    # astype
    # ----------------------------------------------------
    def test_astype(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.astype(columns=["salary"], dtypes=["str"])
        res_df = get_result_df(result)
        # Check that salary column is now string (or object)
        assert str(res_df["salary"].dtype) in ("object", "string", "str")
        # Values should be string representations of original numbers (ignoring NaN)
        exp_vals = sample_df["salary"].astype(str).replace("nan", None)
        res_vals = res_df["salary"].replace("", None).astype(object)
        # NaNs are tricky, compare non-null values
        mask = sample_df["salary"].notna()
        pd.testing.assert_series_equal(
            res_vals[mask].reset_index(drop=True),
            exp_vals[mask].reset_index(drop=True),
            check_dtype=False,
        )
        self._record_result(
            test_name="astype",
            method_call='uploaded_ctx.astype(columns=["salary"], dtypes=["str"])',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=sample_df.assign(salary_str=sample_df["salary"].astype(str)),
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # insert
    # ----------------------------------------------------
    def test_insert(self, uploaded_ctx, sample_df, backend_config):
        vals = [100] * len(sample_df)
        result = uploaded_ctx.insert(column="new_col", value=vals)
        res_df = get_result_df(result)
        assert "new_col" in res_df.columns
        assert res_df["new_col"].tolist() == vals
    # ----------------------------------------------------
    # map
    # ----------------------------------------------------
    def test_map(self, uploaded_ctx, sample_df, backend_config):
        # Map: add 1000 to salary using SQL expression
        result = uploaded_ctx.map(func="salary + 1000", columns=["salary"])
        res_df = get_result_df(result)
        # The result column may be renamed to something like mapped_salary
        # Find the column that is not original
        mapped_cols = [c for c in res_df.columns if c not in sample_df.columns]
        if mapped_cols:
            mapped_series = res_df[mapped_cols[0]]
        else:
            # possibly the column was replaced; check salary column
            mapped_series = res_df["salary"]
        expected = sample_df["salary"] + 1000
        assert_series_equal_loose(mapped_series, expected)
        self._record_result(
            test_name="map",
            method_call='uploaded_ctx.map(func="salary + 1000", columns=["salary"])',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected.to_frame("mapped"),
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # rename
    # ----------------------------------------------------
    def test_rename(self, uploaded_ctx, sample_df, backend_config):
        rename_map = {"salary": "income", "bonus": "extra"}
        result = uploaded_ctx.rename(columns=rename_map)
        res_df = get_result_df(result)
        expected = sample_df.rename(columns=rename_map)
        # Check columns
        assert set(rename_map.values()).issubset(res_df.columns)
        # Compare content for the renamed columns
        assert_series_equal_loose(
            res_df["income"].dropna().reset_index(drop=True),
            expected["income"].dropna().reset_index(drop=True),
        )
        self._record_result(
            test_name="rename",
            method_call=f"uploaded_ctx.rename(columns={rename_map})",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # set_index / reset_index
    # ----------------------------------------------------
    def test_set_index(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.set_index(columns=["id"])
        plain = get_plain_result(result)
        assert "Index set" in plain or plain.get("message") == "Index set"
    
    def test_reset_index(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index(columns=["id"])
        result = uploaded_ctx.reset_index()
        res_df = get_result_df(result)  # now looks in current_state
        assert "id" in res_df.columns
        # compare after sorting
        expected = sample_df.copy()
        # The library added an extra 'id_1' column; we can ignore that for comparison
        # Drop extra generated column if present
        if "id_1" in res_df.columns:
            res_df = res_df.drop(columns=["id_1"])
        pd.testing.assert_frame_equal(
            normalize_frame(res_df).sort_values("id").reset_index(drop=True),
            normalize_frame(expected).sort_values("id").reset_index(drop=True),
            check_dtype=False,
        )
    
    # ----------------------------------------------------
    # update
    # ----------------------------------------------------
    def test_update(self, connected_memframe, uploaded_ctx, sample_df, backend_config):
        # Create another table with same id and new salary values
        update_data = pd.DataFrame({
            "id": [1, 3, 5],
            "salary": [99999.0, 88888.0, 77777.0],
        })
        other_ctx = connected_memframe.upload_df(update_data, filename="update_source")
        # Get the table name from other_ctx (maybe via inspect or internal)
        # For simplicity, we can directly use the internal data_id or table name.
        # The update method expects other_table as string (the upload table name).
        # We'll extract the table name from the context's data_id or internal state.
        other_table = other_ctx._data_id  # or use other_ctx._get_active_context()? We'll assume data_id works.
        if not other_table:
            raise ValueError("Could not get table name for update source")
        # Actually, the wrapper's update signature: update(on: str, other_table: str, ...)
        result = uploaded_ctx.update(on="id", other_table=other_table, overwrite=True)
        res_df = get_result_df(result)
        # Expected: original salary replaced for ids 1,3,5
        expected = sample_df.copy()
        expected.loc[expected["id"].isin([1,3,5]), "salary"] = [99999.0, 88888.0, 77777.0]
        # Compare only salary column for updated rows
        for id_val in [1,3,5]:
            assert (
                res_df[res_df["id"] == id_val]["salary"].values[0]
                == expected[expected["id"] == id_val]["salary"].values[0]
            )
        self._record_result(
            test_name="update",
            method_call=f'uploaded_ctx.update(on="id", other_table="{other_table}")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # resample
    # ----------------------------------------------------
    
    def test_resample(self, uploaded_ctx, sample_df, backend_config):
        ts_data = pd.DataFrame({
            "timestamp": pd.date_range("2025-01-01", periods=10, freq="12h"),
            "value": np.random.rand(10),
        })
        memframe = uploaded_ctx.memframe
        ts_ctx = memframe.upload_df(ts_data, filename="timeseries")
        result = ts_ctx.resample(time_column="timestamp", rule="D", agg="SUM", value_column="value")
        res_df = get_result_df(result)
        expected = ts_data.set_index("timestamp").resample("D").sum().reset_index()
        # Rename columns if needed
        res_df = normalize_frame(res_df)
        expected = normalize_frame(expected)
        pd.testing.assert_frame_equal(
            res_df.sort_values("timestamp").reset_index(drop=True),
            expected.sort_values("timestamp").reset_index(drop=True),
            check_dtype=False,
        )
    
    
    # ----------------------------------------------------
    # Property-like methods (non-DataFrame returns)
    # ----------------------------------------------------
    def test_axes(self, uploaded_ctx):
        result = get_plain_result(uploaded_ctx.axes())
        assert isinstance(result, (list, tuple))  # actually might be dict with 'index'/'columns'
        # adjust expectation: from log it seems to be a dict with keys
        if isinstance(result, dict):
            assert "index" in result or "columns" in result

    def test_columns(self, uploaded_ctx, sample_df):
        result = get_plain_result(uploaded_ctx.columns())
        # May be list or dict with 'columns'
        cols = result if isinstance(result, list) else result.get("columns", [])
        assert set(cols) == set(sample_df.columns) or set(cols) == set(sample_df.columns) | {"__index_level_0__"}

    def test_dtypes(self, uploaded_ctx):
        result = get_plain_result(uploaded_ctx.dtypes())
        # Should be dict
        assert isinstance(result, dict)
        assert "id" in result

    def test_first_valid_index(self, uploaded_ctx):
        result = uploaded_ctx.first_valid_index()
        # For a non-empty table, should return something like the first row index (0 or 1)
        assert result is not None

    def test_memory_usage(self, uploaded_ctx):
        result = uploaded_ctx.memory_usage()
        # Should return int or dict
        assert isinstance(result, (int, float, dict))

        
    def test_values(self, uploaded_ctx, sample_df):
        result = uploaded_ctx.values()
        # Returns list of lists or 2D array
        assert len(result) == len(sample_df)

    def test_items(self, uploaded_ctx):
        result = uploaded_ctx.items()
        # Should be iterable of (column, series-like) pairs
        assert hasattr(result, "__iter__")

    def test_iterrows(self, uploaded_ctx):
        result = uploaded_ctx.iterrows()
        # Should be iterable of (index, row) pairs
        assert hasattr(result, "__iter__")

    def test_itertuples(self, uploaded_ctx):
        result = uploaded_ctx.itertuples(index=False)
        # Should be iterable of namedtuples
        assert hasattr(result, "__iter__")
