# tests/test_arithmetic.py

import os
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np
import pytest


import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


from memframe.main import MemFrame

# ----------------------------------------------------------------------
# Backend configuration - set environment variables for PostgreSQL
# ----------------------------------------------------------------------
LOCAL_DB = "local"
REMOTE_DB = "remote"
DUCKDB_BACKEND = "duckdb"
POSTGRES_BACKEND = "postgres"
CLICKHOUSE_BACKEND = "clickhouse"

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
    CLICKHOUSE_BACKEND: CLICKHOUSE_BACKEND,
}

# Choose which backends to run when the CLI does not provide --db-backend.
TEST_BACKENDS = [
    backend.strip()
    for backend in os.getenv("MEMFRAME_TEST_BACKENDS", "local").split(",")
    if backend.strip()
]
RESULT_DIR = Path(__file__).resolve().parent / "result"


def _usage_error(message: str) -> pytest.UsageError:
    return pytest.UsageError(f"Invalid arithmetic DB configuration: {message}")


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

    db_path = params.get("db_path", "memFrame_new.duckdb")
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


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _validate_clickhouse_params(params: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"backend", "host", "port", "user", "password", "database", "secure", "timeout", "schema_prefix"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _usage_error(f"ClickHouse does not accept params: {', '.join(unknown)}")

    merged: Dict[str, Any] = {
        "backend": CLICKHOUSE_BACKEND,
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv("CLICKHOUSE_PORT", 8123),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "secure": _env_bool("CLICKHOUSE_SECURE", False),
    }
    if os.getenv("CLICKHOUSE_DATABASE"):
        merged["database"] = os.getenv("CLICKHOUSE_DATABASE")
    if os.getenv("CLICKHOUSE_TIMEOUT"):
        merged["timeout"] = os.getenv("CLICKHOUSE_TIMEOUT")
    merged.update(params)
    merged["backend"] = CLICKHOUSE_BACKEND

    for key in ("host", "user"):
        value = merged.get(key)
        if not isinstance(value, str) or not value.strip():
            raise _usage_error(f"ClickHouse param '{key}' must be a non-empty string")
    password = merged.get("password")
    if not isinstance(password, str):
        raise _usage_error("ClickHouse param 'password' must be a string")
    database = merged.get("database")
    if database is not None and (not isinstance(database, str) or not database.strip()):
        raise _usage_error("ClickHouse param 'database' must be a non-empty string")
    secure = merged.get("secure", False)
    if isinstance(secure, str):
        secure = secure.strip().lower() in {"1", "true", "yes", "on"}
    if not isinstance(secure, bool):
        raise _usage_error("ClickHouse param 'secure' must be a boolean")
    merged["secure"] = secure
    if "timeout" in merged:
        try:
            merged["timeout"] = float(merged["timeout"])
        except (TypeError, ValueError) as exc:
            raise _usage_error("ClickHouse param 'timeout' must be a number") from exc
    merged["port"] = _validate_port(merged.get("port", 8123))
    return merged


def _build_backend_config(backend_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    backend = _normalize_backend_name(backend_name)
    if backend == DUCKDB_BACKEND:
        return {
            "backend": DUCKDB_BACKEND,
            "connection_type": "local",
            "params": _validate_duckdb_params(params),
        }
    if backend == POSTGRES_BACKEND:
        return {
            "backend": POSTGRES_BACKEND,
            "connection_type": "remote",
            "params": _validate_postgres_params(params),
        }
    return {
        "backend": CLICKHOUSE_BACKEND,
        "connection_type": "remote",
        "params": _validate_clickhouse_params(params),
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
    """Create the reference DataFrame with columns needed for arithmetic tests."""
    return pd.DataFrame({
        "salary": [1000, 2000, 3000],
        "bonus": [100, 200, 300],
        "tax": [50, 100, 150],
        "score": [10.5, 20.7, 30.2],
        "angle": [0.0, 0.5, 1.0],
        "math": [80, 90, 100],
        "science": [70, 85, 95],
        "old_price": [100, 200, 400],
        "new_price": [120, 250, 500],
        "negative_vals": [-10, -20, -30],
        "float_vals": [10.1234, 20.5678, 30.9999],
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
    asyncio.run(mf.aconnect())
    try:
        yield mf
    finally:
        asyncio.run(mf.aclose())


@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, sample_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    ctx = connected_memframe.upload_df(sample_df, filename="arithmetic_dataset")
    return ctx


# ----------------------------------------------------------------------
# Helper: convert library result to pandas DataFrame
# ----------------------------------------------------------------------
def get_result_df(result: Any) -> pd.DataFrame:
    """Extract a pandas DataFrame from the diverse result types returned."""
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
        if "result" in result and isinstance(result["result"], pd.DataFrame):
            return result["result"]
        if "data" in result and isinstance(result["data"], pd.DataFrame):
            return result["data"]
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")


def get_generated_col(result: Any, fallback: str) -> str:
    """Retrieve the name of the new column (if renamed) else fallback."""
    if isinstance(result, dict):
        cols = result.get("generated_cols") or []
        if cols:
            return cols[0]
    return fallback


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
    # Optionally convert known string columns to avoid dtype mismatches
    return out.reset_index(drop=True)


def unwrap_result_payload(result: Any) -> Any:
    if isinstance(result, dict) and "result" in result:
        return result["result"]
    return result


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


# ----------------------------------------------------------------------
# Parametrize all tests with both backends
# ----------------------------------------------------------------------
class TestArithmeticOperations:
    """All arithmetic tests that require a backend connection."""

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
            pdf_path = RESULT_DIR / f"test_arithmetic_report_{request.node.name}.pdf"
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
        try:
            yield
        except Exception as exc:
            self._mark_current_pdf_records("FAILED", str(exc))
            if self._save_to_file and not self._current_pdf_records:
                self._record_failure_from_traceback(request, exc)
            raise
        else:
            self._mark_current_pdf_records("PASSED", "")
        finally:
            self._current_pdf_records = []

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
            frame_locals.get("df", frame_locals.get("sample_df")),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "original_df", "result"):
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
            test_name=request.node.name,
            method_call=request.node.name,
            original_df=original_df,
            memframe_df=memframe_df,
            pandas_df=pandas_df,
            backend=backend_config.get("connection_type", "unknown"),
            status="FAILED",
            error_message=str(exc),
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
    # Binary operations
    # ----------------------------------------------------
    def test_add(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.add("salary", "bonus", "total_income")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "total_income")

        expected = sample_df.copy()
        expected["total_income"] = expected["salary"] + expected["bonus"]
        assert_series_equal_loose(res_df[out_col], expected["total_income"])
        self._record_result(
            test_name="add",
            method_call='uploaded_ctx.add("salary", "bonus", "total_income")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_subtract(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.subtract("salary", "tax", "salary_after_tax")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary_after_tax")

        expected = sample_df.copy()
        expected["salary_after_tax"] = expected["salary"] - expected["tax"]
        assert_series_equal_loose(res_df[out_col], expected["salary_after_tax"])
        self._record_result(
            test_name="subtract",
            method_call='uploaded_ctx.subtract("salary", "tax", "salary_after_tax")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_multiply(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mul("salary", 2, "double_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "double_salary")

        expected = sample_df.copy()
        expected["double_salary"] = expected["salary"] * 2
        assert_series_equal_loose(res_df[out_col], expected["double_salary"])
        self._record_result(
            test_name="multiply",
            method_call='uploaded_ctx.mul("salary", 2, "double_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_multiply_numeric_text_column(self, connected_memframe, backend_config):
        df = pd.DataFrame({
            "A": [2.0, -4.0, 10.0],
            "B": ["1e-3", "2.5", "-3E2"],
        })
        ctx = connected_memframe.upload_df(df, filename="arithmetic_numeric_text")

        result = ctx.mul("A", "B", "product")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "product")

        expected = df.copy()
        expected["product"] = expected["A"] * pd.to_numeric(expected["B"])
        assert_series_equal_loose(res_df[out_col], expected["product"])
        self._record_result(
            test_name="multiply_numeric_text_column",
            method_call='uploaded_ctx.mul("A", "B", "product")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_divide(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.div("salary", 2, "half_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "half_salary")

        expected = sample_df.copy()
        expected["half_salary"] = expected["salary"] / 2
        assert_series_equal_loose(res_df[out_col], expected["half_salary"])
        self._record_result(
            test_name="divide",
            method_call='uploaded_ctx.div("salary", 2, "half_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_modulo(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mod("salary", 300, "salary_mod")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary_mod")

        expected = sample_df.copy()
        expected["salary_mod"] = expected["salary"] % 300
        assert_series_equal_loose(res_df[out_col], expected["salary_mod"])
        self._record_result(
            test_name="modulo",
            method_call='uploaded_ctx.mod("salary", 300, "salary_mod")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_power(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.pow("tax", 2, "tax_squared")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "tax_squared")

        expected = sample_df.copy()
        expected["tax_squared"] = expected["tax"] ** 2
        assert_series_equal_loose(res_df[out_col], expected["tax_squared"])
        self._record_result(
            test_name="power",
            method_call='uploaded_ctx.pow("tax", 2, "tax_squared")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Unary operations
    # ----------------------------------------------------
    def test_absolute(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.abs("negative_vals", "absolute_vals")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "absolute_vals")

        expected = sample_df.copy()
        expected["absolute_vals"] = expected["negative_vals"].abs()
        assert_series_equal_loose(res_df[out_col], expected["absolute_vals"])
        self._record_result(
            test_name="absolute",
            method_call='uploaded_ctx.abs("negative_vals", "absolute_vals")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_negate(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.negate("salary", "negative_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "negative_salary")

        expected = sample_df.copy()
        expected["negative_salary"] = -expected["salary"]
        assert_series_equal_loose(res_df[out_col], expected["negative_salary"])
        self._record_result(
            test_name="negate",
            method_call='uploaded_ctx.negate("salary", "negative_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_round(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.round("float_vals", 2, "rounded_vals")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "rounded_vals")

        expected = sample_df.copy()
        expected["rounded_vals"] = expected["float_vals"].round(2)
        assert_series_equal_loose(res_df[out_col], expected["rounded_vals"])
        self._record_result(
            test_name="round",
            method_call='uploaded_ctx.round("float_vals", 2, "rounded_vals")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_ceil(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.ceil("score", "ceil_score")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "ceil_score")

        expected = sample_df.copy()
        expected["ceil_score"] = np.ceil(expected["score"])
        assert_series_equal_loose(res_df[out_col], expected["ceil_score"])
        self._record_result(
            test_name="ceil",
            method_call='uploaded_ctx.ceil("score", "ceil_score")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_floor(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.floor("score", "floor_score")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "floor_score")

        expected = sample_df.copy()
        expected["floor_score"] = np.floor(expected["score"])
        assert_series_equal_loose(res_df[out_col], expected["floor_score"])
        self._record_result(
            test_name="floor",
            method_call='uploaded_ctx.floor("score", "floor_score")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_truncate(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.truncate("float_vals", 2, "truncated_vals")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "truncated_vals")

        expected = sample_df.copy()
        expected["truncated_vals"] = expected["float_vals"].apply(
            lambda x: math.trunc(x * 100) / 100
        )
        assert_series_equal_loose(res_df[out_col], expected["truncated_vals"])
        self._record_result(
            test_name="truncate",
            method_call='uploaded_ctx.truncate("float_vals", 2, "truncated_vals")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    
    # ----------------------------------------------------
    # Exponential and logarithmic
    # ----------------------------------------------------
    def test_exp(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.exp("tax", "exp_tax")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "exp_tax")

        expected = sample_df.copy()
        expected["exp_tax"] = np.exp(expected["tax"])
        assert_series_equal_loose(res_df[out_col], expected["exp_tax"])
        self._record_result(
            test_name="exp",
            method_call='uploaded_ctx.exp("tax", "exp_tax")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_log(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.log("salary", "log_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "log_salary")

        expected = sample_df.copy()
        expected["log_salary"] = np.log(expected["salary"])
        assert_series_equal_loose(res_df[out_col], expected["log_salary"])
        self._record_result(
            test_name="log",
            method_call='uploaded_ctx.log("salary", "log_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_log10(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.log10("salary", "log10_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "log10_salary")

        expected = sample_df.copy()
        expected["log10_salary"] = np.log10(expected["salary"])
        assert_series_equal_loose(res_df[out_col], expected["log10_salary"])
        self._record_result(
            test_name="log10",
            method_call='uploaded_ctx.log10("salary", "log10_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_sqrt(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.sqrt("salary", "sqrt_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "sqrt_salary")

        expected = sample_df.copy()
        expected["sqrt_salary"] = np.sqrt(expected["salary"])
        assert_series_equal_loose(res_df[out_col], expected["sqrt_salary"])
        self._record_result(
            test_name="sqrt",
            method_call='uploaded_ctx.sqrt("salary", "sqrt_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Trigonometric functions
    # ----------------------------------------------------
    def test_sin(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.sin("angle", "sin_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "sin_angle")

        expected = sample_df.copy()
        expected["sin_angle"] = np.sin(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["sin_angle"])
        self._record_result(
            test_name="sin",
            method_call='uploaded_ctx.sin("angle", "sin_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_cos(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cos("angle", "cos_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cos_angle")

        expected = sample_df.copy()
        expected["cos_angle"] = np.cos(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["cos_angle"])
        self._record_result(
            test_name="cos",
            method_call='uploaded_ctx.cos("angle", "cos_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_tan(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.tan("angle", "tan_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "tan_angle")

        expected = sample_df.copy()
        expected["tan_angle"] = np.tan(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["tan_angle"])
        self._record_result(
            test_name="tan",
            method_call='uploaded_ctx.tan("angle", "tan_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_asin(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.asin("angle", "asin_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "asin_angle")

        expected = sample_df.copy()
        expected["asin_angle"] = np.arcsin(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["asin_angle"])
        self._record_result(
            test_name="asin",
            method_call='uploaded_ctx.asin("angle", "asin_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_acos(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.acos("angle", "acos_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "acos_angle")

        expected = sample_df.copy()
        expected["acos_angle"] = np.arccos(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["acos_angle"])
        self._record_result(
            test_name="acos",
            method_call='uploaded_ctx.acos("angle", "acos_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_atan(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.atan("angle", "atan_angle")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "atan_angle")

        expected = sample_df.copy()
        expected["atan_angle"] = np.arctan(expected["angle"])
        assert_series_equal_loose(res_df[out_col], expected["atan_angle"])
        self._record_result(
            test_name="atan",
            method_call='uploaded_ctx.atan("angle", "atan_angle")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_atan2(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.atan2("salary", "bonus", "atan2_result")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "atan2_result")

        expected = sample_df.copy()
        expected["atan2_result"] = np.arctan2(expected["salary"], expected["bonus"])
        assert_series_equal_loose(res_df[out_col], expected["atan2_result"])
        self._record_result(
            test_name="atan2",
            method_call='uploaded_ctx.atan2("salary", "bonus", "atan2_result")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Complex operations
    # ----------------------------------------------------
    def test_weighted_average(self, uploaded_ctx, sample_df, backend_config):
        # Core method is weighted_average; wrapper may expose weighted_sum alias.
        result = uploaded_ctx.weighted_sum("math", "science", 0.7, 0.3, "final_score")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "final_score")

        expected = sample_df.copy()
        expected["final_score"] = (expected["math"] * 0.7) + (expected["science"] * 0.3)
        assert_series_equal_loose(res_df[out_col], expected["final_score"])
        self._record_result(
            test_name="weighted_average",
            method_call='uploaded_ctx.weighted_sum("math", "science", 0.7, 0.3, "final_score")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_percentage_change(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.percentage_change("old_price", "new_price", "pct_change")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "pct_change")

        expected = sample_df.copy()
        expected["pct_change"] = (
            (expected["new_price"] - expected["old_price"]) / expected["old_price"] * 100
        )
        assert_series_equal_loose(res_df[out_col], expected["pct_change"])
        self._record_result(
            test_name="percentage_change",
            method_call='uploaded_ctx.percentage_change("old_price", "new_price", "pct_change")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_normalize_range(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.normalize_range("salary", "normalized_salary")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "normalized_salary")

        expected = sample_df.copy()
        s = expected["salary"]
        expected["normalized_salary"] = (s - s.min()) / (s.max() - s.min())
        assert_series_equal_loose(res_df[out_col], expected["normalized_salary"])
        self._record_result(
            test_name="normalize_range",
            method_call='uploaded_ctx.normalize_range("salary", "normalized_salary")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Mutation safety and chaining
    # ----------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        """Ensure original DataFrame is not modified by arithmetic operations."""
        result = uploaded_ctx.add("salary", "bonus", "total_income")
        original_df = get_result_df(uploaded_ctx)

        # Original must not contain the new column
        assert "total_income" not in original_df.columns

        # Original values unchanged
        pd.testing.assert_frame_equal(
            normalize_frame(original_df),
            normalize_frame(sample_df),
            check_dtype=False,
        )
        self._record_result(
            test_name="mutation_safety",
            method_call='uploaded_ctx.add("salary", "bonus", "total_income") → original checked',
            original_df=sample_df,
            memframe_df=original_df,
            pandas_df=sample_df,
            backend=backend_config["connection_type"],
        )
