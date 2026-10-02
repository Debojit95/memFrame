# tests/integration/ops/test_filter.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from memframe.core.analytix.filter.filter_I import F
from memframe.main import MemFrame
from memframe.utils.str_filter_parser import ParseError
from memframe.wrappers.analytix.filter import FilteringWrapper

# ----------------------------------------------------------------------
# Backend configuration – set environment variables for PostgreSQL
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
    return pytest.UsageError(f"Invalid filtering DB configuration: {message}")


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
    allowed = {"backend", "host", "port", "user", "password", "database"}
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
    allowed = {"backend", "host", "port", "user", "password", "database", "secure", "timeout"}
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
    """DataFrame with mixed types for filtering tests."""
    return pd.DataFrame({
        "A": pd.to_datetime([
            "2023-04-01 14:52:29",
            "2023-04-01 14:50:00",
            "2022-01-01 00:00:00",
            "2024-09-01 20:23:43",
            "2024-09-01 20:24:00"
        ]),
        "B": [10, 20, 15, 25, 5],
        "C": [1.5, 3.0, 2.0, 4.5, 5.5],
        "D": ["alpha", "beta", "gamma", "zoom", "zoom"],
        "E": [2.5, 2.0, 3.0, 2.0, 2.6],
        "F": ["cat", "dog", "cat", "dog", "bird"],
        "H": pd.to_datetime([
            "2023-12-01 10:00:00",
            "2024-11-30 12:00:00",
            "2024-12-05 08:00:00",
            "2024-01-01 00:00:00",
            "2023-05-15 18:00:00"
        ])
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
    ctx = connected_memframe.upload_df(sample_df, filename="filtering_dataset")
    return ctx


@pytest.fixture(scope="function")
def uploaded_ctx_deep(backend_config) -> Any:
    """Upload the sample DataFrame on a deep-cache MemFrame (chunk streaming)."""
    mf = MemFrame(
        connection_type=backend_config["connection_type"],
        connection_params=backend_config.get("params", {}),
        deep_cache=True,
    )
    asyncio.run(mf.aconnect())
    try:
        yield mf.upload_df(
            pd.DataFrame({"B": [10, 20, 15, 25, 5]}),
            filename="filtering_dataset_deep",
        )
    finally:
        asyncio.run(mf.aclose())

# ----------------------------------------------------------------------
# Helpers (reused from arithmetic pattern)
# ----------------------------------------------------------------------
def get_result_df(result: Any) -> pd.DataFrame:
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

def assert_frame_equal_loose(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    """Compare DataFrames, ignoring column/index ordering and dtype differences."""
    # Align columns (keep only those present in expected; actual may have extra system columns)
    common_cols = [c for c in expected.columns if c in actual.columns]
    actual = actual[common_cols].copy()
    expected = expected[common_cols].copy()
    # Reset index and sort columns for stable comparison
    actual = actual.reset_index(drop=True).sort_index(axis=1)
    expected = expected.reset_index(drop=True).sort_index(axis=1)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, check_names=False)

def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)

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
    pandas_call="",
):
    sections = [
        ("Original", original_df.head(10)),
        ("MemFrame Result", memframe_df.head(10)),
        ("Pandas Result", pandas_df.head(10)),
    ]
    fig_height = max(8, 2 + sum(max(2, len(df) + 2) for _, df in sections) * 0.4)
    fig, axes = plt.subplots(3, 1, figsize=(16, fig_height))
    fig.suptitle(f"{title}  [{backend}]  {status}", fontsize=12, fontweight="bold")
    fig.text(0.01, 0.965, f"Call: {method_call}", fontsize=10, family="monospace")
    if pandas_call:
        fig.text(0.01, 0.94, f"Pandas: {pandas_call}", fontsize=10, family="monospace")
    if error_message:
        fig.text(0.01, 0.915, f"Failure: {error_message}", fontsize=9, color="crimson")

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

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    pdf.savefig(fig)
    plt.close(fig)

# ----------------------------------------------------------------------
# Test class
# ----------------------------------------------------------------------
class TestFilteringOperations:
    _save_to_file = False
    _saved_results = []

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, request, save_to_file):
        cls = request.cls
        cls._save_to_file = save_to_file
        cls._saved_results = []
        yield
        if cls._save_to_file and cls._saved_results:
            RESULT_DIR.mkdir(parents=True, exist_ok=True)
            pdf_path = RESULT_DIR / f"test_filtering_report_{request.node.name}.pdf"
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
                        result.get("pandas_call", ""),
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
            frame_locals.get("sample_df"),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "final_df", "after_op", "original_uploaded", "df1", "result", "step1", "step2"):
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
        pandas_call: str = "",
    ):
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
                "pandas_call": pandas_call,
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ------------------------------------------------------------------
    # Basic numeric comparisons
    # ------------------------------------------------------------------
    def test_numeric_gt(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("B > 15")
        res_df = get_result_df(result)
        expected = sample_df.query("B > 15")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="numeric_gt",
            method_call='uploaded_ctx.filter("B > 15")',
            pandas_call='sample_df.query("B > 15")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_numeric_lte(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("C <= 2.0")
        res_df = get_result_df(result)
        expected = sample_df.query("C <= 2.0")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="numeric_lte",
            method_call='uploaded_ctx.filter("C <= 2.0")',
            pandas_call='sample_df.query("C <= 2.0")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_numeric_eq(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("E == 2.0")
        res_df = get_result_df(result)
        expected = sample_df.query("E == 2.0")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="numeric_eq",
            method_call='uploaded_ctx.filter("E == 2.0")',
            pandas_call='sample_df.query("E == 2.0")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_numeric_neq(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("B != 15")
        res_df = get_result_df(result)
        expected = sample_df.query("B != 15")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="numeric_neq",
            method_call='uploaded_ctx.filter("B != 15")',
            pandas_call='sample_df.query("B != 15")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # String comparisons
    # ------------------------------------------------------------------
    def test_string_eq(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("D == 'zoom'")
        res_df = get_result_df(result)
        expected = sample_df.query("D == 'zoom'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="string_eq",
            method_call="uploaded_ctx.filter(\"D == 'zoom'\")",
            pandas_call="sample_df.query(\"D == 'zoom'\")",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_string_neq(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("D != 'beta'")
        res_df = get_result_df(result)
        expected = sample_df.query("D != 'beta'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="string_neq",
            method_call="uploaded_ctx.filter(\"D != 'beta'\")",
            pandas_call="sample_df.query(\"D != 'beta'\")",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Datetime comparisons
    # ------------------------------------------------------------------
    def test_datetime_gt(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("A > '2023-01-01'")
        res_df = get_result_df(result)
        expected = sample_df.query("A > '2023-01-01'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="datetime_gt",
            method_call="uploaded_ctx.filter(\"A > '2023-01-01'\")",
            pandas_call="sample_df.query(\"A > '2023-01-01'\")",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_datetime_between(self, uploaded_ctx, sample_df, backend_config):
        # Using compound expression; parser should handle &&
        result = uploaded_ctx.filter("A >= '2023-04-01' && A <= '2024-09-01'")
        res_df = get_result_df(result)
        expected = sample_df.query("A >= '2023-04-01' and A <= '2024-09-01'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="datetime_between",
            method_call="uploaded_ctx.filter(\"A >= '2023-04-01' && A <= '2024-09-01'\")",
            pandas_call="sample_df.query(\"A >= '2023-04-01' and A <= '2024-09-01'\")",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # dt accessor extractions
    # ------------------------------------------------------------------
    def test_dt_minute(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("A.dt.minute >= 50")
        res_df = get_result_df(result)
        expected = sample_df.query("A.dt.minute >= 50")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="dt_minute",
            method_call='uploaded_ctx.filter("A.dt.minute >= 50")',
            pandas_call='sample_df.query("A.dt.minute >= 50")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_dt_year(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("H.dt.year == 2024")
        res_df = get_result_df(result)
        expected = sample_df.query("H.dt.year == 2024")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="dt_year",
            method_call='uploaded_ctx.filter("H.dt.year == 2024")',
            pandas_call='sample_df.query("H.dt.year == 2024")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Complex logical expressions
    # ------------------------------------------------------------------
    def test_complex_and_or(self, uploaded_ctx, sample_df, backend_config):
        expr = "(B > 10 || C < 2) && D != 'beta'"
        result = uploaded_ctx.filter(expr)
        res_df = get_result_df(result)
        expected = sample_df.query("(B > 10 or C < 2) and D != 'beta'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="complex_and_or",
            method_call=f'uploaded_ctx.filter("{expr}")',
            pandas_call='sample_df.query("(B > 10 or C < 2) and D != \'beta\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_complex_mixed_types(self, uploaded_ctx, sample_df, backend_config):
        expr = "(E > 2.571 || A > '2024-09-01 20:23:43') && (D == 'zoom' || H < '2024-12-04')"
        result = uploaded_ctx.filter(expr)
        res_df = get_result_df(result)
        # pandas query equivalent
        expected = sample_df.query(
            "(E > 2.571 or A > '2024-09-01 20:23:43') and (D == 'zoom' or H < '2024-12-04')"
        )
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="complex_mixed_types",
            method_call=f'uploaded_ctx.filter("{expr}")',
            pandas_call='sample_df.query("(E > 2.571 or A > \'2024-09-01 20:23:43\') and (D == \'zoom\' or H < \'2024-12-04\')")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Column selection
    # ------------------------------------------------------------------
    def test_column_selection(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter("B > 10", columns=["A", "B", "D"])
        res_df = get_result_df(result)
        expected = sample_df.query("B > 10")[["A", "B", "D"]]
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="column_selection",
            method_call='uploaded_ctx.filter("B > 10", columns=["A", "B", "D"])',
            pandas_call='sample_df.query("B > 10")[["A", "B", "D"]]',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Mutation safety and chaining
    # ------------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        original_uploaded = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        uploaded_ctx.filter("B > 10")
        after_op = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        pd.testing.assert_frame_equal(
            normalize_frame(after_op),
            normalize_frame(original_uploaded),
            check_dtype=False,
        )
        self._record_result(
            test_name="mutation_safety",
            method_call='uploaded_ctx.filter("B > 10") → original checked',
            pandas_call='sample_df  # unchanged by filter',
            original_df=sample_df,
            memframe_df=after_op,
            pandas_df=original_uploaded,
            backend=backend_config["connection_type"],
        )

    def test_chaining(self, uploaded_ctx, sample_df, backend_config):
        # Step 1: filter
        step1 = uploaded_ctx.filter("B > 10")
        df1 = get_result_df(step1)
        ctx2 = uploaded_ctx.memframe.upload_df(df1, "chain_step2")

        # Step 2: arithmetic add column using original columns (B and C)
        step2 = ctx2.add("B", 5, "B_plus_5")
        final_df = get_result_df(step2)

        expected = sample_df.query("B > 10").copy()
        expected["B_plus_5"] = expected["B"] + 5
        assert_frame_equal_loose(final_df, expected)
        self._record_result(
            test_name="chaining",
            method_call="filter('B > 10') → add('B', 5, 'B_plus_5') chain",
            pandas_call='sample_df.query("B > 10").assign(B_plus_5=lambda d: d["B"] + 5)',
            original_df=sample_df,
            memframe_df=final_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Predicate objects (F.num / F.cat / F.time)
    # ------------------------------------------------------------------
    def test_predicate_numeric(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.num.gte("B", 15) & F.num.lt("B", 25))
        res_df = get_result_df(result)
        expected = sample_df.query("B >= 15 and B < 25")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="predicate_numeric",
            method_call='filter(F.num.gte("B", 15) & F.num.lt("B", 25))',
            pandas_call='sample_df.query("B >= 15 and B < 25")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_numeric_between_outside(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.num.between("B", 10, 20))
        res_df = get_result_df(result)
        expected = sample_df.query("B >= 10 and B <= 20")
        assert_frame_equal_loose(res_df, expected)

        result = uploaded_ctx.filter(F.num.outside("B", 10, 20))
        res_df_out = get_result_df(result)
        expected_out = sample_df.query("B < 10 or B > 20")
        assert_frame_equal_loose(res_df_out, expected_out)
        self._record_result(
            test_name="predicate_numeric_between_outside",
            method_call='filter(F.num.between("B", 10, 20)) / outside',
            pandas_call='sample_df.query("B >= 10 and B <= 20")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_categorical(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.cat.eq("D", "zoom"))
        res_df = get_result_df(result)
        expected = sample_df.query("D == 'zoom'")
        assert_frame_equal_loose(res_df, expected)

        result = uploaded_ctx.filter(F.cat.in_("F", ["cat", "bird"]))
        res_df_in = get_result_df(result)
        expected_in = sample_df.query("F in ['cat', 'bird']")
        assert_frame_equal_loose(res_df_in, expected_in)
        self._record_result(
            test_name="predicate_categorical",
            method_call='filter(F.cat.eq("D", "zoom")) / in_',
            pandas_call='sample_df.query("D == \'zoom\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_categorical_ilike(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.cat.contains("D", "OOM", case_sensitive=False))
        res_df = get_result_df(result)
        expected = sample_df[sample_df["D"].str.lower().str.contains("oom")]
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="predicate_categorical_ilike",
            method_call='filter(F.cat.contains("D", "OOM", case_sensitive=False))',
            pandas_call='sample_df[sample_df["D"].str.lower().str.contains("oom")]',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_categorical_regex(self, uploaded_ctx, sample_df, backend_config):
        if backend_config["backend"] == "duckdb":
            pytest.skip("DuckDB has no ~ regex operator")
        result = uploaded_ctx.filter(F.cat.regex("D", "^z"))
        res_df = get_result_df(result)
        expected = sample_df[sample_df["D"].str.match("^z")]
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="predicate_categorical_regex",
            method_call='filter(F.cat.regex("D", "^z"))',
            pandas_call='sample_df[sample_df["D"].str.match("^z")]',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_null_checks(self, connected_memframe, backend_config):
        df = pd.DataFrame({"g": ["x", None, "y"], "v": [1, 2, 3]})
        ctx = connected_memframe.upload_df(df, filename="filtering_nulls")
        result = ctx.filter(F.cat.is_null("g"))
        res_df = get_result_df(result)
        assert res_df["v"].tolist() == [2]

        result = ctx.filter(F.cat.not_null("g"))
        res_df = get_result_df(result)
        assert sorted(res_df["v"].tolist()) == [1, 3]
        self._record_result(
            test_name="predicate_null_checks",
            method_call='filter(F.cat.is_null("g")) / not_null',
            pandas_call='df[df["g"].notna()]  # and df[df["g"].isna()]',
            original_df=df,
            memframe_df=res_df,
            pandas_df=df.query("g == g"),
            backend=backend_config["connection_type"],
        )

    def test_predicate_datetime(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.time.after("A", "2024-01-01"))
        res_df = get_result_df(result)
        expected = sample_df.query("A > '2024-01-01'")
        assert_frame_equal_loose(res_df, expected)

        result = uploaded_ctx.filter(
            F.time.between("H", "2024-01-01", "2024-12-01")
        )
        res_df_between = get_result_df(result)
        expected_between = sample_df.query("H >= '2024-01-01' and H <= '2024-12-01'")
        assert_frame_equal_loose(res_df_between, expected_between)
        self._record_result(
            test_name="predicate_datetime",
            method_call='filter(F.time.after("A", "2024-01-01")) / between',
            pandas_call='sample_df.query("A > \'2024-01-01\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_relative_time(self, connected_memframe, backend_config):
        now = pd.Timestamp.today().normalize()
        df = pd.DataFrame(
            {"ts": [now - pd.Timedelta(days=10), now - pd.Timedelta(days=400)]}
        )
        ctx = connected_memframe.upload_df(df, filename="filtering_relative")
        result = ctx.filter(F.time.last_days("ts", 30))
        res_df = get_result_df(result)
        assert len(res_df) == 1
        assert pd.to_datetime(res_df["ts"].iloc[0]).date() == (
            now - pd.Timedelta(days=10)
        ).date()
        self._record_result(
            test_name="predicate_relative_time",
            method_call='filter(F.time.last_days("ts", 30))',
            pandas_call='df[df["ts"] >= pd.Timestamp.today().normalize() - pd.Timedelta(days=30)]',
            original_df=df,
            memframe_df=res_df,
            pandas_df=df.head(1),
            backend=backend_config["connection_type"],
        )

    def test_predicate_timezone_aware(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(F.time.before_tz("A", "2024-01-01 00:00:00", "UTC"))
        res_df = get_result_df(result)
        expected = sample_df.query("A < '2024-01-01'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="predicate_timezone_aware",
            method_call='filter(F.time.before_tz("A", "2024-01-01 00:00:00", "UTC"))',
            pandas_call='sample_df.query("A < \'2024-01-01\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Cross-dtype composition (docs example)
    # ------------------------------------------------------------------
    def test_predicate_cross_dtype(self, uploaded_ctx, sample_df, backend_config):
        pred = (
            F.num.gte("B", 15)
            & F.cat.eq("D", "zoom")
            & F.time.on_or_after("A", "2024-01-01")
        )
        result = uploaded_ctx.filter(pred)
        res_df = get_result_df(result)
        expected = sample_df.query(
            "B >= 15 and D == 'zoom' and A >= '2024-01-01'"
        )
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="predicate_cross_dtype",
            method_call='filter(num & cat & time composed)',
            pandas_call='sample_df.query("B >= 15 and D == \'zoom\' and A >= \'2024-01-01\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_string_cross_dtype(self, uploaded_ctx, sample_df, backend_config):
        expr = "B >= 15 && D == 'zoom' && A >= '2024-01-01'"
        result = uploaded_ctx.filter(expr)
        res_df = get_result_df(result)
        expected = sample_df.query("B >= 15 and D == 'zoom' and A >= '2024-01-01'")
        assert_frame_equal_loose(res_df, expected)
        self._record_result(
            test_name="string_cross_dtype",
            method_call=f'uploaded_ctx.filter("{expr}")',
            pandas_call='sample_df.query("B >= 15 and D == \'zoom\' and A >= \'2024-01-01\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_predicate_not_xor(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.filter(~F.cat.eq("D", "zoom"))
        res_df = get_result_df(result)
        expected = sample_df.query("D != 'zoom'")
        assert_frame_equal_loose(res_df, expected)

        result = uploaded_ctx.filter(
            F.num.gt("B", 20) ^ F.cat.eq("D", "zoom")
        )
        res_df_xor = get_result_df(result)
        mask = (sample_df["B"] > 20) ^ (sample_df["D"] == "zoom")
        expected_xor = sample_df[mask]
        assert_frame_equal_loose(res_df_xor, expected_xor)
        self._record_result(
            test_name="predicate_not_xor",
            method_call='filter(~eq) / filter(gt ^ eq)',
            pandas_call='sample_df.query("D != \'zoom\'")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Chunked streaming (needs deep_cache — L1 drops the table on return)
    # ------------------------------------------------------------------
    def test_chunk_streaming(self, uploaded_ctx_deep, backend_config):
        result = uploaded_ctx_deep.filter("B > 10", chunk_size=2)
        assert not result.get("is_error"), result.get("error_message")
        assert "iterator" in result

        async def _collect(it):
            return [chunk async for chunk in it]

        chunks = asyncio.run(_collect(result["iterator"]))
        combined = pd.concat(chunks, ignore_index=True)
        assert sorted(combined["B"].tolist()) == [15, 20, 25]
        self._record_result(
            test_name="chunk_streaming",
            method_call='filter("B > 10", chunk_size=2)',
            pandas_call='sample_df.query("B > 10")  # streamed in chunks',
            original_df=pd.DataFrame({"B": [10, 20, 15, 25, 5]}),
            memframe_df=combined,
            pandas_df=pd.DataFrame({"B": [15, 20, 25]}),
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Parse errors
    # ------------------------------------------------------------------
    def test_parse_error(self, uploaded_ctx, sample_df, backend_config):
        with pytest.raises(ParseError):
            uploaded_ctx.filter("B ??? 15")

    # ------------------------------------------------------------------
    # create_flag — in-place boolean mask on the source table
    # ------------------------------------------------------------------
    def _source_frame(self, ctx, n):
        return get_result_df(ctx.head(n=n))

    def test_flag_subset(self, uploaded_ctx, sample_df, backend_config):
        result = FilteringWrapper(uploaded_ctx).filter("B > 15", create_flag=True)
        assert not result.get("is_error"), result.get("error_message")
        assert result["flag_column"] == "filter_flag"

        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert "filter_flag" in src.columns
        assert src["filter_flag"].astype(bool).tolist() == [False, True, False, True, False]
        # Filtered result itself is unchanged
        assert sorted(get_result_df(result)["B"].tolist()) == [20, 25]
        self._record_result(
            test_name="flag_subset",
            method_call='filter("B > 15", create_flag=True)',
            pandas_call='sample_df.query("B > 15")  # + filter_flag mask column',
            original_df=sample_df,
            memframe_df=src,
            pandas_df=sample_df,
            backend=backend_config["connection_type"],
        )

    def test_flag_predicate_form(self, uploaded_ctx, sample_df, backend_config):
        result = FilteringWrapper(uploaded_ctx).filter(
            F.cat.eq("D", "zoom"), create_flag=True
        )
        assert result["flag_column"] == "filter_flag"
        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert src["filter_flag"].astype(bool).tolist() == [False, False, False, True, True]

    def test_flag_empty_match_skipped(self, uploaded_ctx, sample_df, backend_config):
        result = FilteringWrapper(uploaded_ctx).filter("B > 1000", create_flag=True)
        assert not result.get("is_error"), result.get("error_message")
        assert result["flag_column"] is None
        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert "filter_flag" not in src.columns

    def test_flag_full_match_skipped(self, uploaded_ctx, sample_df, backend_config):
        result = FilteringWrapper(uploaded_ctx).filter("B > 0", create_flag=True)
        assert result["flag_column"] is None
        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert "filter_flag" not in src.columns

    def test_flag_off_by_default(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.filter("B > 15")
        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert "filter_flag" not in src.columns

    def test_flag_null_reads_false(self, connected_memframe, backend_config):
        df = pd.DataFrame({"g": ["x", None, "y"], "v": [1, 2, 3]})
        ctx = connected_memframe.upload_df(df, filename="filtering_flag_null")
        result = FilteringWrapper(ctx).filter(F.cat.eq("g", "x"), create_flag=True)
        assert result["flag_column"] == "filter_flag"
        src = self._source_frame(ctx, len(df))
        assert src["filter_flag"].astype(bool).tolist() == [True, False, False]

    def test_flag_rededupe_on_refilter(self, uploaded_ctx, sample_df, backend_config):
        FilteringWrapper(uploaded_ctx).filter("B > 15", create_flag=True)
        result = FilteringWrapper(uploaded_ctx).filter("B > 20", create_flag=True)
        assert result["flag_column"] == "filter_flag_1"
        src = self._source_frame(uploaded_ctx, len(sample_df))
        assert "filter_flag" in src.columns
        assert "filter_flag_1" in src.columns
        assert src["filter_flag_1"].astype(bool).tolist() == [False, False, False, True, False]
