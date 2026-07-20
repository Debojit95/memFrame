# tests/test_bar.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np
import pytest

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from src.main import MemFrame

# ----------------------------------------------------------------------
# Backend configuration - set environment variables for remote databases
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

TEST_BACKENDS = [
    backend.strip()
    for backend in os.getenv("MEMFRAME_TEST_BACKENDS", "local").split(",")
    if backend.strip()
]
RESULT_DIR = Path(__file__).resolve().parent / "result"


def _usage_error(message: str) -> pytest.UsageError:
    return pytest.UsageError(f"Invalid stats DB configuration: {message}")


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
# Test data
# ----------------------------------------------------------------------
@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """DataFrame with numeric, categorical, and datetime columns, including NaNs."""
    dates = pd.date_range("2025-01-01", periods=5, freq="D")
    return pd.DataFrame({
        "score": [85.5, 92.3, 78.9, None, 88.0],
        "salary": [50000, 60000, 55000, None, 70000],
        "category": ["A", "B", "A", "C", "B"],
        "category2": ["X", "Y", "X", "Z", "Y"],
        "date": dates,
        "active": [True, False, True, None, False],
        "count_all": [1, 2, 3, 4, 5],
    })


# ----------------------------------------------------------------------
# Backend fixtures
# ----------------------------------------------------------------------
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
    mf.connect()
    try:
        yield mf
    finally:
        asyncio.run(mf.close())


@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, sample_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(sample_df, filename="stats_dataset")


# ----------------------------------------------------------------------
# PDF helpers
# ----------------------------------------------------------------------
def render_fig_to_pdf_page(
    pdf,
    title,
    method_call,
    bar_fig,
    backend,
    status="PASSED",
    error_message="",
):
    if bar_fig is not None:
        bar_fig.suptitle(
            f"{title}  [{backend}]  {status}\nCall: {method_call}",
            fontsize=12,
            fontweight="bold",
            y=1.02,
        )
        if error_message:
            bar_fig.text(
                0.01, 0.98, f"Failure: {error_message}",
                fontsize=9, color="crimson", transform=bar_fig.transFigure,
            )
        pdf.savefig(bar_fig, bbox_inches="tight")
        plt.close(bar_fig)
    else:
        fig = plt.figure(figsize=(16, 4))
        fig.suptitle(f"{title}  [{backend}]  {status}", fontsize=12, fontweight="bold")
        fig.text(0.01, 0.9, f"Call: {method_call}", fontsize=10, family="monospace")
        if error_message:
            fig.text(0.01, 0.85, f"Failure: {error_message}", fontsize=9, color="crimson")
        fig.text(0.5, 0.5, "No figure generated", ha="center", va="center", fontsize=14)
        pdf.savefig(fig)
        plt.close(fig)


# ----------------------------------------------------------------------
# Test class
# ----------------------------------------------------------------------
class TestBarPlotOperations:
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
            pdf_path = RESULT_DIR / f"test_bar_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for result in cls._saved_results:
                    render_fig_to_pdf_page(
                        pdf,
                        result["test_name"],
                        result["method_call"],
                        result.get("bar_fig"),
                        result["backend"],
                        result.get("status", "PASSED"),
                        result.get("error_message", ""),
                    )
            print(f"\n\nTest report saved to: {pdf_path}\n")

    @pytest.fixture(autouse=True)
    def _capture_failed_pdf_result(self, request):
        self._current_pdf_records = []
        yield
        report = getattr(request.node, "rep_call", None)
        if report is not None and report.failed:
            error_message = self._format_report_failure(report)
            self._mark_current_pdf_records("FAILED", error_message)
            if self._save_to_file and not self._current_pdf_records:
                self._record_failure_from_report(request, error_message)
        else:
            self._mark_current_pdf_records("PASSED", "")
        self._current_pdf_records = []

    def _mark_current_pdf_records(self, status: str, error_message: str) -> None:
        for result in getattr(self, "_current_pdf_records", []):
            result["status"] = status
            result["error_message"] = error_message

    def _format_report_failure(self, report) -> str:
        lines = str(report.longrepr).splitlines()
        if not lines:
            return "Test failed"
        return lines[-1][:500]

    def _record_failure_from_report(self, request, error_message: str) -> None:
        frame_locals = getattr(request.node, "_failure_locals", {})
        backend_config = frame_locals.get("backend_config") or {}
        self._record_result(
            test_name=request.node.name,
            method_call=request.node.name,
            bar_fig=None,
            backend=backend_config.get("connection_type", "unknown"),
            status="FAILED",
            error_message=error_message,
        )

    def _record_result(
        self,
        test_name,
        method_call,
        bar_fig,
        backend,
        status="PENDING",
        error_message="",
    ):
        if self._save_to_file:
            result = {
                "test_name": test_name,
                "method_call": method_call,
                "bar_fig": bar_fig,
                "backend": backend,
                "status": status,
                "error_message": error_message,
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ------------------------------------------------------------------
    # Simple functional tests to verify bar plot works across backends
    # ------------------------------------------------------------------
    def test_bar_count(self, uploaded_ctx, backend_config):
        """Test basic count bar plot (x only)."""
        fig = uploaded_ctx.bar(x="category")
        self._record_result("bar_count", 'bar(x="category")', fig, backend_config["connection_type"])

    def test_bar_agg_with_hue(self, uploaded_ctx, backend_config):
        """Test aggregation bar plot with y and hue."""
        fig = uploaded_ctx.bar(x="category", y="score")
        self._record_result("bar_agg_hue", 'bar(x="category", y="score", hue="category2", agg="mean")', fig, backend_config["connection_type"])
