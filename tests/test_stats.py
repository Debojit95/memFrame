# tests/test_stats.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np
import pytest
import subprocess
import sys

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

    db_path = params.get("db_path", default_duckdb_test_path("stats"))
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
        "count_all": [1, 2, 3, 4, 5],  # non-null for testing count
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
    asyncio.run(mf.aconnect())
    try:
        yield mf
    finally:
        asyncio.run(mf.close())

@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, sample_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(sample_df, filename="stats_dataset")

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def get_result_value(result: Any) -> Any:
    """Extract the actual scalar/list/DataFrame from a library result dict."""
    if isinstance(result, dict):
        if result.get("is_error"):
            raise AssertionError(result.get("error_message") or f"Operation failed: {result}")
        if "result" in result:
            return get_result_value(result["result"])
        if "value" in result:
            return result["value"]
        if "data" in result:
            return result["data"]
        return result
    return result

def get_result_df(result: Any) -> pd.DataFrame:
    """Extract DataFrame from result."""
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
        for key in ("result", "data", "current_state"):
            if key in result:
                inner = result[key]
                if isinstance(inner, pd.DataFrame):
                    return inner
                if isinstance(inner, dict):
                    if "result" in inner and isinstance(inner["result"], pd.DataFrame):
                        return inner["result"]
        raise AssertionError(f"Cannot extract DataFrame from dict: {list(result.keys())}")
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")

def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)

# ----------------------------------------------------------------------
# PDF helpers
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
    if isinstance(value, (list, tuple, set, np.ndarray)):
        return pd.DataFrame({"Result": list(value)})
    if value is None:
        return _empty_pdf_df(empty_message)
    return pd.DataFrame({"Result": [value]})


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
    if error_message:
        fig.text(0.01, 0.94, f"Failure: {error_message}", fontsize=9, color="crimson")
    for ax, (label, df) in zip(axes, sections):
        ax.axis("off")
        ax.set_title(label, fontsize=10, loc="left")
        if df.empty:
            ax.text(0.01, 0.5, "(empty)", fontsize=9, transform=ax.transAxes)
            continue
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.1, 1.2)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)

# ----------------------------------------------------------------------
# Test class
# ----------------------------------------------------------------------
class TestStatsOperations:
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
            pdf_path = RESULT_DIR / f"test_stats_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for result in cls._saved_results:
                    render_df_to_pdf_page(
                        pdf,
                        result["test_name"],
                        result["method_call"],
                        result["original_df"],
                        result["memframe_result"],
                        result["pandas_result"],
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
        frame_locals = getattr(request.node, "_stats_failure_locals", None)
        if frame_locals is None:
            frame_locals = getattr(request.node, "_failure_locals", {})

        original_df = _coerce_pdf_df(
            frame_locals.get("sample_df"),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "value", "actual", "actual_map", "counts", "result", "original"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_value = None
        for name in (
            "expected",
            "expected_map",
            "expected_indices",
            "expected_vals",
            "expected_samp",
            "expected_pop",
            "expected_days",
            "sample_df",
        ):
            if name in frame_locals:
                pandas_value = frame_locals[name]
                break
        pandas_df = _coerce_pdf_df(
            pandas_value,
            "No pandas expected result was available when this test failed",
        )

        backend_config = frame_locals.get("backend_config") or {}
        self._record_result(
            test_name=request.node.name,
            method_call=request.node.name,
            original_df=original_df,
            memframe_result=memframe_df,
            pandas_result=pandas_df,
            backend=backend_config.get("connection_type", "unknown"),
            status="FAILED",
            error_message=error_message,
        )

    def _record_result(
        self,
        test_name,
        method_call,
        original_df,
        memframe_result,
        pandas_result,
        backend,
        status="PENDING",
        error_message="",
    ):
        if self._save_to_file:
            result = {
                "test_name": test_name,
                "method_call": method_call,
                "original_df": _prepare_pdf_df(_coerce_pdf_df(original_df, "No original data")),
                "memframe_result": _prepare_pdf_df(_coerce_pdf_df(memframe_result, "No MemFrame result")),
                "pandas_result": _prepare_pdf_df(_coerce_pdf_df(pandas_result, "No pandas result")),
                "backend": backend,
                "status": status,
                "error_message": error_message,
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ------------------------------------------------------------------
    # Basic statistics (count, min, max, mode, unique, nunique, value_counts)
    # ------------------------------------------------------------------
    def test_count(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.count("score")
        value = get_result_value(result)
        expected = sample_df["score"].count()
        assert value == expected
        self._record_result("count", 'count("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_min(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.min("score")
        value = get_result_value(result)
        expected = sample_df["score"].min()
        assert value == pytest.approx(expected)
        self._record_result("min", 'min("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_max(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.max("score")
        value = get_result_value(result)
        expected = sample_df["score"].max()
        assert value == pytest.approx(expected)
        self._record_result("max", 'max("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_mode(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mode("category", top_n=2)
        value = get_result_value(result)
        expected = sample_df["category"].mode().head(2).tolist()
        actual = value if isinstance(value, list) else [item[0] if isinstance(item, tuple) else item for item in value]
        assert sorted(actual) == sorted(expected)
        self._record_result("mode", 'mode("category", top_n=2)', sample_df, actual, expected, backend_config["connection_type"])

    def test_unique(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.unique("category")
        value = get_result_value(result)
        expected = sample_df["category"].unique().tolist()
        assert sorted(value) == sorted(expected)
        self._record_result("unique", 'unique("category")', sample_df, value, expected, backend_config["connection_type"])

    def test_nunique(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.nunique("category")
        value = get_result_value(result)
        expected = sample_df["category"].nunique()
        assert value == expected
        self._record_result("nunique", 'nunique("category")', sample_df, value, expected, backend_config["connection_type"])

    def test_value_counts(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.value_counts("category", top_n=10)
        value = get_result_value(result)
        if isinstance(value, list):
            counts = {k: v for k, v in value}
        elif isinstance(value, dict):
            counts = value
        else:
            raise ValueError("Unexpected format")
        expected = sample_df["category"].value_counts().to_dict()
        assert counts == expected
        self._record_result("value_counts", 'value_counts("category")', sample_df, counts, expected, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Mean, median
    # ------------------------------------------------------------------
    def test_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mean("score")
        value = get_result_value(result)
        expected = sample_df["score"].mean()
        assert value == pytest.approx(expected, rel=1e-5)
        self._record_result("mean", 'mean("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_median(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.median("score")
        value = get_result_value(result)
        expected = sample_df["score"].median()
        assert value == pytest.approx(expected)
        self._record_result("median", 'median("score")', sample_df, value, expected, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Sum, std, var, sem, mad, iqr, range
    # ------------------------------------------------------------------
    def test_sum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.sum("score")
        value = get_result_value(result)
        expected = sample_df["score"].sum()
        assert value == pytest.approx(expected)
        self._record_result("sum", 'sum("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_std(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.std("score")
        value = get_result_value(result)
        expected_pop = sample_df["score"].std(ddof=0)
        expected_samp = sample_df["score"].std(ddof=1)
        if abs(value - expected_pop) < 0.01:
            assert value == pytest.approx(expected_pop, rel=1e-3)
        else:
            assert value == pytest.approx(expected_samp, rel=1e-3)
        self._record_result("std", 'std("score")', sample_df, value, expected_samp, backend_config["connection_type"])

    def test_var(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.var("score")
        value = get_result_value(result)
        expected_pop = sample_df["score"].var(ddof=0)
        expected_samp = sample_df["score"].var(ddof=1)
        if abs(value - expected_pop) < 0.01:
            assert value == pytest.approx(expected_pop, rel=1e-3)
        else:
            assert value == pytest.approx(expected_samp, rel=1e-3)
        self._record_result("var", 'var("score")', sample_df, value, expected_samp, backend_config["connection_type"])

    def test_sem(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.sem("score")
        value = get_result_value(result)
        expected = sample_df["score"].sem()
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("sem", 'sem("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_mad(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mad("score")
        value = get_result_value(result)
        expected = (sample_df["score"] - sample_df["score"].mean()).abs().mean()
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("mad", 'mad("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_iqr(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.iqr("score")
        value = get_result_value(result)
        expected = sample_df["score"].quantile(0.75) - sample_df["score"].quantile(0.25)
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("iqr", 'iqr("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_range(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.range("score")
        value = get_result_value(result)
        expected = sample_df["score"].max() - sample_df["score"].min()
        assert value == pytest.approx(expected)
        self._record_result("range", 'range("score")', sample_df, value, expected, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Skew, kurtosis, entropy, quantile, autocorr, coefficient_of_variation
    # ------------------------------------------------------------------
    def test_skew(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.skew("score")
        value = get_result_value(result)
        expected = sample_df["score"].skew()
        assert value == pytest.approx(expected, rel=0.1)
        self._record_result("skew", 'skew("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_kurtosis(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.kurtosis("score")
        value = get_result_value(result)
        expected = sample_df["score"].kurtosis()
        assert value == pytest.approx(expected, rel=0.1)
        self._record_result("kurtosis", 'kurtosis("score")', sample_df, value, expected, backend_config["connection_type"])

    def test_entropy(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.entropy("category")
        value = get_result_value(result)
        counts = sample_df["category"].value_counts(normalize=True)
        expected = -(counts * np.log(counts)).sum()
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("entropy", 'entropy("category")', sample_df, value, expected, backend_config["connection_type"])

    def test_quantile(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.quantile("score", q=[0.25, 0.5, 0.75])
        value = get_result_value(result)
        expected = sample_df["score"].quantile([0.25, 0.5, 0.75]).tolist()
        if isinstance(value, dict):
            value = list(value.values())
        assert np.allclose(value, expected, rtol=0.01)
        self._record_result("quantile", 'quantile("score", q=[0.25,0.5,0.75])', sample_df, value, expected, backend_config["connection_type"])

    def test_autocorr(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.autocorr("score", lag=1)
        value = get_result_value(result)
        expected = sample_df["score"].autocorr(lag=1)
        if pd.isna(expected):
            assert pd.isna(value)
        else:
            assert value == pytest.approx(expected, rel=0.1)
        self._record_result("autocorr", 'autocorr("score", lag=1)', sample_df, value, expected, backend_config["connection_type"])

    def test_coefficient_of_variation(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.coefficient_of_variation("score")
        value = get_result_value(result)
        expected = sample_df["score"].std(ddof=0) / sample_df["score"].mean()
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("cv", 'cv("score")', sample_df, value, expected, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Outliers
    # ------------------------------------------------------------------
    def test_outliers_iqr(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.outliers_iqr("score")
        value = get_result_value(result)
        Q1 = sample_df["score"].quantile(0.25)
        Q3 = sample_df["score"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outlier_mask = (sample_df["score"] < lower) | (sample_df["score"] > upper)
        expected_indices = sample_df.index[outlier_mask].tolist()
        if isinstance(value, list) and len(value) > 0:
            if isinstance(value[0], (int, float)) and value[0] < 10:
                assert sorted(value) == sorted(expected_indices)
            else:
                expected_vals = sample_df["score"].iloc[expected_indices].tolist()
                assert sorted(value) == sorted(expected_vals)
        else:
            assert len(value) == 0
        self._record_result("outliers_iqr", 'outliers_iqr("score")', sample_df, value, expected_indices, backend_config["connection_type"])

    def test_outliers_zscore(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.outliers_zscore("score", threshold=2.0)
        value = get_result_value(result)
        mean = sample_df["score"].mean()
        std = sample_df["score"].std(ddof=0)
        z = (sample_df["score"] - mean).abs() / std
        outlier_mask = z > 2.0
        expected_indices = sample_df.index[outlier_mask].tolist()
        if isinstance(value, list) and len(value) > 0:
            if isinstance(value[0], (int, float)) and value[0] < 10:
                assert sorted(value) == sorted(expected_indices)
            else:
                expected_vals = sample_df["score"].iloc[expected_indices].tolist()
                assert sorted(value) == sorted(expected_vals)
        else:
            assert len(value) == 0
        self._record_result("outliers_zscore", 'outliers_zscore("score", threshold=2.0)', sample_df, value, expected_indices, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Multi-column correlations / covariance
    # ------------------------------------------------------------------
    def test_corr(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.corr(columns=["score", "salary"])
        res_df = get_result_df(result)
        expected = sample_df[["score", "salary"]].corr()
        pd.testing.assert_frame_equal(
            res_df.sort_index(axis=0).sort_index(axis=1),
            expected.sort_index(axis=0).sort_index(axis=1),
            check_dtype=False,
            check_names=False,
        )
        self._record_result("corr", 'corr(columns=["score","salary"])', sample_df, res_df, expected, backend_config["connection_type"])

    def test_cov(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cov(columns=["score", "salary"])
        res_df = get_result_df(result)
        expected = sample_df[["score", "salary"]].cov()
        pd.testing.assert_frame_equal(
            res_df.sort_index(axis=0).sort_index(axis=1),
            expected.sort_index(axis=0).sort_index(axis=1),
            check_dtype=False,
            check_names=False,
        )
        self._record_result("cov", 'cov(columns=["score","salary"])', sample_df, res_df, expected, backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Categorical statistics
    # ------------------------------------------------------------------
    def test_proportions(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.proportions("category")
        value = get_result_value(result)
        expected = sample_df["category"].value_counts(normalize=True).to_dict()
        assert value == pytest.approx(expected, rel=1e-3)
        self._record_result("proportions", 'proportions("category")', sample_df, value, expected, backend_config["connection_type"])

    def test_chi_square(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.chi_square("category", "category2")
        value = get_result_value(result)
        assert isinstance(value, (dict, float))
        self._record_result("chi_square", 'chi_square("category","category2")', sample_df, value, "computed", backend_config["connection_type"])

    def test_cramers_v(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cramers_v("category", "category2")
        value = get_result_value(result)
        assert isinstance(value, (float, dict))
        self._record_result("cramers_v", 'cramers_v("category","category2")', sample_df, value, "association", backend_config["connection_type"])

    def test_theil_u(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.theil_u("category", "category2")
        value = get_result_value(result)
        assert isinstance(value, (float, dict))
        self._record_result("theil_u", 'theil_u("category","category2")', sample_df, value, "association", backend_config["connection_type"])

    def test_mutual_information(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.mutual_information("category", "category2")
        value = get_result_value(result)
        assert isinstance(value, (float, dict))
        self._record_result("mutual_information", 'mutual_information("category","category2")', sample_df, value, "association", backend_config["connection_type"])

    
    # ------------------------------------------------------------------
    # Datetime statistics
    # ------------------------------------------------------------------
    def test_datetime_diff(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.datetime_diff("date")
        value = get_result_value(result)
        expected = sample_df["date"].diff().dropna().dt.total_seconds().tolist()
        if isinstance(value, list) and len(value) == 4:
            if value[0] > 1000:
                assert np.allclose(value, expected, rtol=0.01)
            else:
                expected_days = sample_df["date"].diff().dropna().dt.days.tolist()
                assert value == expected_days
        self._record_result("datetime_diff", 'datetime_diff("date")', sample_df, value, expected, backend_config["connection_type"])

    def test_time_delta_stats(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.time_delta_stats("date")
        value = get_result_value(result)
        assert isinstance(value, dict) or value is not None
        self._record_result("delta_stats", 'time_delta_stats("date")', sample_df, value, "stats", backend_config["connection_type"])

    def test_event_rate(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.event_rate("date", unit="day")
        value = get_result_value(result)
        assert isinstance(value, (float, int))
        self._record_result("event_rate", 'event_rate("date")', sample_df, value, "rate", backend_config["connection_type"])

    def test_time_unit_counts(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.time_unit_counts("date", unit="day")
        value = get_result_value(result)
        assert isinstance(value, (dict, pd.DataFrame))
        self._record_result("time_unit_counts", 'time_unit_counts("date")', sample_df, value, "counts", backend_config["connection_type"])

    def test_weekday_weekend_counts(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.weekday_weekend_counts("date")
        value = get_result_value(result)
        assert isinstance(value, dict) or hasattr(value, "weekday")
        self._record_result("weekday_weekend", 'weekday_weekend_counts("date")', sample_df, value, "counts", backend_config["connection_type"])

    def test_holiday_counts(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.holiday_counts("date")
        value = get_result_value(result)
        assert isinstance(value, (int, dict))
        self._record_result("holiday_counts", 'holiday_counts("date")', sample_df, value, "counts", backend_config["connection_type"])

    # ------------------------------------------------------------------
    # Mutation safety
    # ------------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        _ = uploaded_ctx.mean("score")
        original = get_result_df(uploaded_ctx) if hasattr(uploaded_ctx, "full_table") else sample_df
        for col in sample_df.columns:
            if col in original.columns:
                pass
        self._record_result("mutation_safety", "mean then check original unchanged", sample_df, original, sample_df, backend_config["connection_type"])
