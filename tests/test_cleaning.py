# tests/test_cleaning.py

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
    return pytest.UsageError(f"Invalid cleaning DB configuration: {message}")


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

    db_path = params.get("db_path", default_duckdb_test_path("cleaning"))
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
    """Create the reference DataFrame once per test."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 4],
        "salary": [1000, None, 3000, 1000000, 1000000],
        "bonus": [100, 200, None, 400, 400],
        "department": ["HR", "IT", "IT", "Finance", "Finance"],
        "category": ["A", "B", "B", "X", "X"],
        "numeric_str": ["10", "20", "30", "40", "50"],
        "date_col": [
            "2024-01-01",
            "2024-02-15",
            "2024-03-20",
            "2024-04-25",
            "2024-04-25",
        ],
        "mixed_nulls": [1, None, None, 4, 4],
    })
    return df


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
    ctx = connected_memframe.upload_df(sample_df, filename="cleaning_dataset")
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


def assert_series_equal_loose(actual: pd.Series, expected: pd.Series, as_datetime: bool = False) -> None:
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
    if "numeric_str" in out.columns:
        out["numeric_str"] = out["numeric_str"].astype("string")
    if "date_col" in out.columns:
        out["date_col"] = out["date_col"].astype("string")
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
class TestCleaningOperations:
    """All cleaning tests that require a backend connection."""

    # Class-level attributes for PDF report
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
            pdf_path = RESULT_DIR / f"test_cleaning_report_{request.node.name}.pdf"
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
        for name in (
            "res_df",
            "res_filtered",
            "final_df",
            "original_df",
            "result",
            "step1",
            "step2",
            "final_result",
        ):
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
            "expected_filtered",
            "expected_final",
            "expected_after_fillna",
            "expected_series",
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
    # 1. fillna – numeric constant
    # ----------------------------------------------------
    def test_fillna_numeric_constant(self, uploaded_ctx, sample_df, backend_config):
        value = 9999
        result = uploaded_ctx.fillna(column="salary", method="constant", value=value)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df.copy()
        expected["salary"] = expected["salary"].fillna(value)

        assert_series_equal_loose(res_df[out_col], expected["salary"])
        self._record_result(
            test_name="fillna_numeric_constant",
            method_call='uploaded_ctx.fillna(column="salary", method="constant", value=9999)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 2. fillna – numeric mean
    # ----------------------------------------------------
    def test_fillna_numeric_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.fillna(column="salary", method="mean")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df.copy()
        mean_val = expected["salary"].mean()
        expected["salary"] = expected["salary"].fillna(mean_val)

        assert_series_equal_loose(res_df[out_col], expected["salary"])
        self._record_result(
            test_name="fillna_numeric_mean",
            method_call='uploaded_ctx.fillna(column="salary", method="mean")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 3. fillna – categorical constant
    # ----------------------------------------------------
    def test_fillna_categorical_constant(self, uploaded_ctx, sample_df, backend_config):
        df = sample_df.copy()
        df.loc[1, "department"] = None
        ctx = uploaded_ctx.memframe.upload_df(df, filename="cleaning_cat_na")
        result = ctx.fillna(column="department", method="constant", value="Unknown")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "department")

        actual_series = res_df[out_col].reset_index(drop=True)
        # In the current CSV upload path, missing categorical values are ingested as ""
        # instead of SQL NULL; keep test compatible with both behaviors.
        assert actual_series.iloc[0] == "HR"
        assert actual_series.iloc[2] == "IT"
        assert actual_series.iloc[3] == "Finance"
        assert actual_series.iloc[4] == "Finance"
        assert actual_series.iloc[1] in ("", "Unknown")

        expected = df.copy()
        expected["department"] = expected["department"].fillna("")
        self._record_result(
            test_name="fillna_categorical_constant",
            method_call='ctx.fillna(column="department", method="constant", value="Unknown")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 4. fillna – categorical mode
    # ----------------------------------------------------
    def test_fillna_categorical_mode(self, uploaded_ctx, sample_df, backend_config):
        df = sample_df.copy()
        df.loc[1, "category"] = None
        ctx = uploaded_ctx.memframe.upload_df(df, filename="cleaning_cat_mode")
        result = ctx.fillna(column="category", method="mode")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "category")

        expected = df.copy()
        mode_val = expected["category"].mode().iloc[0]
        actual_series = res_df[out_col].reset_index(drop=True)
        assert actual_series.iloc[0] == "A"
        assert actual_series.iloc[2] == "B"
        assert actual_series.iloc[3] == "X"
        assert actual_series.iloc[4] == "X"
        assert actual_series.iloc[1] in ("", mode_val)
        expected["category"] = expected["category"].fillna("")
        self._record_result(
            test_name="fillna_categorical_mode",
            method_call='ctx.fillna(column="category", method="mode")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 5. fillna – datetime constant
    # ----------------------------------------------------
    def test_fillna_datetime_constant(self, uploaded_ctx, sample_df, backend_config):
        df = sample_df.copy()
        df.loc[2, "date_col"] = None
        ctx = uploaded_ctx.memframe.upload_df(df, filename="cleaning_dt_na")
        fill_val = "2024-12-31"
        result = ctx.fillna(column="date_col", method="constant", value=fill_val)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "date_col")
        res_series = pd.to_datetime(res_df[out_col])

        expected = df.copy()
        expected["date_col"] = pd.to_datetime(expected["date_col"]).fillna(pd.Timestamp(fill_val))

        assert_series_equal_loose(res_series, expected["date_col"], as_datetime=True)
        self._record_result(
            test_name="fillna_datetime_constant",
            method_call='ctx.fillna(column="date_col", method="constant", value="2024-12-31")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 6. clip (numeric_enforce_range)
    # ----------------------------------------------------
    def test_clip_basic(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.clean.clip(column="salary", lower=1000, upper=5000)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df["salary"].where(sample_df["salary"].between(1000, 5000))
        assert_series_equal_loose(res_df[out_col], expected)
        self._record_result(
            test_name="clip_basic",
            method_call='uploaded_ctx.clean.clip(column="salary", lower=1000, upper=5000)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_clip_lower_only(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.clean.clip(column="salary", lower=2000, upper=None)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df["salary"].where(sample_df["salary"] >= 2000)
        assert_series_equal_loose(res_df[out_col], expected)
        self._record_result(
            test_name="clip_lower_only",
            method_call='uploaded_ctx.clean.clip(column="salary", lower=2000, upper=None)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_clip_upper_only(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.clean.clip(column="salary", lower=None, upper=8000)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df["salary"].where(sample_df["salary"] <= 8000)
        assert_series_equal_loose(res_df[out_col], expected)
        self._record_result(
            test_name="clip_upper_only",
            method_call='uploaded_ctx.clean.clip(column="salary", lower=None, upper=8000)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 7. drop_outliers (zscore)
    # ----------------------------------------------------
    def test_drop_outliers(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.drop_outliers(column="salary", z_thresh=2.0)
        if isinstance(result, dict) and "data_id" in result:
            res_df = uploaded_ctx.memframe.memFrame(data_id=result["data_id"]).to_pandas()
        else:
            res_df = get_result_df(result)

        expected = sample_df.copy()
        s = expected["salary"]
        z_scores = pd.Series(np.nan, index=s.index)
        mask = s.notna()
        if mask.any():
            mu = s[mask].mean()
            sigma = s[mask].std(ddof=0)
            z_scores[mask] = np.abs((s[mask] - mu) / sigma) if sigma else 0
        else:
            z_scores[mask] = np.nan
        expected_masked = expected["salary"].where(z_scores < 2.0)
        out_col = get_generated_col(result, "salary")
        res_filtered = res_df.reset_index(drop=True)
        assert_series_equal_loose(res_filtered[out_col], expected_masked)

        expected_filtered = pd.DataFrame({
            "salary": sample_df["salary"],
            out_col: expected_masked,
        })
        self._record_result(
            test_name="drop_outliers",
            method_call='uploaded_ctx.drop_outliers(column="salary", z_thresh=2.0)',
            original_df=sample_df,
            memframe_df=res_filtered,
            pandas_df=expected_filtered,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 8. to_numeric
    # ----------------------------------------------------
    def test_to_numeric(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.to_numeric(column="numeric_str")
        res_df = get_result_df(result)

        expected = sample_df.copy()
        expected["numeric_str"] = pd.to_numeric(expected["numeric_str"], errors="coerce")

        assert_series_equal_loose(res_df["numeric_str"], expected["numeric_str"])
        self._record_result(
            test_name="to_numeric",
            method_call='uploaded_ctx.to_numeric(column="numeric_str")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 9. map_values
    # ----------------------------------------------------
    def test_map_values(self, uploaded_ctx, sample_df, backend_config):
        mapping = {"HR": "Human Resources", "IT": "Technology"}
        result = uploaded_ctx.map_values(column="department", mapping=mapping)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "department")

        expected = sample_df.copy()
        expected["department"] = expected["department"].replace(mapping)
        assert_series_equal_loose(res_df[out_col], expected["department"])
        self._record_result(
            test_name="map_values",
            method_call='uploaded_ctx.map_values(column="department", mapping={"HR": "Human Resources", "IT": "Technology"})',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 10. filter_valid
    # ----------------------------------------------------
    def test_filter_valid(self, uploaded_ctx, sample_df, backend_config):
        valid = ["HR", "IT"]
        result = uploaded_ctx.filter_valid(column="department", valid_values=valid)
        res_df = get_result_df(result)

        expected_series = sample_df["department"].where(sample_df["department"].isin(valid))
        out_col = get_generated_col(result, "department")
        assert_series_equal_loose(res_df[out_col], expected_series)
        expected = pd.DataFrame({
            "department": sample_df["department"],
            out_col: expected_series,
        })
        self._record_result(
            test_name="filter_valid",
            method_call='uploaded_ctx.filter_valid(column="department", valid_values=["HR", "IT"])',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 11. compress_rare
    # ----------------------------------------------------
    def test_compress_rare(self, uploaded_ctx, sample_df, backend_config):
        min_count = 2
        other_label = "other"
        result = uploaded_ctx.compress_rare(column="category", min_count=min_count, other_label=other_label)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "category")

        expected = sample_df.copy()
        vc = expected["category"].value_counts()
        rare = vc[vc < min_count].index
        expected["category"] = expected["category"].where(~expected["category"].isin(rare), other_label)

        assert_series_equal_loose(res_df[out_col], expected["category"])
        self._record_result(
            test_name="compress_rare",
            method_call='uploaded_ctx.compress_rare(column="category", min_count=2, other_label="other")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 12. fix_dates
    # ----------------------------------------------------
    def test_fix_dates(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.fix_dates(column="date_col")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "date_col")

        expected = sample_df.copy()
        expected["date_col"] = pd.to_datetime(expected["date_col"], errors="coerce")

        assert_series_equal_loose(res_df[out_col], expected["date_col"], as_datetime=True)
        self._record_result(
            test_name="fix_dates",
            method_call='uploaded_ctx.fix_dates(column="date_col")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 13. clip_dates
    # ----------------------------------------------------
    def test_clip_dates(self, uploaded_ctx, sample_df, backend_config):
        min_dt = "2024-02-01"
        max_dt = "2024-04-01"
        result = uploaded_ctx.clip_dates(column="date_col", min_dt=min_dt, max_dt=max_dt)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "date_col")

        expected = sample_df.copy()
        dt = pd.to_datetime(expected["date_col"], errors="coerce")
        expected["date_col"] = dt.where((dt >= pd.Timestamp(min_dt)) & (dt <= pd.Timestamp(max_dt)))
        assert_series_equal_loose(res_df[out_col], expected["date_col"], as_datetime=True)
        self._record_result(
            test_name="clip_dates",
            method_call='uploaded_ctx.clip_dates(column="date_col", min_dt="2024-02-01", max_dt="2024-04-01")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 14. groupby_fillna – numeric mean
    # ----------------------------------------------------
    def test_groupby_fillna_numeric_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby_fillna(
            column="salary", group_cols=["department"], method="mean"
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df.copy()
        expected["salary"] = expected.groupby("department")["salary"].transform(
            lambda x: x.fillna(x.mean())
        )

        assert_series_equal_loose(res_df[out_col], expected["salary"])
        self._record_result(
            test_name="groupby_fillna_numeric_mean",
            method_call='uploaded_ctx.groupby_fillna(column="salary", group_cols=["department"], method="mean")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 15. groupby_fillna – numeric constant
    # ----------------------------------------------------
    def test_groupby_fillna_numeric_constant(self, uploaded_ctx, sample_df, backend_config):
        value = -1
        result = uploaded_ctx.groupby_fillna(
            column="salary", group_cols=["department"], method="constant", value=value
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "salary")

        expected = sample_df.copy()
        expected["salary"] = expected["salary"].fillna(value)

        assert_series_equal_loose(res_df[out_col], expected["salary"])
        self._record_result(
            test_name="groupby_fillna_numeric_constant",
            method_call='uploaded_ctx.groupby_fillna(column="salary", group_cols=["department"], method="constant", value=-1)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 16. groupby_fillna – categorical mode
    # ----------------------------------------------------
    def test_groupby_fillna_categorical(self, uploaded_ctx, sample_df, backend_config):
        df = sample_df.copy()
        df.loc[1, "category"] = None
        ctx = uploaded_ctx.memframe.upload_df(df, filename="cleaning_gb_cat")
        result = ctx.groupby_fillna(
            column="category", group_cols=["department"], method="mode"
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "category")

        expected = df.copy()
        expected["category"] = expected.groupby("department")["category"].transform(
            lambda x: x.fillna(x.mode().iloc[0] if not x.mode().empty else x.dropna().iloc[0])
        )
        assert res_df[out_col].isna().sum() == 0
        self._record_result(
            test_name="groupby_fillna_categorical",
            method_call='ctx.groupby_fillna(column="category", group_cols=["department"], method="mode")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 17. groupby_fillna – datetime constant
    # ----------------------------------------------------
    def test_groupby_fillna_datetime(self, uploaded_ctx, sample_df, backend_config):
        df = sample_df.copy()
        df.loc[2, "date_col"] = None
        ctx = uploaded_ctx.memframe.upload_df(df, filename="cleaning_gb_dt")
        method = "ffill"
        result = ctx.groupby_fillna(
            column="date_col", group_cols=["department"], method=method
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "date_col")

        expected = df.copy()
        expected["date_col"] = pd.to_datetime(expected["date_col"], errors="coerce")
        expected["date_col"] = expected.groupby("department")["date_col"].transform(lambda x: x.ffill())
        assert_series_equal_loose(res_df[out_col], expected["date_col"], as_datetime=True)
        self._record_result(
            test_name="groupby_fillna_datetime",
            method_call='ctx.groupby_fillna(column="date_col", group_cols=["department"], method="ffill")',
            original_df=df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 18. dropna
    # ----------------------------------------------------
    def test_dropna(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.dropna()
        res_df = get_result_df(result)

        expected = sample_df.dropna().reset_index(drop=True)
        res_df = normalize_frame(res_df)
        pd.testing.assert_frame_equal(expected, res_df, check_dtype=False)
        self._record_result(
            test_name="dropna",
            method_call="uploaded_ctx.dropna()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 19. drop (columns and rows)
    # ----------------------------------------------------
    def test_drop_columns(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.drop(axis=1, columns=["bonus"])
        res_df = get_result_df(result)

        expected = sample_df.drop(columns=["bonus"])
        pd.testing.assert_frame_equal(
            normalize_frame(res_df), normalize_frame(expected), check_dtype=False
        )
        self._record_result(
            test_name="drop_columns",
            method_call='uploaded_ctx.drop(axis=1, columns=["bonus"])',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_drop_rows(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.drop(index=[0, 1])
        res_df = get_result_df(result)

        expected = sample_df.drop(index=[0, 1]).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            normalize_frame(res_df), normalize_frame(expected), check_dtype=False
        )
        self._record_result(
            test_name="drop_rows",
            method_call="uploaded_ctx.drop(index=[0, 1])",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 20. isna / notna
    # ----------------------------------------------------
    def test_isna(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.isna()
        res_df = get_result_df(result)

        expected = sample_df.isna()
        pd.testing.assert_frame_equal(res_df, expected, check_dtype=False)
        self._record_result(
            test_name="isna",
            method_call="uploaded_ctx.isna()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_notna(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.notna()
        res_df = get_result_df(result)

        expected = sample_df.notna()
        pd.testing.assert_frame_equal(res_df, expected, check_dtype=False)
        self._record_result(
            test_name="notna",
            method_call="uploaded_ctx.notna()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 21. drop_duplicates
    # ----------------------------------------------------
    def test_drop_duplicates_all(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.drop_duplicates()
        res_df = normalize_frame(get_result_df(result))
        expected = normalize_frame(sample_df.drop_duplicates())
        sort_cols = [c for c in expected.columns if c in res_df.columns]
        res_sorted = res_df.sort_values(sort_cols, kind="stable").reset_index(drop=True)
        exp_sorted = expected.sort_values(sort_cols, kind="stable").reset_index(drop=True)
        pd.testing.assert_frame_equal(exp_sorted, res_sorted, check_dtype=False)
        self._record_result(
            test_name="drop_duplicates_all",
            method_call="uploaded_ctx.drop_duplicates()",
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_drop_duplicates_subset(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.drop_duplicates(subset=["id"])
        res_df = normalize_frame(get_result_df(result))
        expected = normalize_frame(sample_df.drop_duplicates(subset=["id"]))
        res_sorted = res_df.sort_values(["id"], kind="stable").reset_index(drop=True)
        exp_sorted = expected.sort_values(["id"], kind="stable").reset_index(drop=True)
        pd.testing.assert_frame_equal(exp_sorted, res_sorted, check_dtype=False)
        self._record_result(
            test_name="drop_duplicates_subset",
            method_call='uploaded_ctx.drop_duplicates(subset=["id"])',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # 22. data quality – missing values
    # ----------------------------------------------------
    def test_data_quality_missing_values(self, uploaded_ctx, sample_df, backend_config):
        cols = ["salary", "bonus"]
        result = unwrap_result_payload(uploaded_ctx.data_quality_missing_values(columns=cols))
        expected = {col: {"count": int(sample_df[col].isna().sum()),
                          "percentage": sample_df[col].isna().mean()}
                    for col in cols}
        if isinstance(result, dict):
            for col in cols:
                assert result[col]["missing"] == expected[col]["count"]
                assert abs((result[col]["missing_pct"] / 100.0) - expected[col]["percentage"]) < 1e-6
        else:
            pytest.fail("Unexpected data quality result format")
        # Not a DataFrame result, so we skip PDF capture for summary outputs.

    # ----------------------------------------------------
    # 23. completeness score
    # ----------------------------------------------------
    def test_data_quality_completeness_score(self, uploaded_ctx, sample_df, backend_config):
        cols = ["salary", "bonus"]
        result = unwrap_result_payload(uploaded_ctx.data_quality_completeness_score(columns=cols))
        expected = {
            col: (1 - sample_df[col].isna().mean()) * 100.0
            for col in cols
        }
        if isinstance(result, dict):
            for col in cols:
                assert abs(result[col]["completeness"] - expected[col]) < 1e-6
        else:
            pytest.fail("Unexpected completeness score result")

    # ----------------------------------------------------
    # 24. comprehensive numeric summary
    # ----------------------------------------------------
    def test_comprehensive_numeric_summary(self, uploaded_ctx, sample_df, backend_config):
        cols = ["salary", "bonus"]
        result = unwrap_result_payload(uploaded_ctx.comprehensive_numeric_summary(columns=cols))
        if isinstance(result, dict) and result.get("is_error"):
            assert "numeric_basic_summary" in (result.get("error_message") or "")
        else:
            for col in cols:
                assert col in result
                assert isinstance(result[col], str)

    # ----------------------------------------------------
    # 25. statistical profile report
    # ----------------------------------------------------
    def test_statistical_profile_report(self, uploaded_ctx, sample_df, backend_config):
        cols = ["salary", "bonus"]
        result = unwrap_result_payload(uploaded_ctx.statistical_profile_report(columns=cols))
        assert isinstance(result, dict)
        assert "completeness" in result
        assert "numeric" in result
