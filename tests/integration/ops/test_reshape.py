# tests/test_reshape.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np
import pytest

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from memframe import MemFrame

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
    return pytest.UsageError(f"Invalid reshape DB configuration: {message}")


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
        raise _usage_error("Postgres/ClickHouse param 'port' must be an integer") from exc
    if port < 1 or port > 65535:
        raise _usage_error("Postgres/ClickHouse param 'port' must be between 1 and 65535")
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
# Fixtures for test data
# ----------------------------------------------------------------------

@pytest.fixture(scope="function")
def explode_df() -> pd.DataFrame:
    """DataFrame with a column containing lists for explode tests."""
    return pd.DataFrame({
        "id": [1, 2],
        "category": ["A", "B"],
        # ponytail: bracketed strings, not real lists — the uploader cannot
        # ingest list<item> columns (pyarrow cast), and explode parses text.
        "values": ["[10, 20, 30]", "[40, 50]"],
        "tags": ["x,y,z", "u,v"],
    })


@pytest.fixture(scope="function")
def melt_df() -> pd.DataFrame:
    """DataFrame suitable for melt tests."""
    return pd.DataFrame({
        "student": ["Alice", "Bob"],
        "math": [85, 90],
        "science": [92, 88],
        "english": [78, 85],
    })


@pytest.fixture(scope="function")
def pivot_df() -> pd.DataFrame:
    """DataFrame for pivot / pivot_table / crosstab tests."""
    return pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=6, freq="D"),
        "city": ["NY", "NY", "LA", "LA", "NY", "LA"],
        "product": ["A", "B", "A", "B", "A", "B"],
        "sales": [100, 200, 150, 300, 120, 180],
        "quantity": [1, 2, 1, 3, 1, 2],
    })


@pytest.fixture(scope="function")
def rank_df() -> pd.DataFrame:
    """DataFrame for rank and groupby_rank tests."""
    return pd.DataFrame({
        "department": ["HR", "HR", "IT", "IT", "IT", "Sales"],
        "employee": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
        "score": [85, 92, 78, 88, 92, 80],
        "salary": [50000, 55000, 60000, 62000, 62000, 45000],
    })


@pytest.fixture(scope="function")
def transpose_df() -> pd.DataFrame:
    """Small DataFrame for transpose tests."""
    return pd.DataFrame({
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "C": [7, 8, 9],
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
        asyncio.run(mf.aclose())


# Uploaded contexts for each test dataset
@pytest.fixture(scope="function")
def explode_ctx(connected_memframe, explode_df) -> Any:
    return connected_memframe.upload_df(explode_df, filename="explode_dataset")

@pytest.fixture(scope="function")
def melt_ctx(connected_memframe, melt_df) -> Any:
    return connected_memframe.upload_df(melt_df, filename="melt_dataset")

@pytest.fixture(scope="function")
def pivot_ctx(connected_memframe, pivot_df) -> Any:
    return connected_memframe.upload_df(pivot_df, filename="pivot_dataset")

@pytest.fixture(scope="function")
def rank_ctx(connected_memframe, rank_df) -> Any:
    return connected_memframe.upload_df(rank_df, filename="rank_dataset")

@pytest.fixture(scope="function")
def transpose_ctx(connected_memframe, transpose_df) -> Any:
    return connected_memframe.upload_df(transpose_df, filename="transpose_dataset")


# ----------------------------------------------------------------------
# Helpers
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
        for key in ("result", "current_state", "data"):
            if key in result:
                inner = result[key]
                if isinstance(inner, pd.DataFrame):
                    return inner
                if isinstance(inner, dict):
                    if "result" in inner and isinstance(inner["result"], pd.DataFrame):
                        return inner["result"]
                    if "data" in inner and isinstance(inner["data"], pd.DataFrame):
                        return inner["data"]
        raise AssertionError(f"Cannot extract DataFrame from dict: {list(result.keys())}")
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Drop helper columns and reset index for stable comparisons."""
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)


def sort_by_available_id(df: pd.DataFrame) -> pd.DataFrame:
    """Sort using an available id-like column."""
    for col in ("id", "student", "employee", "department"):
        if col in df.columns:
            return df.sort_values(col).reset_index(drop=True)
    return df.reset_index(drop=True)


def _to_second_precision_str(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")


def assert_series_equal_loose(actual: pd.Series, expected: pd.Series):
    """Compare two Series ignoring name and dtype."""
    pd.testing.assert_series_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_names=False,
    )


# ----------------------------------------------------------------------
# PDF helper (optional but consistent with previous patterns)
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
class TestReshapingOperations:
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
            pdf_path = RESULT_DIR / f"test_reshape_report_{request.node.name}.pdf"
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

        original_value = None
        for name in ("explode_df", "melt_df", "pivot_df", "transpose_df", "rank_df"):
            if name in frame_locals:
                original_value = frame_locals[name]
                break
        original_df = _coerce_pdf_df(
            original_value,
            "Original DataFrame was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "res_sorted", "actual_pairs", "result"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_value = None
        for name in ("expected", "exp_sorted", "expected_pairs"):
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
        test_name,
        method_call,
        original_df,
        memframe_df,
        pandas_df,
        backend,
        status="PENDING",
        error_message="",
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
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ------------------------------------------------------------------
    # explode
    # ------------------------------------------------------------------
    def test_explode(self, explode_ctx, explode_df, backend_config):
        # Core explode splits bracketed list-like strings.
        result = explode_ctx.explode("values")
        res_df = get_result_df(result)
        expected = explode_df.assign(
            values=explode_df["values"].apply(
                lambda s: [v.strip() for v in str(s).strip("[]").split(",")]
            )
        ).explode("values").reset_index(drop=True)
        expected = expected[res_df.columns]
        # Core split preserves surrounding spaces in list-like strings; normalize both sides.
        res_df["values"] = res_df["values"].astype(str).str.strip()
        expected["values"] = expected["values"].astype(str).str.strip()
        # Align columns (the library may keep original order)
        pd.testing.assert_frame_equal(
            normalize_frame(res_df).sort_values("id").reset_index(drop=True),
            normalize_frame(expected).sort_values("id").reset_index(drop=True),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="explode",
            method_call='explode_ctx.explode("tags")',
            original_df=explode_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # melt
    # ------------------------------------------------------------------
    def test_melt(self, melt_ctx, melt_df, backend_config):
        result = melt_ctx.melt(id_vars=["student"], value_vars=["math", "science", "english"],
                               var_name="subject", value_name="score")
        res_df = get_result_df(result)
        expected = melt_df.melt(id_vars="student", value_vars=["math", "science", "english"],
                                var_name="subject", value_name="score")
        pd.testing.assert_frame_equal(
            normalize_frame(res_df).sort_values(["student", "subject"]).reset_index(drop=True),
            normalize_frame(expected).sort_values(["student", "subject"]).reset_index(drop=True),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="melt",
            method_call='melt_ctx.melt(...)',
            original_df=melt_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # pivot
    # ------------------------------------------------------------------
    def test_pivot(self, pivot_ctx, pivot_df, backend_config):
        # pivot on index='date', columns='product', values='sales'
        result = pivot_ctx.pivot(index="date", columns="product", values="sales")
        res_df = get_result_df(result)
        expected = pivot_df.pivot(index="date", columns="product", values="sales").reset_index()
        res_sorted = res_df.sort_values("date").reset_index(drop=True)
        exp_sorted = expected.sort_values("date").reset_index(drop=True)
        if "date" in res_sorted.columns and "date" in exp_sorted.columns:
            res_sorted["date"] = _to_second_precision_str(res_sorted["date"])
            exp_sorted["date"] = _to_second_precision_str(exp_sorted["date"])
        common_cols = [c for c in res_sorted.columns if c in exp_sorted.columns]
        pd.testing.assert_frame_equal(
            res_sorted[common_cols],
            exp_sorted[common_cols],
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="pivot",
            method_call='pivot_ctx.pivot(index="date", columns="product", values="sales")',
            original_df=pivot_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # pivot_table
    # ------------------------------------------------------------------
    def test_pivot_table(self, pivot_ctx, pivot_df, backend_config):
        result = pivot_ctx.pivot_table(index="city", columns="product", values="sales", aggfunc="sum")
        res_df = get_result_df(result)
        expected = pivot_df.pivot_table(index="city", columns="product", values="sales", aggfunc="sum").reset_index()
        common_cols = [c for c in res_df.columns if c in expected.columns]
        res_sorted = res_df.sort_values("city").reset_index(drop=True)[common_cols]
        exp_sorted = expected.sort_values("city").reset_index(drop=True)[common_cols]
        pd.testing.assert_frame_equal(
            res_sorted,
            exp_sorted,
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="pivot_table",
            method_call='pivot_ctx.pivot_table(index="city", columns="product", values="sales", aggfunc="sum")',
            original_df=pivot_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # crosstab
    # ------------------------------------------------------------------
    def test_crosstab(self, pivot_ctx, pivot_df, backend_config):
        result = pivot_ctx.crosstab(index="city", columns="product", values="sales", aggfunc="sum")
        res_df = get_result_df(result)
        expected = pd.crosstab(
            index=pivot_df["city"], columns=pivot_df["product"],
            values=pivot_df["sales"], aggfunc="sum"
        ).reset_index()
        common_cols = [c for c in res_df.columns if c in expected.columns]
        res_sorted = res_df.sort_values("city").reset_index(drop=True)[common_cols]
        exp_sorted = expected.sort_values("city").reset_index(drop=True)[common_cols]
        pd.testing.assert_frame_equal(res_sorted, exp_sorted, check_dtype=False, check_names=False)
        self._record_result(
            test_name="crosstab",
            method_call='pivot_ctx.crosstab(index="city", columns="product", values="sales", aggfunc="sum")',
            original_df=pivot_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # transpose
    # ------------------------------------------------------------------
    def test_transpose(self, transpose_ctx, transpose_df, backend_config):
        result = transpose_ctx.transpose()
        res_df = get_result_df(result)
        expected = pd.DataFrame({
            "column_name": transpose_df.columns.astype(str),
            "1": transpose_df.iloc[0].values,
            "2": transpose_df.iloc[1].values,
            "3": transpose_df.iloc[2].values,
        })
        expected = expected[res_df.columns]
        # ponytail: GROUP BY returns rows in arbitrary order (postgres) —
        # compare order-insensitively like the other reshape tests.
        pd.testing.assert_frame_equal(
            normalize_frame(res_df).sort_values("column_name").reset_index(drop=True),
            normalize_frame(expected).sort_values("column_name").reset_index(drop=True),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="transpose",
            method_call="transpose_ctx.transpose()",
            original_df=transpose_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # rank
    # ------------------------------------------------------------------
    def test_rank(self, rank_ctx, rank_df, backend_config):
        result = rank_ctx.rank(columns=["score"], method="average", ascending=False)
        res_df = get_result_df(result)
        expected = rank_df.copy()
        expected["score_rank"] = expected["score"].rank(ascending=False, method="average")
        rank_col = [c for c in res_df.columns if "rank" in c.lower() and c != "score"]
        if not rank_col:
            rank_col = ["score_rank"]
        expected_pairs = expected[["score", "score_rank"]].sort_values(["score", "score_rank"]).reset_index(drop=True)
        actual_pairs = res_df[["score", rank_col[0]]].rename(columns={rank_col[0]: "score_rank"})
        actual_pairs = actual_pairs.sort_values(["score", "score_rank"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual_pairs, expected_pairs, check_dtype=False, check_names=False)
        self._record_result(
            test_name="rank",
            method_call='rank_ctx.rank(columns=["score"], method="average", ascending=False)',
            original_df=rank_df,
            memframe_df=res_df,
            pandas_df=expected[["department","employee","score","salary","score_rank"]],
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # groupby_rank
    # ------------------------------------------------------------------
    def test_groupby_rank(self, rank_ctx, rank_df, backend_config):
        result = rank_ctx.groupby_rank(groupby="department", columns=["score"], method="dense", ascending=False)
        res_df = get_result_df(result)
        expected = rank_df.copy()
        expected["score_rank"] = expected.groupby("department")["score"].rank(ascending=False, method="dense")
        rank_col = [c for c in res_df.columns if "rank" in c.lower()]
        if not rank_col:
            rank_col = ["score_rank"]
        expected_pairs = expected[["department", "score", "score_rank"]].sort_values(
            ["department", "score", "score_rank"]
        ).reset_index(drop=True)
        actual_pairs = res_df[["department", "score", rank_col[0]]].rename(columns={rank_col[0]: "score_rank"})
        actual_pairs = actual_pairs.sort_values(["department", "score", "score_rank"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual_pairs, expected_pairs, check_dtype=False, check_names=False)
        self._record_result(
            test_name="groupby_rank",
            method_call='rank_ctx.groupby_rank(groupby="department", columns=["score"], method="dense", ascending=False)',
            original_df=rank_df,
            memframe_df=res_df,
            pandas_df=expected[["department","employee","score","salary","score_rank"]],
            backend=backend_config["connection_type"],
        )
