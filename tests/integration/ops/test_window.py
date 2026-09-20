# tests/integration/ops/test_window.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from memframe.main import MemFrame

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
    return pytest.UsageError(f"Invalid window DB configuration: {message}")


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
        raise _usage_error(
            "Postgres/ClickHouse param 'port' must be between 1 and 65535"
        )
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
def time_series_df() -> pd.DataFrame:
    """DataFrame with a datetime column, numeric values, and a null.

    Deliberately shuffled so physical row order differs from ``date`` order —
    ``order_by`` tests exercise real ordering, and no-``order_by`` tests run
    against an unordered table.
    """
    dates = pd.date_range("2025-01-01", periods=6, freq="D")
    df = pd.DataFrame({
        "date": dates,
        "sales": [10, 20, 15, 30, 25, 35],
        "quantity": [1, 2, 1, 3, 2, 3],
        "category": ["A", "B", "A", "B", "A", "B"],
        "score": [85.5, 92.3, 78.9, None, 88.0, 91.2],  # includes NaN
    })
    return df.sample(frac=1, random_state=42).reset_index(drop=True)

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

@pytest.fixture(scope="function")
def uploaded_ctx(connected_memframe, time_series_df) -> Any:
    """Upload the sample DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(time_series_df, filename="window_dataset")

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def get_result_df(result: Any) -> pd.DataFrame:
    """Extract DataFrame from library result."""
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
                    if "data" in inner and isinstance(inner["data"], pd.DataFrame):
                        return inner["data"]
        raise AssertionError(f"Cannot extract DataFrame from dict: {list(result.keys())}")
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")

def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)

def assert_series_equal_loose(actual: pd.Series, expected: pd.Series):
    pd.testing.assert_series_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_names=False,
    )


def _generated_col(res_df: pd.DataFrame, column: str) -> str:
    for col in res_df.columns:
        if col != column and str(col).startswith(str(column)):
            return col
    raise AssertionError(f"No generated column for {column!r} in {list(res_df.columns)}")


def _ordered(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("date").reset_index(drop=True)


def _assert_ordered_result(res_df: pd.DataFrame, value_col: str, expected: pd.Series) -> None:
    """Compare an order_by='date' result; preview rows are in physical order."""
    actual = res_df.sort_values("date").reset_index(drop=True)[value_col]
    assert_series_equal_loose(actual.astype(float), expected.astype(float))


def _assert_positional_result(res_df: pd.DataFrame, value_col: str, expected: pd.Series) -> None:
    """Compare a no-order_by result; physical/insertion order is preserved."""
    assert_series_equal_loose(res_df[value_col].astype(float), expected.astype(float))


def _assert_datetime_by_key(res_df: pd.DataFrame, value_col: str, key_df: pd.DataFrame) -> None:
    """Compare a datetime result ordered by a non-date key (merge on date)."""
    actual = res_df[["date", value_col]].copy()
    exp = key_df[["date", "expected"]].copy()
    actual["date"] = pd.to_datetime(actual["date"])
    exp["date"] = pd.to_datetime(exp["date"])
    merged = actual.merge(exp, on="date", how="inner").sort_values("date")
    actual_vals = pd.to_datetime(merged[value_col])
    expected_vals = pd.to_datetime(merged["expected"])
    assert list(actual_vals.isna()) == list(expected_vals.isna())
    mask = expected_vals.notna()
    assert_series_equal_loose(
        actual_vals[mask].astype("int64"), expected_vals[mask].astype("int64")
    )


def _pandas_rolling_oracle(func: str, series: pd.Series, window: int) -> pd.Series:
    """pandas equivalent of each engine rolling function (min_periods=1)."""
    r = series.rolling(window, min_periods=1)
    if func == "min":
        return r.min()
    if func == "max":
        return r.max()
    if func == "count":
        return r.count()
    if func == "mean":
        return r.mean()
    if func == "sum":
        return r.sum()
    if func == "std":
        return r.std(ddof=0)  # engine rolling std is population
    if func == "sem":
        return r.std(ddof=1) / r.count().pow(0.5)
    if func == "rank":
        return r.rank()
    if func == "nunique":
        return r.apply(lambda x: len(set(x)), raw=True)
    if func == "first":
        return r.apply(lambda x: x.iloc[0], raw=False)
    if func == "last":
        return r.apply(lambda x: x.iloc[-1], raw=False)
    raise ValueError(f"no pandas oracle for {func!r}")

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
class TestWindowOperations:
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
            pdf_path = RESULT_DIR / f"test_window_report_{request.node.name}.pdf"
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
        frame_locals = getattr(request.node, "_window_failure_locals", None)
        if frame_locals is None:
            frame_locals = getattr(request.node, "_failure_locals", {})

        original_df = _coerce_pdf_df(
            frame_locals.get("time_series_df"),
            "time_series_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "original", "actual_vals", "original_sales", "result"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_value = None
        for name in ("expected", "expected_sales", "time_series_df"):
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
            error_message=error_message,
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

    # ----------------------------------------------------------------
    # Rolling (direct, order_by="date")
    # ----------------------------------------------------------------
    def test_rolling_mean_direct(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(column="sales", window=3, func="mean", order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(3, min_periods=1).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="rolling_mean_direct",
            method_call='rolling(column="sales", window=3, func="mean", order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_rolling_sum_direct(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(column="sales", window=2, func="sum", order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(2, min_periods=1).sum()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="rolling_sum_direct",
            method_call='rolling(column="sales", window=2, func="sum", order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Rolling (direct, no order_by — unordered physical order)
    # ----------------------------------------------------------------
    def test_rolling_mean_no_order(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(column="sales", window=3, func="mean")
        res_df = get_result_df(result)
        expected = time_series_df["sales"].rolling(3, min_periods=1).mean()
        _assert_positional_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="rolling_mean_no_order",
            method_call='rolling(column="sales", window=3, func="mean")  # no order_by',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_expanding_sum_no_order(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.expanding(column="sales", func="sum", min_periods=1)
        res_df = get_result_df(result)
        expected = time_series_df["sales"].expanding(1).sum()
        _assert_positional_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="expanding_sum_no_order",
            method_call='expanding(column="sales", func="sum", min_periods=1)  # no order_by',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_ewm_mean_no_order(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.ewm(
            column="sales", span=2, func="mean", adjust=False, ignore_na=False
        )
        res_df = get_result_df(result)
        expected = (
            time_series_df["sales"]
            .ewm(span=2, adjust=False, ignore_na=False)
            .mean()
        )
        _assert_positional_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="ewm_mean_no_order",
            method_call='ewm(column="sales", span=2, func="mean", adjust=False)  # no order_by',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Fluent rolling interface
    # ----------------------------------------------------------------
    def test_fluent_rolling_mean(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.on("sales").rolling(3).mean(order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(3, min_periods=1).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="fluent_rolling_mean",
            method_call='on("sales").rolling(3).mean(order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_fluent_rolling_std(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.on("sales").rolling(3).std(order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(3, min_periods=1).std(ddof=0)
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="fluent_rolling_std",
            method_call='on("sales").rolling(3).std(order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_fluent_rolling_quantile(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.on("sales").rolling(3).quantile(q=0.5, order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(3).quantile(0.5)
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="fluent_rolling_quantile",
            method_call='on("sales").rolling(3).quantile(q=0.5, order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Rolling — variable coverage (multi-func, nulls, func sweep)
    # ----------------------------------------------------------------
    def test_rolling_multi_func(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(
            column="sales", window=3, func=["sum", "mean"], order_by="date"
        )
        res_df = get_result_df(result)
        ordered = _ordered(time_series_df)
        for func in ("sum", "mean"):
            col = f"sales_rolling_{func}_w3"
            expected = _pandas_rolling_oracle(func, ordered["sales"], 3)
            _assert_ordered_result(res_df, col, expected)
        self._record_result(
            test_name="rolling_multi_func",
            method_call='rolling(column="sales", window=3, func=["sum", "mean"], order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=ordered,
            backend=backend_config["connection_type"],
        )

    def test_rolling_mean_with_nulls(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(column="score", window=3, func="mean", order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["score"].rolling(3, min_periods=1).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "score"), expected)
        self._record_result(
            test_name="rolling_mean_with_nulls",
            method_call='rolling(column="score", window=3, func="mean", order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    @pytest.mark.parametrize(
        "func",
        ["min", "max", "count", "mean", "sum", "std", "sem", "rank", "nunique", "first", "last"],
    )
    def test_rolling_func_sweep(self, uploaded_ctx, time_series_df, backend_config, func):
        result = uploaded_ctx.rolling(
            column="sales", window=3, func=func, order_by="date"
        )
        res_df = get_result_df(result)
        expected = _pandas_rolling_oracle(func, _ordered(time_series_df)["sales"], 3)
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name=f"rolling_func_sweep_{func}",
            method_call=f'rolling(column="sales", window=3, func="{func}", order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Rolling — datetime + edge windows
    # ----------------------------------------------------------------
    def test_rolling_median_datetime(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(
            column="date", window=3, func="median", order_by="sales"
        )
        res_df = get_result_df(result)
        by_sales = time_series_df.sort_values("sales").reset_index(drop=True)
        epoch = by_sales["date"].astype("int64")
        expected = pd.to_datetime(epoch.rolling(3, min_periods=1).median())
        _assert_datetime_by_key(
            res_df,
            _generated_col(res_df, "date"),
            pd.DataFrame({"date": by_sales["date"], "expected": expected}),
        )
        self._record_result(
            test_name="rolling_median_datetime",
            method_call='rolling(column="date", window=3, func="median", order_by="sales")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=by_sales,
            backend=backend_config["connection_type"],
        )

    def test_rolling_min_datetime(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(
            column="date", window=3, func="min", order_by="sales"
        )
        res_df = get_result_df(result)
        by_sales = time_series_df.sort_values("sales").reset_index(drop=True)
        epoch = by_sales["date"].astype("int64")
        expected = pd.to_datetime(epoch.rolling(3, min_periods=1).min())
        _assert_datetime_by_key(
            res_df,
            _generated_col(res_df, "date"),
            pd.DataFrame({"date": by_sales["date"], "expected": expected}),
        )
        self._record_result(
            test_name="rolling_min_datetime",
            method_call='rolling(column="date", window=3, func="min", order_by="sales")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=by_sales,
            backend=backend_config["connection_type"],
        )

    def test_rolling_window_one(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.rolling(column="sales", window=1, func="sum", order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(1, min_periods=1).sum()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="rolling_window_one",
            method_call='rolling(column="sales", window=1, func="sum", order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_rolling_window_larger_than_frame(
        self, uploaded_ctx, time_series_df, backend_config
    ):
        result = uploaded_ctx.rolling(
            column="sales", window=10, func="mean", order_by="date"
        )
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].rolling(10, min_periods=1).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)

        quantile = uploaded_ctx.rolling(
            column="sales", window=10, func="quantile", order_by="date", q=0.5
        )
        quantile_df = get_result_df(quantile)
        assert quantile_df[_generated_col(quantile_df, "sales")].isna().all()
        self._record_result(
            test_name="rolling_window_larger_than_frame",
            method_call='rolling(column="sales", window=10, ...) on a 6-row frame',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Expanding
    # ----------------------------------------------------------------
    def test_expanding_sum_direct(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.expanding(column="sales", func="sum", order_by="date", min_periods=1)
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].expanding(1).sum()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="expanding_sum_direct",
            method_call='expanding(column="sales", func="sum", order_by="date", min_periods=1)',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_fluent_expanding_mean(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.on("sales").expanding(min_periods=2).mean(order_by="date")
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["sales"].expanding(2).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="fluent_expanding_mean",
            method_call='on("sales").expanding(min_periods=2).mean(order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_expanding_mean_min_periods_nulls(
        self, uploaded_ctx, time_series_df, backend_config
    ):
        result = uploaded_ctx.expanding(
            column="score", func="mean", order_by="date", min_periods=2
        )
        res_df = get_result_df(result)
        expected = _ordered(time_series_df)["score"].expanding(2).mean()
        _assert_ordered_result(res_df, _generated_col(res_df, "score"), expected)
        self._record_result(
            test_name="expanding_mean_min_periods_nulls",
            method_call='expanding(column="score", func="mean", order_by="date", min_periods=2)',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # EWM
    # ----------------------------------------------------------------
    def test_ewm_mean_direct(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.ewm(column="sales", span=2, func="mean", order_by="date", adjust=False, ignore_na=False)
        res_df = get_result_df(result)
        expected = (
            _ordered(time_series_df)["sales"]
            .ewm(span=2, adjust=False, ignore_na=False)
            .mean()
        )
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="ewm_mean_direct",
            method_call='ewm(column="sales", span=2, func="mean", order_by="date", adjust=False)',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_fluent_ewm_std(self, uploaded_ctx, time_series_df, backend_config):
        result = uploaded_ctx.on("sales").ewm(halflife=2, adjust=False).std(order_by="date")
        res_df = get_result_df(result)
        expected = (
            _ordered(time_series_df)["sales"]
            .ewm(halflife=2, adjust=False)
            .std()
        )
        _assert_ordered_result(res_df, _generated_col(res_df, "sales"), expected)
        self._record_result(
            test_name="fluent_ewm_std",
            method_call='on("sales").ewm(halflife=2, adjust=False).std(order_by="date")',
            original_df=time_series_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------------------
    # Mutation safety
    # ----------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, time_series_df, backend_config):
        _ = uploaded_ctx.rolling(column="sales", window=2, func="mean", order_by="date")
        original = get_result_df(uploaded_ctx.head(n=len(time_series_df)))
        # original DataFrame should still have the original sales values
        original_sales = original.sort_values("date")["sales"].reset_index(drop=True)
        expected_sales = time_series_df.sort_values("date")["sales"].reset_index(drop=True)
        # Check non-null values match
        mask = expected_sales.notna()
        assert_series_equal_loose(original_sales[mask], expected_sales[mask])
        self._record_result(
            test_name="mutation_safety",
            method_call="rolling then check original unchanged",
            original_df=time_series_df,
            memframe_df=original,
            pandas_df=time_series_df,
            backend=backend_config["connection_type"],
        )
