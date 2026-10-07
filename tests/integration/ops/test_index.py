# tests/integration/ops/test_index.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from memframe import MemFrame
from memframe.exceptions import OperationError

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
    return pytest.UsageError(f"Invalid index DB configuration: {message}")


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
# Test data fixtures
# ----------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """Small frame with an int key, a second key col, and float values.

    `sale` is float so gap-introducing reindexes compare cleanly across
    backends (no int/NaN dtype churn).
    """
    return pd.DataFrame({
        "month": [1, 4, 7, 10],
        "year": [2012, 2014, 2013, 2014],
        "sale": [55.0, 40.0, 84.0, 31.0],
        "region": ["E", "W", "E", "W"],
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
    return connected_memframe.upload_df(sample_df, filename="index_dataset")


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


def assert_frames_equal(res_df: pd.DataFrame, expected: pd.DataFrame):
    pd.testing.assert_frame_equal(
        normalize_frame(res_df),
        normalize_frame(expected),
        check_dtype=False,
        check_names=False,
    )


def normalize_text_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Stringify object columns on both sides.

    Typed engines coerce fills (fill_value=0 into VARCHAR becomes '0');
    pandas keeps the raw scalar. Comparing as text asserts the values match
    under the engine's documented coercion.
    """
    out = df.copy()
    for col in out.select_dtypes(include="object").columns:
        out[col] = out[col].astype(str)
    return out


# ----------------------------------------------------------------------
# PDF helpers (for optional reporting)
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
    if isinstance(value, list):
        return pd.DataFrame({"index": value})
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
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.1, 1.2)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Test class
# ----------------------------------------------------------------------
class TestIndexOperations:
    """Index tests that require a backend connection (ctx-level API)."""

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
            pdf_path = RESULT_DIR / f"test_index_report_{request.node.name}.pdf"
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
        frame_locals = getattr(request.node, "_index_failure_locals", None)
        if frame_locals is None:
            frame_locals = getattr(request.node, "_failure_locals", {})

        original_df = _coerce_pdf_df(
            frame_locals.get("sample_df"),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "original", "result"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_value = None
        for name in ("expected", "expected_df", "expected_series", "sample_df"):
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
        pandas_call="",
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
    # set/get index
    # ------------------------------------------------------------------
    def test_set_and_get_index(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.set_index("month")
        assert result["index_cols"] == ["month"]
        assert uploaded_ctx.index == [1, 4, 7, 10]
        expected = sample_df.set_index("month").index.tolist()
        assert uploaded_ctx.index == expected
        self._record_result(
            test_name="set_and_get_index",
            method_call='uploaded_ctx.set_index("month")',
            pandas_call='df.set_index("month").index.tolist()',
            original_df=sample_df,
            memframe_df=uploaded_ctx.index,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_synthetic_index(self, uploaded_ctx, sample_df, backend_config):
        assert uploaded_ctx.index == [0, 1, 2, 3]
        self._record_result(
            test_name="synthetic_index",
            method_call="uploaded_ctx.index",
            pandas_call="list(range(len(df)))",
            original_df=sample_df,
            memframe_df=uploaded_ctx.index,
            pandas_df=list(range(len(sample_df))),
            backend=backend_config["connection_type"],
        )

    def test_set_unknown_key_errors(self, uploaded_ctx, sample_df, backend_config):
        with pytest.raises(OperationError, match="not found"):
            uploaded_ctx.set_index("nope")
        self._record_result(
            test_name="set_unknown_key_errors",
            method_call='uploaded_ctx.set_index("nope")',
            pandas_call="n/a — guard test (OperationError)",
            original_df=sample_df,
            memframe_df=_empty_pdf_df("expected OperationError"),
            pandas_df=_empty_pdf_df("n/a"),
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # reindex rows
    # ------------------------------------------------------------------
    def test_reindex_plain(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([1, 2, 4]))
        expected = sample_df.set_index("month").reindex([1, 2, 4]).reset_index()
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_plain",
            method_call="uploaded_ctx.reindex([1, 2, 4])",
            pandas_call='df.set_index("month").reindex([1, 2, 4]).reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_fill_value(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([1, 2, 4], fill_value=0))
        expected = sample_df.set_index("month").reindex([1, 2, 4], fill_value=0).reset_index()
        assert_frames_equal(normalize_text_cols(res_df), normalize_text_cols(expected))
        self._record_result(
            test_name="reindex_fill_value",
            method_call="uploaded_ctx.reindex([1, 2, 4], fill_value=0)",
            pandas_call='df.set_index("month").reindex([1, 2, 4], fill_value=0).reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_columns(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex(columns=["sale", "nope"], fill_value=-1))
        expected = sample_df.reindex(columns=["sale", "nope"], fill_value=-1)
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_columns",
            method_call='uploaded_ctx.reindex(columns=["sale", "nope"], fill_value=-1)',
            pandas_call='df.reindex(columns=["sale", "nope"], fill_value=-1)',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_ffill(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([1, 2, 3, 4, 7], method="ffill"))
        expected = sample_df.set_index("month").reindex([1, 2, 3, 4, 7], method="ffill").reset_index()
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_ffill",
            method_call='uploaded_ctx.reindex([1, 2, 3, 4, 7], method="ffill")',
            pandas_call='df.set_index("month").reindex([1, 2, 3, 4, 7], method="ffill").reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_ffill_limit(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([1, 2, 3, 4, 7], method="ffill", limit=1))
        expected = (
            sample_df.set_index("month").reindex([1, 2, 3, 4, 7], method="ffill", limit=1).reset_index()
        )
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_ffill_limit",
            method_call='uploaded_ctx.reindex([1, 2, 3, 4, 7], method="ffill", limit=1)',
            pandas_call='df.set_index("month").reindex(..., method="ffill", limit=1).reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_bfill(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([1, 2, 3, 4, 7], method="bfill"))
        expected = sample_df.set_index("month").reindex([1, 2, 3, 4, 7], method="bfill").reset_index()
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_bfill",
            method_call='uploaded_ctx.reindex([1, 2, 3, 4, 7], method="bfill")',
            pandas_call='df.set_index("month").reindex(..., method="bfill").reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_reindex_nearest(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex([2, 9], method="nearest"))
        expected = sample_df.set_index("month").reindex([2, 9], method="nearest").reset_index()
        assert_frames_equal(res_df, expected)
        self._record_result(
            test_name="reindex_nearest",
            method_call='uploaded_ctx.reindex([2, 9], method="nearest")',
            pandas_call='df.set_index("month").reindex([2, 9], method="nearest").reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # multi-index
    # ------------------------------------------------------------------
    def test_multi_index(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index(["year", "month"])
        assert uploaded_ctx.index == [(2012, 1), (2013, 7), (2014, 4), (2014, 10)]
        res_df = get_result_df(uploaded_ctx.reindex([(2012, 1), (2014, 5), (2014, 10)], fill_value=0))
        expected = (
            sample_df.set_index(["year", "month"])
            .reindex([(2012, 1), (2014, 5), (2014, 10)], fill_value=0)
            .reset_index()
        )
        assert_frames_equal(normalize_text_cols(res_df), normalize_text_cols(expected))
        self._record_result(
            test_name="multi_index",
            method_call='uploaded_ctx.set_index(["year", "month"]) + reindex(...)',
            pandas_call='df.set_index(["year", "month"]).reindex([...], fill_value=0).reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # reset / guards
    # ------------------------------------------------------------------
    def test_reset_level(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index(["year", "month"])
        result = uploaded_ctx.reset_index(level="year")
        assert result["index_cols"] == ["month"]
        assert uploaded_ctx.index == [1, 4, 7, 10]
        self._record_result(
            test_name="reset_level",
            method_call='uploaded_ctx.reset_index(level="year")',
            pandas_call="n/a — metadata op",
            original_df=sample_df,
            memframe_df=uploaded_ctx.index,
            pandas_df=[1, 4, 7, 10],
            backend=backend_config["connection_type"],
        )

    def test_reset_all_and_guard(self, uploaded_ctx, sample_df, backend_config):
        uploaded_ctx.set_index("month")
        assert uploaded_ctx.reset_index()["index_cols"] == []
        with pytest.raises(OperationError, match="set_index"):
            uploaded_ctx.reindex([1, 2])
        with pytest.raises(OperationError, match="rename"):
            uploaded_ctx.set_index("month")
            uploaded_ctx.reset_index(names={"month": "m"})
        self._record_result(
            test_name="reset_all_and_guard",
            method_call="uploaded_ctx.reset_index() + guard cases",
            pandas_call="n/a — guard tests (OperationError)",
            original_df=sample_df,
            memframe_df=_empty_pdf_df("expected OperationErrors"),
            pandas_df=_empty_pdf_df("n/a"),
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # reindex_like
    # ------------------------------------------------------------------
    def test_reindex_like(self, connected_memframe, uploaded_ctx, sample_df, backend_config):
        other = connected_memframe.upload_df(
            pd.DataFrame({"month": [4, 7], "sale": [400.0, 700.0]}),
            filename="index_like_source",
        )
        other.set_index("month")
        uploaded_ctx.set_index("month")
        res_df = get_result_df(uploaded_ctx.reindex_like(other._data_id, fill_value=-1))
        # ponytail: like pandas reindex_like, columns conform to the other
        # frame too — the other upload only carries month/sale.
        expected = (
            sample_df.set_index("month")
            .reindex([4, 7], fill_value=-1)
            .reset_index()[["month", "sale"]]
        )
        assert_frames_equal(normalize_text_cols(res_df), normalize_text_cols(expected))
        self._record_result(
            test_name="reindex_like",
            method_call="uploaded_ctx.reindex_like(other_id, fill_value=-1)",
            pandas_call='df.set_index("month").reindex([4, 7], fill_value=-1).reset_index()',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # source-table immutability
    # ------------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        before = get_result_df(uploaded_ctx.full_table()).sort_values("month").reset_index(drop=True)
        uploaded_ctx.set_index("month")
        _ = uploaded_ctx.reindex([1, 2, 4, 7, 10], method="ffill")
        _ = uploaded_ctx.reindex(columns=["sale", "nope"], fill_value=0)
        uploaded_ctx.reset_index()
        after = get_result_df(uploaded_ctx.full_table()).sort_values("month").reset_index(drop=True)
        assert_frames_equal(after, normalize_frame(sample_df).sort_values("month").reset_index(drop=True))
        assert_frames_equal(after, before)
        self._record_result(
            test_name="mutation_safety",
            method_call="set/reindex/reset workflow, then full_table()",
            pandas_call="n/a — source table must be unchanged",
            original_df=sample_df,
            memframe_df=after,
            pandas_df=normalize_frame(sample_df).sort_values("month").reset_index(drop=True),
            backend=backend_config["connection_type"],
        )
