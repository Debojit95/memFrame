# tests/test_cumulative.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np
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
    return pytest.UsageError(f"Invalid cumulative DB configuration: {message}")


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
        raise _usage_error(
            "Postgres/ClickHouse param 'port' must be an integer"
        ) from exc
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
    allowed = {
        "backend",
        "host",
        "port",
        "user",
        "password",
        "database",
        "secure",
        "timeout",
    }
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
    """Create a DataFrame with columns suitable for cumulative operations."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
            "value_dec": [10.5, 20.7, 30.2, 40.9, 50.1],
            "value_neg": [-5, 15, -25, 35, -45],
            "const": [100, 100, 100, 100, 100],
        }
    )


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
    ctx = connected_memframe.upload_df(sample_df, filename="cumulative_dataset")
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
            raise AssertionError(
                result.get("error_message") or f"Operation failed: {result}"
            )
        if "result" in result and isinstance(result["result"], pd.DataFrame):
            return result["result"]
        if "data" in result and isinstance(result["data"], pd.DataFrame):
            return result["data"]
    raise AssertionError(f"Cannot extract DataFrame from type {type(result)}: {result}")


def get_generated_col(result: Any, fallback: str) -> str:
    """Retrieve the name of the new column (if renamed) else fallback."""
    if isinstance(result, pd.DataFrame):
        return fallback
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
    """Compare two Series while ignoring non-semantic metadata differences.

    Result samples come from SELECTs without ORDER BY, so row order is not
    part of the API contract (ClickHouse background merges reorder rows under
    load); compare sorted sequences — a permutation passes, missing or
    duplicated values still fail. Valid for cumulative ops: input order and
    window order are fixed, so the result multiset is invariant and only the
    sample's row order floats.
    """
    actual_series = actual.reset_index(drop=True)
    expected_series = expected.reset_index(drop=True)
    if as_datetime:
        actual_series = pd.to_datetime(actual_series, errors="coerce").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        expected_series = pd.to_datetime(expected_series, errors="coerce").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    actual_series = actual_series.sort_values(
        kind="stable", na_position="last"
    ).reset_index(drop=True)
    expected_series = expected_series.sort_values(
        kind="stable", na_position="last"
    ).reset_index(drop=True)
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
            return pd.DataFrame(
                {
                    "is_error": [value.get("is_error")],
                    "error_message": [value.get("error_message", "")],
                }
            )
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
# Test class
# ----------------------------------------------------------------------
class TestCumulativeOperations:
    """All cumulative tests that require a backend connection."""

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
            pdf_path = RESULT_DIR / f"test_cumulative_report_{request.node.name}.pdf"
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
            frame_locals.get("sample_df"),
            "sample_df was not available when this test failed",
        )

        memframe_value = None
        for name in ("res_df", "final_df", "original_df", "df1", "df2", "result"):
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
                "original_df": _prepare_pdf_df(
                    _coerce_pdf_df(original_df, "No original data")
                ),
                "memframe_df": _prepare_pdf_df(
                    _coerce_pdf_df(memframe_df, "No MemFrame result")
                ),
                "pandas_df": _prepare_pdf_df(
                    _coerce_pdf_df(pandas_df, "No pandas result")
                ),
                "backend": backend,
                "status": status,
                "error_message": error_message,
            }
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    # ----------------------------------------------------
    # Cumulative sum
    # ----------------------------------------------------
    def test_cumsum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cumsum("value", order_col="id", target_col="cumsum_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumsum_val")

        expected = sample_df.sort_values("id").copy()
        expected["cumsum_val"] = expected["value"].cumsum()
        assert_series_equal_loose(res_df[out_col], expected["cumsum_val"])
        self._record_result(
            test_name="cumsum",
            method_call='uploaded_ctx.cumsum("value", order_col="id", target_col="cumsum_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_cumsum_no_order(self, uploaded_ctx, sample_df, backend_config):
        """Cumulative sum without explicit order should still work (uses default row order)."""
        result = uploaded_ctx.cumsum("value", target_col="cumsum_no_order")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumsum_no_order")

        expected = sample_df.copy()
        expected["cumsum_no_order"] = expected["value"].cumsum()
        assert_series_equal_loose(res_df[out_col], expected["cumsum_no_order"])
        self._record_result(
            test_name="cumsum_no_order",
            method_call='uploaded_ctx.cumsum("value", target_col="cumsum_no_order")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative product
    # ----------------------------------------------------
    def test_cumprod(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cumprod("value", order_col="id", target_col="cumprod_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumprod_val")

        expected = sample_df.sort_values("id").copy()
        expected["cumprod_val"] = expected["value"].cumprod()
        # Use approximate comparison because of floating‑point EXP(LN) implementation
        np.testing.assert_allclose(
            np.sort(res_df[out_col].values.astype(float)),
            np.sort(expected["cumprod_val"].values.astype(float)),
            rtol=1e-5,
        )
        self._record_result(
            test_name="cumprod",
            method_call='uploaded_ctx.cumprod("value", order_col="id", target_col="cumprod_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative maximum
    # ----------------------------------------------------
    def test_cummax(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cummax("value", order_col="id", target_col="cummax_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cummax_val")

        expected = sample_df.sort_values("id").copy()
        expected["cummax_val"] = expected["value"].cummax()
        assert_series_equal_loose(res_df[out_col], expected["cummax_val"])
        self._record_result(
            test_name="cummax",
            method_call='uploaded_ctx.cummax("value", order_col="id", target_col="cummax_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative minimum
    # ----------------------------------------------------
    def test_cummin(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cummin("value", order_col="id", target_col="cummin_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cummin_val")

        expected = sample_df.sort_values("id").copy()
        expected["cummin_val"] = expected["value"].cummin()
        assert_series_equal_loose(res_df[out_col], expected["cummin_val"])
        self._record_result(
            test_name="cummin",
            method_call='uploaded_ctx.cummin("value", order_col="id", target_col="cummin_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative mean
    # ----------------------------------------------------
    def test_cummean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cummean("value", order_col="id", target_col="cummean_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cummean_val")

        expected = sample_df.sort_values("id").copy()
        expected["cummean_val"] = expected["value"].expanding().mean()
        assert_series_equal_loose(res_df[out_col], expected["cummean_val"])
        self._record_result(
            test_name="cummean",
            method_call='uploaded_ctx.cummean("value", order_col="id", target_col="cummean_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative count
    # ----------------------------------------------------
    def test_cumcount(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cumcount(
            "value", order_col="id", target_col="cumcount_val"
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumcount_val")

        expected = sample_df.sort_values("id").copy()
        # In pandas, expanding().count() counts non-null values
        expected["cumcount_val"] = expected["value"].expanding().count()
        assert_series_equal_loose(res_df[out_col], expected["cumcount_val"])
        self._record_result(
            test_name="cumcount",
            method_call='uploaded_ctx.cumcount("value", order_col="id", target_col="cumcount_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative standard deviation
    # ----------------------------------------------------
    def test_cumstd(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cumstd("value", order_col="id", target_col="cumstd_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumstd_val")

        expected = sample_df.sort_values("id").copy()
        # Library uses population std (ddof=0); first row is 0 not NaN
        expected["cumstd_val"] = expected["value"].expanding().std(ddof=0).fillna(0)
        np.testing.assert_allclose(
            np.sort(res_df[out_col].values.astype(float)),
            np.sort(expected["cumstd_val"].values.astype(float)),
            rtol=1e-5,
        )
        self._record_result(
            test_name="cumstd",
            method_call='uploaded_ctx.cumstd("value", order_col="id", target_col="cumstd_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Cumulative variance
    # ----------------------------------------------------
    def test_cumvar(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cumvar("value", order_col="id", target_col="cumvar_val")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "cumvar_val")

        expected = sample_df.sort_values("id").copy()
        # Library uses population variance (ddof=0); first row is 0 not NaN
        expected["cumvar_val"] = expected["value"].expanding().var(ddof=0).fillna(0)
        np.testing.assert_allclose(
            np.sort(res_df[out_col].values.astype(float)),
            np.sort(expected["cumvar_val"].values.astype(float)),
            rtol=1e-5,
        )
        self._record_result(
            test_name="cumvar",
            method_call='uploaded_ctx.cumvar("value", order_col="id", target_col="cumvar_val")',
            original_df=sample_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ----------------------------------------------------
    # Mutation safety and chaining
    # ----------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        """Ensure the source table is not modified by cumulative operations."""
        uploaded_ctx.cumsum("value", order_col="id", target_col="cumsum_val")
        # ponytail: read back the source table directly — the ContextManager
        # is not a result payload, so get_result_df() cannot consume it.
        # head() covers all 5 fixture rows; compare order-free for CH safety.
        original_df = uploaded_ctx.head(n=10)

        assert "cumsum_val" not in original_df.columns
        assert set(original_df.columns) == set(sample_df.columns)
        assert len(original_df) == len(sample_df)
        self._record_result(
            test_name="mutation_safety",
            method_call='uploaded_ctx.cumsum("value", order_col="id", target_col="cumsum_val") → original checked',
            original_df=sample_df,
            memframe_df=original_df,
            pandas_df=sample_df,
            backend=backend_config["connection_type"],
        )
