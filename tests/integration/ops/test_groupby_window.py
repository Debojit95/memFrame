# tests/test_groupby_window.py

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
    return pytest.UsageError(f"Invalid groupby_window DB configuration: {message}")


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
# Fixtures
# ----------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """DataFrame with groups, order column, and numeric columns for window ops."""
    return pd.DataFrame({
        "id":    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "group": ["A","A","A","A","A","B","B","B","B","B"],
        "sub":   ["p","p","q","q","p","p","p","q","q","p"],
        "value": [10, 20, 15, 30, 25, 5, 10, 20, 15, 5],
        "value_dec": [10.5, 20.7, 15.2, 30.9, 25.1, 5.5, 10.3, 20.8, 15.0, 5.2],
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
    ctx = connected_memframe.upload_df(sample_df, filename="groupby_window_dataset")
    return ctx


# ----------------------------------------------------------------------
# Helpers
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


def get_generated_col(result: Any, fallback: str) -> str:
    if isinstance(result, dict):
        cols = result.get("new_columns") or result.get("generated_cols") or []
        if cols:
            return cols[0]
    return fallback


def assert_allclose_numeric(actual, desired, rtol=1e-5) -> None:
    # ponytail: Postgres returns Decimal for aggregates while DuckDB
    # returns float; coerce both sides so assert_allclose can subtract.
    np.testing.assert_allclose(
        pd.to_numeric(actual, errors="coerce"),
        pd.to_numeric(desired, errors="coerce"),
        rtol=rtol,
    )


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)


def safe_run(op):
    """Run an operation, returning the result DataFrame or an error DataFrame."""
    try:
        return get_result_df(op)
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})


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
class TestGroupByWindowOperations:
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
            pdf_path = RESULT_DIR / f"test_groupby_window_report_{request.node.name}.pdf"
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
        for name in ("res_df", "after", "original", "err_df", "res"):
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
    # Helpers: pandas mirrors (order-aligned on group + id)
    # ----------------------------------------------------------------
    @staticmethod
    def _pandas_rolling(frame: pd.DataFrame, keys, col: str, window: int, func: str):
        # ponytail: the SQL frame always includes the current row
        # (min_periods=1 semantics) — mirror it in pandas.
        ordered = frame.sort_values([*keys, "id"]).copy()
        rolled = ordered.groupby(list(keys))[col].rolling(window, min_periods=1)
        return pd.Series(getattr(rolled, func)().values, index=ordered.index)

    # --- Direct rolling ---
    def test_direct_rolling_mean(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper

        result = asyncio.run(
            GroupByWindowStatsWrapper(uploaded_ctx).arolling(
                column="value", window=2, func="mean",
                group_cols="group", order_by="id",
            )
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        # align on (group, id): both frames carry the order key
        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "direct_rolling_mean",
            'arolling("value",2,"mean",group_cols="group",order_by="id")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_direct_rolling_sum(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper

        result = asyncio.run(
            GroupByWindowStatsWrapper(uploaded_ctx).arolling(
                column="value", window=3, func="sum",
                group_cols="group", order_by="id",
            )
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w3_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 3, "sum")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "direct_rolling_sum",
            'arolling("value",3,"sum",group_cols="group",order_by="id")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    # --- GroupBy builder: rolling ---
    def test_groupby_rolling_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_rolling_mean",
            '.groupby("group").rolling(2).mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_groupby_rolling_sum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(3, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w3_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 3, "sum")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_rolling_sum",
            '.groupby("group").rolling(3).sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_groupby_rolling_max(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").max("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_max_w2_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "max")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_rolling_max",
            '.groupby("group").rolling(2).max("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_groupby_rolling_no_order(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2).mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        assert out_col in list(res_df.columns)
        assert len(res_df) == len(sample_df)
        pandas_df = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        pandas_df[out_col] = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean").values
        self._record_result(
            "groupby_rolling_no_order",
            '.groupby("group").rolling(2).mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    # --- Expanding ---
    def test_groupby_expanding_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(min_periods=1, order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_mean_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby("group")["value"].expanding().mean().reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_expanding_mean",
            '.groupby("group").expanding(min_periods=1).mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_groupby_expanding_sum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(min_periods=1, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_sum_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby("group")["value"].expanding().sum().reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_expanding_sum",
            '.groupby("group").expanding(min_periods=1).sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    # --- EWM (engine port pending — see docs) ---
    # Grouped EWM is not covered: ewm_groupby depends on engine helpers
    # (_ensure_row_id, _alpha_from_params, _compute_ewm_array) that did not
    # survive the window core split. Re-add these tests with the port.

    # --- Aggregation ---
    def test_groupby_rolling_agg(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg("value", ["mean", "sum"])
        res_df = get_result_df(result)

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        assert_allclose_numeric(
            check["value_rolling_mean_w2_by_group"],
            self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean"),
        )
        assert_allclose_numeric(
            check["value_rolling_sum_w2_by_group"],
            self._pandas_rolling(exp_ordered, ["group"], "value", 2, "sum"),
        )
        pandas_df = exp_ordered.copy()
        pandas_df["value_rolling_mean_w2_by_group"] = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean").values
        pandas_df["value_rolling_sum_w2_by_group"] = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "sum").values
        self._record_result(
            "groupby_rolling_agg",
            '.groupby("group").rolling(2).agg("value",["mean","sum"])',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    # --- Multicolumn group-by ---
    def test_groupby_multicolumn_rolling(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").rolling(2, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w2_by_group_sub")

        check = res_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group", "sub"], "value", 2, "sum")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_multicolumn_rolling",
            '.groupby("group", "sub").rolling(2).sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_groupby_multicolumn_expanding(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").expanding(order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_mean_by_group_sub")

        check = res_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby(["group", "sub"])["value"].expanding().mean().reset_index(level=[0, 1], drop=True)
        )
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "groupby_multicolumn_expanding",
            '.groupby("group", "sub").expanding().mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    def test_direct_multicolumn_rolling(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper

        result = asyncio.run(
            GroupByWindowStatsWrapper(uploaded_ctx).arolling(
                column="value", window=2, func="max",
                group_cols=["group", "sub"], order_by="id",
            )
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_max_w2_by_group_sub")

        check = res_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group", "sub"], "value", 2, "max")
        assert_allclose_numeric(check[out_col], exp_vals)
        pandas_df = exp_ordered.copy()
        pandas_df[out_col] = np.asarray(exp_vals).reshape(-1)
        self._record_result(
            "direct_multicolumn_rolling",
            'arolling("value",2,"max",group_cols=["group","sub"],order_by="id")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
        )

    # --- map_feature=True: single and multi-column group-by ---
    def test_map_feature_single_groupby(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg(
            "value", ["mean"], map_feature=True
        )
        assert result["mapped_columns"] == ["value_rolling_mean_w2_by_group"]

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_mean_w2_by_group" in list(mapped.columns)
        assert "group" in list(mapped.columns)  # keys not duplicated

        check = mapped.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean")
        assert_allclose_numeric(check["value_rolling_mean_w2_by_group"], exp_vals)
        self._record_result(
            "map_feature_single_groupby",
            'groupby("group").rolling(2).agg("value", ["mean"], map_feature=True)',
            sample_df, mapped, exp_ordered, backend_config["connection_type"],
        )

    def test_map_feature_multi_groupby(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").rolling(2, order_by="id").agg(
            "value", ["sum"], map_feature=True
        )
        assert result["mapped_columns"] == ["value_rolling_sum_w2_by_group_sub"]

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_sum_w2_by_group_sub" in list(mapped.columns)

        check = mapped.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "sub", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group", "sub"], "value", 2, "sum")
        assert_allclose_numeric(check["value_rolling_sum_w2_by_group_sub"], exp_vals)
        self._record_result(
            "map_feature_multi_groupby",
            'groupby("group", "sub").rolling(2).agg("value", ["sum"], map_feature=True)',
            sample_df, mapped, exp_ordered, backend_config["connection_type"],
        )

    def test_map_feature_expanding(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(order_by="id").agg(
            "value", ["sum"], map_feature=True
        )
        assert result["mapped_columns"] == ["value_expanding_sum_by_group"]

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        check = mapped.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby("group")["value"].expanding().sum().reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(check["value_expanding_sum_by_group"], exp_vals)
        self._record_result(
            "map_feature_expanding",
            'groupby("group").expanding().agg("value", ["sum"], map_feature=True)',
            sample_df, mapped, exp_ordered, backend_config["connection_type"],
        )

    def test_map_feature_false_leaves_original_untouched(
        self, uploaded_ctx, sample_df, backend_config
    ):
        before = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg(
            "value", ["mean"], map_feature=False
        )

        assert result["is_error"] is False
        assert result["mapped_table"] is None
        assert result["mapped_columns"] == []

        after = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        pd.testing.assert_frame_equal(
            normalize_frame(after),
            normalize_frame(before),
            check_dtype=False,
        )
        self._record_result(
            "map_feature_false_leaves_original_untouched",
            'groupby("group").rolling(2).agg("value", ["mean"], map_feature=False)',
            sample_df, after, before, backend_config["connection_type"],
        )

    def test_map_feature_true_then_false(
        self, uploaded_ctx, sample_df, backend_config
    ):
        first = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg(
            "value", ["mean"], map_feature=True
        )
        assert first["is_error"] is False

        second = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg(
            "value", ["mean"], map_feature=False
        )
        assert second["is_error"] is False

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_mean_w2_by_group" in list(mapped.columns)

        check = mapped.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = self._pandas_rolling(exp_ordered, ["group"], "value", 2, "mean")
        assert_allclose_numeric(check["value_rolling_mean_w2_by_group"], exp_vals)
        self._record_result(
            "map_feature_true_then_false",
            'rolling agg map_feature=True then map_feature=False',
            sample_df, mapped, exp_ordered, backend_config["connection_type"],
        )

    # --- Mutation safety ---
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        original_uploaded = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        uploaded_ctx.groupby("group").rolling(2, order_by="id").mean("value")
        after_op = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        pd.testing.assert_frame_equal(
            normalize_frame(after_op),
            normalize_frame(original_uploaded),
            check_dtype=False,
        )
        self._record_result(
            "mutation_safety",
            '.groupby("group").rolling(2).mean("value") → original checked',
            sample_df, after_op, original_uploaded, backend_config["connection_type"],
        )
