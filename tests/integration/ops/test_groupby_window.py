# tests/test_groupby_window.py

import os
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np
import pytest

from matplotlib.backends.backend_pdf import PdfPages

from memframe.main import MemFrame
from tests.db_test_utils import default_duckdb_test_path
from pdf_report import render_df_to_pdf_page

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

    db_path = params.get("db_path", default_duckdb_test_path("integration"))
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

    # ----------------------------------------------------------------
    # Helpers: pandas mirrors (aligned back to the original row order)
    # ----------------------------------------------------------------
    @staticmethod
    def _ordered(frame: pd.DataFrame, order_by):
        """Sort only by ``order_by``; without it the engine uses upload row order."""
        if not order_by:
            return frame
        return frame.sort_values(order_by, kind="mergesort")

    @staticmethod
    def _aligned(result, index):
        """Drop the group levels pandas adds and restore the original row order."""
        return result.droplevel(list(range(result.index.nlevels - 1))).reindex(index)

    @classmethod
    def _pandas_rolling(cls, frame, keys, col: str, window: int, func: str,
                        order_by=None):
        # Mirrors the engine default: min_periods == window.
        rolled = (
            cls._ordered(frame, order_by)
            .groupby(list(keys), sort=False)[col]
            .rolling(window)
        )
        return cls._aligned(getattr(rolled, func)(), frame.index)

    # --- Direct rolling ---
    def test_direct_rolling_mean(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        result = asyncio.run(
            uploaded_ctx.groupby("group").rolling(2, order_by="id").amean("value")
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean", "id")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "direct_rolling_mean",
            'await ctx.groupby("group").rolling(2, order_by="id").amean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)',
        )

    def test_direct_rolling_sum(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        result = asyncio.run(
            uploaded_ctx.groupby("group").rolling(3, order_by="id").asum("value")
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w3_by_group")

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 3, "sum", "id")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "direct_rolling_sum",
            'await ctx.groupby("group").rolling(3, order_by="id").asum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(3).sum().droplevel(0).reindex(df.index)',
        )

    # --- GroupBy builder: rolling ---
    def test_groupby_rolling_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean", "id")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_rolling_mean",
            'dataset.groupby("group").rolling(2, order_by="id").mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)',
        )

    @pytest.mark.parametrize("closed", ["right", "left", "both", "neither"])
    def test_groupby_rolling_center_closed(
        self, uploaded_ctx, sample_df, backend_config, closed
    ):
        result = uploaded_ctx.groupby("group").rolling(
            3, order_by="id", min_periods=1, center=True, closed=closed
        ).mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w3_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby("group", sort=False)["value"]
            .rolling(3, min_periods=1, center=True, closed=closed)
            .mean()
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(check[out_col], exp_vals)

    def test_groupby_rolling_time_window_first_last(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 4 + ["b"] * 3,
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "d": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-02", "2024-01-03",
                        "2024-01-04", "2024-01-01", "2024-01-03",
                        "2024-01-05",
                    ]
                ),
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-02", "2024-01-03",
                        "2024-01-04", "2024-01-01", "2024-01-03",
                        "2024-01-05",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_fl")
        result = context.groupby("g").rolling(
            "3D", order_by="t"
        ).last("d")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "d_rolling_last_w259200s_by_g")

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        gts = ordered["t"].to_numpy()
        gds = ordered["d"].to_numpy()
        gids = ordered["g"].to_numpy()
        expected = []
        for group in pd.unique(gids):
            idxs = np.flatnonzero(gids == group)
            for j, pos in enumerate(idxs):
                mask = (
                    gts[idxs] > gts[pos] - np.timedelta64(3, "D")
                ) & (gts[idxs] <= gts[pos])
                expected.append(gds[idxs[np.flatnonzero(mask)[-1]]])
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        np.testing.assert_array_equal(
            pd.to_datetime(check[out_col]).to_numpy(),
            pd.to_datetime(expected).to_numpy(),
        )

    def test_groupby_rolling_quantile_interpolation(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 4 + ["b"] * 4,
                "x": [1.0, 2.0, 4.0, 8.0, 10.0, 20.0, 40.0, 80.0],
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-02", "2024-01-03",
                        "2024-01-05", "2024-01-01", "2024-01-02",
                        "2024-01-04", "2024-01-06",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_qi")
        result = context.groupby("g").rolling(
            "3D", order_by="t"
        ).quantile("x", q=0.75, interpolation="nearest")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "x_rolling_q0.75_w259200s_by_g")

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        expected = pd.concat(
            [
                grp.set_index("t", drop=False).rolling("3D")["x"].quantile(
                    0.75, interpolation="nearest"
                )
                for _, grp in ordered.groupby("g", sort=False)
            ]
        )
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        assert_allclose_numeric(check[out_col], expected)

    def test_groupby_rolling_skew_time_window(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 5 + ["b"] * 4,
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0],
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-02", "2024-01-03",
                        "2024-01-04", "2024-01-05", "2024-01-01",
                        "2024-01-02", "2024-01-04", "2024-01-06",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_skew")
        result = context.groupby("g").rolling(
            "3D", order_by="t"
        ).skew("x")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "x_rolling_skew_w259200s_by_g")

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        expected = pd.concat(
            [
                grp.set_index("t", drop=False).rolling("3D")["x"].skew()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        )
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        np.testing.assert_allclose(
            pd.to_numeric(check[out_col], errors="coerce"),
            pd.to_numeric(expected, errors="coerce"),
            rtol=1e-6, atol=1e-9, equal_nan=True,
        )

    def test_groupby_rolling_centered_time_window(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 4 + ["b"] * 3,
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-02", "2024-01-03",
                        "2024-01-05", "2024-01-01", "2024-01-03",
                        "2024-01-05",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_center")
        result = context.groupby("g").rolling(
            "2D", order_by="t", center=True
        ).sum("x")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "x_rolling_sum_w172800s_by_g")

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        expected = pd.concat(
            [
                grp.rolling("2D", on="t", center=True)["x"].sum()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        )
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        assert_allclose_numeric(check[out_col], expected)

    def test_groupby_rolling_time_window_quantile(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 4 + ["b"] * 3,
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-01", "2024-01-03",
                        "2024-01-04", "2024-01-04", "2024-01-06",
                        "2024-01-10",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_timeq")
        result = context.groupby("g").rolling(
            "3D", order_by="t"
        ).quantile("x", q=0.5)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "x_rolling_q0.5_w259200s_by_g")

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        expected = pd.concat(
            [
                grp.set_index("t", drop=False).rolling("3D")["x"].quantile(0.5)
                for _, grp in ordered.groupby("g", sort=False)
            ]
        )
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        assert_allclose_numeric(check[out_col], expected)

    def test_groupby_rolling_time_window(
        self, connected_memframe, backend_config
    ):
        frame = pd.DataFrame(
            {
                "g": ["a"] * 4 + ["b"] * 3,
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "t": pd.to_datetime(
                    [
                        "2024-01-01", "2024-01-01", "2024-01-03",
                        "2024-01-04", "2024-01-04", "2024-01-06",
                        "2024-01-10",
                    ]
                ),
            }
        )
        context = connected_memframe.upload_df(frame, filename="gb_time")
        result = context.groupby("g").rolling(
            "3D", order_by="t"
        ).mean("x")
        res_df = get_result_df(result)
        out_col = get_generated_col(
            result, "x_rolling_mean_w259200s_by_g"
        )

        ordered = frame.sort_values(["g", "t"], kind="mergesort")
        expected = pd.concat(
            [
                grp.rolling("3D", on="t")["x"].mean()
                for _, grp in ordered.groupby("g", sort=False)
            ]
        )
        check = res_df.sort_values(["g", "t"]).reset_index(drop=True)
        assert_allclose_numeric(check[out_col], expected)

    def test_groupby_rolling_weighted_mean(
        self, uploaded_ctx, sample_df, backend_config
    ):
        result = uploaded_ctx.groupby("group").rolling(
            3, order_by="id", min_periods=1, win_type="hann"
        ).mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w3_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        expected = pd.concat(
            [
                grp["value"].rolling(3, min_periods=1, win_type="hann").mean()
                for _, grp in exp_ordered.groupby("group", sort=False)
            ]
        )
        assert_allclose_numeric(check[out_col], expected)

    def test_groupby_rolling_quantile_centered(
        self, uploaded_ctx, sample_df, backend_config
    ):
        result = uploaded_ctx.groupby("group").rolling(
            3, order_by="id", min_periods=1, center=True
        ).quantile("value", q=0.5)
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_q0.5_w3_by_group")

        check = res_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_vals = (
            exp_ordered.groupby("group", sort=False)["value"]
            .rolling(3, min_periods=1, center=True)
            .quantile(0.5)
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(check[out_col], exp_vals)

    def test_groupby_rolling_sum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(3, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w3_by_group")

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 3, "sum", "id")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_rolling_sum",
            'dataset.groupby("group").rolling(3, order_by="id").sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(3).sum().droplevel(0).reindex(df.index)',
        )

    def test_groupby_rolling_max(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").max("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_max_w2_by_group")

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 2, "max", "id")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_rolling_max",
            'dataset.groupby("group").rolling(2, order_by="id").max("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).max().droplevel(0).reindex(df.index)',
        )

    def test_groupby_rolling_no_order(
        self, connected_memframe, sample_df, backend_config
    ):
        # physical order must differ from id order, or this test proves nothing
        shuffled = sample_df.sample(frac=1, random_state=0).reset_index(drop=True)
        ctx = connected_memframe.upload_df(shuffled, filename="gb_window_no_order")
        result = ctx.groupby("group").rolling(2).mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_mean_w2_by_group")

        assert out_col in list(res_df.columns)
        assert len(res_df) == len(shuffled)
        exp_vals = self._pandas_rolling(shuffled, ["group"], "value", 2, "mean")
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = shuffled.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_rolling_no_order",
            'dataset.groupby("group").rolling(2).mean("value")  # no order_by: upload row order',
            shuffled, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)  # no sort: upload row order',
        )

    # --- Expanding ---
    def test_groupby_expanding_mean(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(min_periods=1, order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_mean_by_group")

        exp_vals = self._aligned(
            self._ordered(sample_df, "id")
            .groupby("group")["value"].expanding().mean(),
            sample_df.index,
        )
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_expanding_mean",
            'dataset.groupby("group").expanding(min_periods=1, order_by="id").mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].expanding().mean().droplevel(0).reindex(df.index)',
        )

    def test_groupby_expanding_sum(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(min_periods=1, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_sum_by_group")

        exp_vals = self._aligned(
            self._ordered(sample_df, "id")
            .groupby("group")["value"].expanding().sum(),
            sample_df.index,
        )
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_expanding_sum",
            'dataset.groupby("group").expanding(min_periods=1, order_by="id").sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].expanding().sum().droplevel(0).reindex(df.index)',
        )

    # --- EWM ---
    def test_groupby_ewm_source_writeback_and_cache(
        self, uploaded_ctx, sample_df, backend_config
    ):
        first = uploaded_ctx.groupby("group").ewm(span=3, order_by="id").mean("value")
        second = uploaded_ctx.groupby("group").ewm(span=3, order_by="id").mean("value")

        assert first["is_error"] is False
        assert second.get("result_metadata", {}).get("from_cache") is True

        generated = get_generated_col(first, "value_ewm_mean_by_group")
        assert generated in uploaded_ctx.dtypes
        first_df = get_result_df(first)
        second_df = get_result_df(second)
        assert list(first_df.columns) == ["value", "group", "id", generated]
        assert first_df.equals(second_df)

        self._record_result(
            "groupby_ewm",
            'dataset.groupby("group").ewm(span=3, order_by="id").mean("value")',
            sample_df, first_df, second_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].ewm(span=3).mean().droplevel(0).reindex(df.index)',
        )

    def test_groupby_ewm_adjust_false_matches_pandas(
        self, uploaded_ctx, sample_df, backend_config
    ):
        result = uploaded_ctx.groupby("group").ewm(
            span=2, order_by="id", adjust=False
        ).agg("value", ["mean", "var"])
        assert result["is_error"] is False
        res_df = get_result_df(result).sort_values(["group", "id"]).reset_index(drop=True)
        exp_ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        exp_mean = (
            exp_ordered.groupby("group")["value"]
            .ewm(span=2, adjust=False).mean()
            .reset_index(level=0, drop=True)
        )
        exp_var = (
            exp_ordered.groupby("group")["value"]
            .ewm(span=2, adjust=False).var()
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(res_df["value_ewm_mean_by_group"], exp_mean)
        assert_allclose_numeric(res_df["value_ewm_var_by_group"], exp_var)
        first_rows = res_df.groupby("group", sort=False).head(1)
        assert first_rows["value_ewm_var_by_group"].isna().all()

    def test_groupby_ewm_no_order(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").ewm(span=2).mean("value")
        assert result["is_error"] is False
        res_df = get_result_df(result)
        assert "value_ewm_mean_by_group" in res_df.columns
        assert len(res_df) == len(sample_df)

    def test_groupby_rolling_specials(self, uploaded_ctx, sample_df, backend_config):
        ordered = sample_df.sort_values(["group", "id"]).reset_index(drop=True)
        grouped = ordered.groupby("group", sort=False)["value"]

        q = uploaded_ctx.groupby("group").rolling(3, order_by="id").quantile(
            "value", q=0.5
        )
        got_q = get_result_df(q).sort_values(["group", "id"]).reset_index(drop=True)
        exp_q = (
            grouped.rolling(3).quantile(0.5)
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(got_q["value_rolling_q0.5_w3_by_group"], exp_q)

        nun = uploaded_ctx.groupby("group").rolling(3, order_by="id").nunique("value")
        got_nun = get_result_df(nun).sort_values(["group", "id"]).reset_index(drop=True)
        exp_nun = (
            grouped.rolling(3)
            .apply(lambda values: values.nunique(), raw=False)
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(got_nun["value_rolling_nunique_w3_by_group"], exp_nun)

        eq = uploaded_ctx.groupby("group").expanding(order_by="id").quantile(
            "value", q=0.5
        )
        got_eq = get_result_df(eq).sort_values(["group", "id"]).reset_index(drop=True)
        exp_eq = (
            grouped.expanding(min_periods=1).quantile(0.5)
            .reset_index(level=0, drop=True)
        )
        assert_allclose_numeric(got_eq["value_expanding_q0.5_by_group"], exp_eq)

    # --- Aggregation ---
    def test_groupby_rolling_agg(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg("value", ["mean", "sum"])
        res_df = get_result_df(result)

        exp_mean = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean", "id")
        exp_sum = self._pandas_rolling(sample_df, ["group"], "value", 2, "sum", "id")
        assert_allclose_numeric(res_df["value_rolling_mean_w2_by_group"], exp_mean)
        assert_allclose_numeric(res_df["value_rolling_sum_w2_by_group"], exp_sum)
        pandas_df = sample_df.copy()
        pandas_df["value_rolling_mean_w2_by_group"] = exp_mean.to_numpy()
        pandas_df["value_rolling_sum_w2_by_group"] = exp_sum.to_numpy()
        self._record_result(
            "groupby_rolling_agg",
            'dataset.groupby("group").rolling(2, order_by="id").agg("value",["mean","sum"])',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).agg(["mean", "sum"]).droplevel(0).reindex(df.index)',
        )

    # --- Multicolumn group-by ---
    def test_groupby_multicolumn_rolling(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").rolling(2, order_by="id").sum("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_sum_w2_by_group_sub")

        exp_vals = self._pandas_rolling(
            sample_df, ["group", "sub"], "value", 2, "sum", "id"
        )
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_multicolumn_rolling",
            'dataset.groupby("group", "sub").rolling(2, order_by="id").sum("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby(["group", "sub"])["value"].rolling(2).sum().droplevel([0, 1]).reindex(df.index)',
        )

    def test_groupby_multicolumn_expanding(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").expanding(order_by="id").mean("value")
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_expanding_mean_by_group_sub")

        exp_vals = self._aligned(
            self._ordered(sample_df, "id")
            .groupby(["group", "sub"])["value"].expanding().mean(),
            sample_df.index,
        )
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "groupby_multicolumn_expanding",
            'dataset.groupby("group", "sub").expanding(order_by="id").mean("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby(["group", "sub"])["value"].expanding().mean().droplevel([0, 1]).reindex(df.index)',
        )

    def test_direct_multicolumn_rolling(self, uploaded_ctx, sample_df, backend_config):
        import asyncio

        result = asyncio.run(
            uploaded_ctx.groupby("group", "sub").rolling(2, order_by="id").amax("value")
        )
        res_df = get_result_df(result)
        out_col = get_generated_col(result, "value_rolling_max_w2_by_group_sub")

        exp_vals = self._pandas_rolling(
            sample_df, ["group", "sub"], "value", 2, "max", "id"
        )
        assert_allclose_numeric(res_df[out_col], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df[out_col] = exp_vals.to_numpy()
        self._record_result(
            "direct_multicolumn_rolling",
            'await ctx.groupby("group", "sub").rolling(2, order_by="id").amax("value")',
            sample_df, res_df, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby(["group", "sub"])["value"].rolling(2).max().droplevel([0, 1]).reindex(df.index)',
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

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean", "id")
        assert_allclose_numeric(mapped["value_rolling_mean_w2_by_group"], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df["value_rolling_mean_w2_by_group"] = exp_vals.to_numpy()
        self._record_result(
            "map_feature_single_groupby",
            'dataset.groupby("group").rolling(2, order_by="id")'
            '.agg("value", ["mean"], map_feature=True)',
            sample_df, mapped, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)  # mapped back',
        )

    def test_map_feature_multi_groupby(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group", "sub").rolling(2, order_by="id").agg(
            "value", ["sum"], map_feature=True
        )
        assert result["mapped_columns"] == ["value_rolling_sum_w2_by_group_sub"]

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_sum_w2_by_group_sub" in list(mapped.columns)

        exp_vals = self._pandas_rolling(
            sample_df, ["group", "sub"], "value", 2, "sum", "id"
        )
        assert_allclose_numeric(mapped["value_rolling_sum_w2_by_group_sub"], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df["value_rolling_sum_w2_by_group_sub"] = exp_vals.to_numpy()
        self._record_result(
            "map_feature_multi_groupby",
            'dataset.groupby("group", "sub").rolling(2, order_by="id")'
            '.agg("value", ["sum"], map_feature=True)',
            sample_df, mapped, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby(["group", "sub"])["value"].rolling(2).sum().droplevel([0, 1]).reindex(df.index)  # mapped back',
        )

    def test_map_feature_expanding(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.groupby("group").expanding(order_by="id").agg(
            "value", ["sum"], map_feature=True
        )
        assert result["mapped_columns"] == ["value_expanding_sum_by_group"]

        mapped = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        exp_vals = self._aligned(
            self._ordered(sample_df, "id")
            .groupby("group")["value"].expanding().sum(),
            sample_df.index,
        )
        assert_allclose_numeric(mapped["value_expanding_sum_by_group"], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df["value_expanding_sum_by_group"] = exp_vals.to_numpy()
        self._record_result(
            "map_feature_expanding",
            'dataset.groupby("group").expanding(order_by="id")'
            '.agg("value", ["sum"], map_feature=True)',
            sample_df, mapped, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].expanding().sum().droplevel(0).reindex(df.index)  # mapped back',
        )

    def test_default_writes_generated_column_to_source(
        self, uploaded_ctx, sample_df, backend_config
    ):
        before = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        result = uploaded_ctx.groupby("group").rolling(2, order_by="id").agg(
            "value", ["mean"], map_feature=False
        )

        assert result["is_error"] is False
        assert result["mapped_table"] is not None
        assert result["mapped_columns"] == ["value_rolling_mean_w2_by_group"]
        assert list(result["result"].columns) == [
            "value", "group", "id", "value_rolling_mean_w2_by_group",
        ]

        after = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_mean_w2_by_group" in after.columns
        key = [c for c in ("group", "id") if c in before.columns]
        after_sorted = after.sort_values(key).reset_index(drop=True)[before.columns]
        before_sorted = before.sort_values(key).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            normalize_frame(after_sorted),
            normalize_frame(before_sorted),
            check_dtype=False,
        )
        self._record_result(
            "default_writes_generated_column_to_source",
            'dataset.groupby("group").rolling(2, order_by="id")'
            '.agg("value", ["mean"])',
            sample_df, after, before, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)',
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

        exp_vals = self._pandas_rolling(sample_df, ["group"], "value", 2, "mean", "id")
        assert_allclose_numeric(mapped["value_rolling_mean_w2_by_group"], exp_vals)
        pandas_df = sample_df.copy()
        pandas_df["value_rolling_mean_w2_by_group"] = exp_vals.to_numpy()
        self._record_result(
            "map_feature_true_then_false",
            'dataset.groupby("group").rolling(2, order_by="id")'
            '.agg("value", ["mean"])  # map_feature=True then False',
            sample_df, mapped, pandas_df, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)  # rerun drops and re-adds',
        )

    # --- Mutation safety ---
    def test_source_gains_generated_column(self, uploaded_ctx, sample_df, backend_config):
        original_uploaded = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        uploaded_ctx.groupby("group").rolling(2, order_by="id").mean("value")
        after_op = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "value_rolling_mean_w2_by_group" in after_op.columns
        key = [c for c in ("group", "id") if c in original_uploaded.columns]
        after_sorted = after_op.sort_values(key).reset_index(drop=True)[original_uploaded.columns]
        before_sorted = original_uploaded.sort_values(key).reset_index(drop=True)
        pd.testing.assert_frame_equal(
            normalize_frame(after_sorted),
            normalize_frame(before_sorted),
            check_dtype=False,
        )
        self._record_result(
            "source_gains_generated_column",
            'dataset.groupby("group").rolling(2, order_by="id").mean("value")',
            sample_df, after_op, original_uploaded, backend_config["connection_type"],
            pandas_call='df.sort_values("id", kind="mergesort").groupby("group")["value"].rolling(2).mean().droplevel(0).reindex(df.index)',
        )
