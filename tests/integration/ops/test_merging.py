# tests/integration/ops/test_merging.py

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
    return pytest.UsageError(f"Invalid merging DB configuration: {message}")


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
def left_df() -> pd.DataFrame:
    """Left DataFrame (employees) for merge/join tests."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4],
        "employee": ["Ava", "Ben", "Cara", "Dan"],
        "department": ["Sales", "Engineering", "Engineering", "Support"],
        "salary": [52000, 91000, 88000, 61000],
    })


@pytest.fixture(scope="function")
def right_df() -> pd.DataFrame:
    """Right DataFrame (payroll) for merge/join tests (some ids missing)."""
    return pd.DataFrame({
        "id": [2, 3, 4, 5],
        "bank": ["North", "North", "South", "West"],
        "bonus": [4000, 7500, 6000, 3500],
        "tax": [9000, 14000, 13000, 8000],
    })


@pytest.fixture(scope="function")
def concat_df1() -> pd.DataFrame:
    """First DataFrame for concat tests."""
    return pd.DataFrame({
        "A": [1, 2, 3],
        "B": ["x", "y", "z"],
    })


@pytest.fixture(scope="function")
def concat_df2() -> pd.DataFrame:
    """Second DataFrame for concat tests (same columns)."""
    return pd.DataFrame({
        "A": [4, 5, 6],
        "B": ["u", "v", "w"],
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
def left_ctx(connected_memframe, left_df) -> Any:
    """Upload left DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(left_df, filename="left_dataset")


@pytest.fixture(scope="function")
def right_ctx(connected_memframe, right_df) -> Any:
    """Upload right DataFrame and return a ContextManager."""
    return connected_memframe.upload_df(right_df, filename="right_dataset")


@pytest.fixture(scope="function")
def concat_ctx1(connected_memframe, concat_df1) -> Any:
    """Upload first concat DataFrame."""
    return connected_memframe.upload_df(concat_df1, filename="concat_df1")


@pytest.fixture(scope="function")
def concat_ctx2(connected_memframe, concat_df2) -> Any:
    """Upload second concat DataFrame."""
    return connected_memframe.upload_df(concat_df2, filename="concat_df2")


# ----------------------------------------------------------------------
# Helpers (same as in test_inspect.py)
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
    """Drop helper columns and normalize index for stable DataFrame comparisons."""
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)


def sort_by_available_id(df: pd.DataFrame) -> pd.DataFrame:
    """Sort using an available id-like column for stable comparisons."""
    for key in ("id", "id_x", "id_L", "id_y", "id_R"):
        if key in df.columns:
            return df.sort_values(key).reset_index(drop=True)
    return df.reset_index(drop=True)


def expected_merge_with_dual_keys(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    how: str,
    left_key_name: str,
    right_key_name: str,
) -> pd.DataFrame:
    """Build pandas expected output with two key columns (backend-style)."""
    left_renamed = left_df.rename(columns={"id": left_key_name})
    right_renamed = right_df.rename(columns={"id": right_key_name})
    return pd.merge(
        left_renamed,
        right_renamed,
        left_on=left_key_name,
        right_on=right_key_name,
        how=how,
    )


# ----------------------------------------------------------------------
# PDF generation helper (optional, kept for consistency)
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
    original_dfs,
    memframe_df,
    pandas_df,
    backend,
    status="PASSED",
    error_message="",
):
    """Create a single PDF page with method call + inputs + MemFrame/Pandas snapshots.

    ``original_dfs`` is the list of input DataFrames: one for single-table ops,
    two (Left Table / Right Table) for merge/join/concat.
    """
    sections = []
    if len(original_dfs) == 1:
        sections.append(("Original", original_dfs[0].head(10)))
    else:
        labels = ["Left Table", "Right Table", "Table 3", "Table 4"]
        for label, df in zip(labels, original_dfs):
            sections.append((label, df.head(10)))
    sections.append(("MemFrame Result", memframe_df.head(10)))
    sections.append(("Pandas Result", pandas_df.head(10)))

    fig_height = max(8, 2 + sum(max(2, len(df) + 2) for _, df in sections) * 0.4)
    fig, axes = plt.subplots(len(sections), 1, figsize=(16, fig_height))
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
class TestMergingOperations:
    """All merge/join/concat tests that require a backend connection."""

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
            pdf_path = RESULT_DIR / f"test_merging_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for result in cls._saved_results:
                    render_df_to_pdf_page(
                        pdf,
                        result["test_name"],
                        result["method_call"],
                        result["original_dfs"],
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
            frame_locals.get("left_df", frame_locals.get("concat_df1")),
            "Original DataFrame was not available when this test failed",
        )
        original_df2_value = frame_locals.get("right_df", frame_locals.get("concat_df2"))
        original_df2 = (
            _coerce_pdf_df(
                original_df2_value,
                "Second original DataFrame was not available when this test failed",
            )
            if original_df2_value is not None
            else None
        )

        memframe_value = None
        for name in ("res_df", "add_df", "left_orig", "right_orig", "merged_ctx", "result", "add_result"):
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
            original_df2=original_df2,
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
        original_df2: pd.DataFrame = None,
        status: str = "PENDING",
        error_message: str = "",
    ):
        """Store test result for PDF generation."""
        if self._save_to_file:
            original_dfs = [
                _prepare_pdf_df(_coerce_pdf_df(original_df, "No original data"))
            ]
            if original_df2 is not None:
                original_dfs.append(
                    _prepare_pdf_df(
                        _coerce_pdf_df(original_df2, "No second original data")
                    )
                )
            result = {
                "test_name": test_name,
                "method_call": method_call,
                "original_dfs": original_dfs,
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
    # Merge tests
    # ------------------------------------------------------------------
    def test_merge_inner(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Inner merge on 'id'."""
        result = left_ctx.merge(right_ctx, on="id", how="inner")
        res_df = get_result_df(result)
        expected = expected_merge_with_dual_keys(left_df, right_df, "inner", "id_x", "id_y")
        res_sorted = sort_by_available_id(res_df)
        exp_sorted = sort_by_available_id(expected)
        pd.testing.assert_frame_equal(
            normalize_frame(res_sorted),
            normalize_frame(exp_sorted),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="merge_inner",
            method_call='left_ctx.merge(right_ctx, on="id", how="inner")',
            original_df=left_df,
            original_df2=right_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_merge_left(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Left join merge."""
        result = left_ctx.merge(right_ctx, on="id", how="left")
        res_df = get_result_df(result)
        expected = expected_merge_with_dual_keys(left_df, right_df, "left", "id_x", "id_y")
        # Fill NaN for string columns with '' to match library's handling
        for col in expected.columns:
            if expected[col].dtype == object and col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        res_sorted = sort_by_available_id(res_df)
        exp_sorted = sort_by_available_id(expected)
        pd.testing.assert_frame_equal(
            normalize_frame(res_sorted),
            normalize_frame(exp_sorted),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="merge_left",
            method_call='left_ctx.merge(right_ctx, on="id", how="left")',
            original_df=left_df,
            original_df2=right_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_merge_right(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Right join merge."""
        result = left_ctx.merge(right_ctx, on="id", how="right")
        res_df = get_result_df(result)
        expected = expected_merge_with_dual_keys(left_df, right_df, "right", "id_x", "id_y")
        # Fill string nulls
        for col in expected.columns:
            if expected[col].dtype == object and col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        res_sorted = sort_by_available_id(res_df)
        exp_sorted = sort_by_available_id(expected)
        pd.testing.assert_frame_equal(
            normalize_frame(res_sorted),
            normalize_frame(exp_sorted),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="merge_right",
            method_call='left_ctx.merge(right_ctx, on="id", how="right")',
            original_df=left_df,
            original_df2=right_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_merge_outer(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Outer join merge."""
        result = left_ctx.merge(right_ctx, on="id", how="outer")
        res_df = get_result_df(result)
        expected = expected_merge_with_dual_keys(left_df, right_df, "outer", "id_x", "id_y")
        for col in expected.columns:
            if expected[col].dtype == object and col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        res_sorted = sort_by_available_id(res_df)
        exp_sorted = sort_by_available_id(expected)
        pd.testing.assert_frame_equal(
            normalize_frame(res_sorted),
            normalize_frame(exp_sorted),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="merge_outer",
            method_call='left_ctx.merge(right_ctx, on="id", how="outer")',
            original_df=left_df,
            original_df2=right_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    def test_merge_direct_call(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """ContextManager is not callable; direct call should raise TypeError."""
        with pytest.raises(TypeError):
            left_ctx(right_ctx, on="id", how="inner")

    # ------------------------------------------------------------------
    # Join test
    # ------------------------------------------------------------------
    def test_join(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Join with 'id' as index-like column (should behave like merge with suffixes)."""
        result = left_ctx.join(right_ctx, on="id", how="left", lsuffix="_L", rsuffix="_R")
        res_df = get_result_df(result)
        expected = expected_merge_with_dual_keys(left_df, right_df, "left", "id_L", "id_R")
        for col in expected.columns:
            if expected[col].dtype == object and col in res_df.columns:
                res_df[col] = res_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        res_sorted = sort_by_available_id(res_df)
        exp_sorted = sort_by_available_id(expected)
        pd.testing.assert_frame_equal(
            normalize_frame(res_sorted),
            normalize_frame(exp_sorted),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="join",
            method_call='left_ctx.join(right_ctx, on="id", how="left", lsuffix="_L", rsuffix="_R")',
            original_df=left_df,
            original_df2=right_df,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Concat tests
    # ------------------------------------------------------------------
    def test_concat_axis0(self, concat_ctx1, concat_ctx2, concat_df1, concat_df2, backend_config):
        """Vertical concatenation (axis=0) with identical columns."""
        result = concat_ctx1.concat([concat_ctx2], axis=0)
        res_df = get_result_df(result)
        expected = pd.concat([concat_df1, concat_df2], axis=0, ignore_index=True)
        pd.testing.assert_frame_equal(
            normalize_frame(res_df).reset_index(drop=True),
            normalize_frame(expected).reset_index(drop=True),
            check_dtype=False,
            check_names=False,
        )
        self._record_result(
            test_name="concat_axis0",
            method_call="concat_ctx1.concat([concat_ctx2], axis=0)",
            original_df=concat_df1,
            original_df2=concat_df2,
            memframe_df=res_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )

    # ------------------------------------------------------------------
    # Mutation safety and chaining
    # ------------------------------------------------------------------
    def test_mutation_safety(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Original DataFrames should remain unchanged after merge."""
        _ = left_ctx.merge(right_ctx, on="id", how="inner")
        left_orig = get_result_df(left_ctx.full_table())
        right_orig = get_result_df(right_ctx.full_table())
        pd.testing.assert_frame_equal(
            normalize_frame(left_orig),
            normalize_frame(left_df),
            check_dtype=False,
        )
        pd.testing.assert_frame_equal(
            normalize_frame(right_orig),
            normalize_frame(right_df),
            check_dtype=False,
        )
        self._record_result(
            test_name="mutation_safety",
            method_call="merge → check original left unchanged",
            original_df=left_df,
            original_df2=right_df,
            memframe_df=left_orig,
            pandas_df=left_df,
            backend=backend_config["connection_type"],
        )

    def test_chain_merge_and_op(self, left_ctx, right_ctx, left_df, right_df, backend_config):
        """Chain merge with an arithmetic operation (add constant)."""
        merged_ctx = left_ctx.merge(right_ctx, on="id", how="inner")
        res_df = get_result_df(merged_ctx)
        new_ctx = left_ctx.memframe.upload_df(res_df, filename="chain_merge_result")
        # Now perform an addition operation
        add_result = new_ctx.add("salary", "bonus", "total_comp")
        add_df = get_result_df(add_result)
        expected = pd.merge(left_df, right_df, left_on="id", right_on="id", how="inner", suffixes=("_x", "_y"))
        expected["total_comp"] = expected["salary"] + expected["bonus"]
        # Operation output keeps only arithmetic columns; align expected projection.
        expected = expected[[c for c in add_df.columns if c in expected.columns]]
        # Compare
        for col in expected.columns:
            if expected[col].dtype == object and col in add_df.columns:
                add_df[col] = add_df[col].fillna("")
                expected[col] = expected[col].fillna("")
        pd.testing.assert_frame_equal(
            normalize_frame(add_df).reset_index(drop=True),
            normalize_frame(expected).reset_index(drop=True),
            check_dtype=False,
        )
        self._record_result(
            test_name="chain_merge_and_op",
            method_call="merge → upload → add",
            original_df=left_df,
            original_df2=right_df,
            memframe_df=add_df,
            pandas_df=expected,
            backend=backend_config["connection_type"],
        )
