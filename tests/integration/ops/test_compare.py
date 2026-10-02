# tests/integration/ops/test_compare.py

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
    return pytest.UsageError(f"Invalid compare DB configuration: {message}")


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
# Fixtures
# ----------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_df() -> pd.DataFrame:
    """DataFrame with numeric, categorical, and datetime columns for comparison tests."""
    return pd.DataFrame({
        "id":       [1, 2, 3, 4, 5],
        "val_a":    [10, 20, 30, 40, 50],
        "val_b":    [10, 25, 25, 40, 55],
        "cat_a":    ["X", "Y", "Z", "W", None],
        "cat_b":    ["X", "Y", "Y", "W", "V"],
        "date_a":   pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01",
                                    "2023-04-01", None]),
        "date_b":   pd.to_datetime(["2023-01-01", "2023-02-15", "2023-02-28",
                                    "2023-04-01", "2023-05-01"]),
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
    ctx = connected_memframe.upload_df(sample_df, filename="compare_dataset")
    return ctx


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def get_result_df(result: Any) -> pd.DataFrame:
    """Extract a DataFrame from various result types."""
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
            raise AssertionError(result.get("error_message") or "Operation failed")
        if "result" in result and isinstance(result["result"], pd.DataFrame):
            return result["result"]
        if "data" in result and isinstance(result["data"], pd.DataFrame):
            return result["data"]
    raise AssertionError(f"Cannot extract DataFrame from {type(result)}: {result}")


def get_generated_col(result: Any, fallback: str) -> str:
    if isinstance(result, dict):
        cols = result.get("new_column") or result.get("generated_cols") or []
        if isinstance(cols, list) and cols:
            return cols[0]
        if isinstance(cols, str):
            return cols
    return fallback


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    helper_cols = [c for c in out.columns if str(c).startswith("__")]
    if helper_cols:
        out = out.drop(columns=helper_cols)
    return out.reset_index(drop=True)


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
class TestCompareOperations:
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
            pdf_path = RESULT_DIR / f"test_compare_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for rec in cls._saved_results:
                    render_df_to_pdf_page(
                        pdf,
                        rec["test_name"],
                        rec["method_call"],
                        rec["original_df"],
                        rec["memframe_df"],
                        rec["pandas_df"],
                        rec["backend"],
                        rec.get("status", "PASSED"),
                        rec.get("error_message", ""),
                        rec.get("pandas_call", ""),
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
        for rec in getattr(self, "_current_pdf_records", []):
            rec["status"] = status
            rec["error_message"] = error_message

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
        for name in ("res_df", "original", "result", "actual"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(
            memframe_value,
            "No MemFrame result was available when this test failed",
        )

        pandas_value = None
        for name in ("expected", "expected_series", "expected_col", "expected_vals"):
            if name in frame_locals:
                pandas_value = frame_locals[name]
                break
        pandas_df = _coerce_pdf_df(
            pandas_value,
            "No pandas expected result was available when this test failed",
        )

        backend_config = frame_locals.get("backend_config") or {}
        self._record_result(
            request.node.name,
            request.node.name,
            original_df,
            memframe_df,
            pandas_df,
            backend_config.get("connection_type", "unknown"),
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
        pandas_call="",
    ):
        if self._save_to_file:
            rec = {
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
            self._saved_results.append(rec)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(rec)

    # ------------------------------------------------------------------
    # Numeric comparisons (three-argument form)
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("op,suffix,expected_col", [
        ("==", "eq", [True, False, False, True, False]),
        ("!=", "ne", [False, True, True, False, True]),
        (">", "gt", [False, False, True, False, False]),
        ("<", "lt", [False, True, False, False, True]),
        (">=", "ge", [True, False, True, True, False]),
        ("<=", "le", [True, True, False, True, True]),
    ])
    def test_numeric_compare(self, uploaded_ctx, sample_df, backend_config, op, suffix, expected_col):
        result = uploaded_ctx.compare("val_a", "val_b", op)
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, f"cmp_val_a_{suffix}_val_b")
        # Actual values
        actual = res_df[new_col].astype(bool).tolist()
        assert actual == expected_col

        # Pandas equivalent
        pd_compare = {
            "==": lambda a,b: a == b,
            "!=": lambda a,b: a != b,
            ">":  lambda a,b: a > b,
            "<":  lambda a,b: a < b,
            ">=": lambda a,b: a >= b,
            "<=": lambda a,b: a <= b,
        }[op](sample_df["val_a"], sample_df["val_b"])
        expected = sample_df.copy()
        expected[new_col] = pd_compare
        self._record_result(
            f"numeric_{op}",
            f'compare("val_a", "val_b", "{op}")',
            sample_df,
            res_df,
            expected,
            backend_config["connection_type"],
            pandas_call=f'(sample_df["val_a"] {op} sample_df["val_b"])',
        )

    # ------------------------------------------------------------------
    # Categorical comparisons
    # ------------------------------------------------------------------
    def test_categorical_eq(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("cat_a", "cat_b", "==")
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, "cmp_cat_a_eq_cat_b")
        # Expected: (cat_a == cat_b) with None handling -> SQL gives NULL for any NULL operand
        expected_series = (sample_df["cat_a"] == sample_df["cat_b"]).astype(object)
        # SQL: NULL == something or NULL == NULL returns NULL, which becomes None in pandas
        # Convert None to pd.NA or NaN depending on dtype
        actual = res_df[new_col]
        for i in range(len(sample_df)):
            a = sample_df["cat_a"].iloc[i]
            b = sample_df["cat_b"].iloc[i]
            if a is None or b is None:
                assert pd.isna(actual.iloc[i]), f"Row {i} expected None"
            else:
                assert actual.iloc[i] == (a == b)
        # record
        expected = sample_df.copy()
        expected[new_col] = expected_series
        self._record_result("categorical_eq", 'compare("cat_a", "cat_b", "==")',
                            sample_df, res_df, expected, backend_config["connection_type"],
                            pandas_call='(sample_df["cat_a"] == sample_df["cat_b"])')

    def test_categorical_ne(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("cat_a", "cat_b", "!=")
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, "cmp_cat_a_ne_cat_b")
        actual = res_df[new_col]
        for i in range(len(sample_df)):
            a = sample_df["cat_a"].iloc[i]
            b = sample_df["cat_b"].iloc[i]
            if a is None or b is None:
                assert pd.isna(actual.iloc[i])
            else:
                assert actual.iloc[i] == (a != b)
        expected = sample_df.copy()
        expected[new_col] = (sample_df["cat_a"] != sample_df["cat_b"])
        self._record_result("categorical_ne", 'compare("cat_a", "cat_b", "!=")',
                            sample_df, res_df, expected, backend_config["connection_type"],
                            pandas_call='(sample_df["cat_a"] != sample_df["cat_b"])')

    # ------------------------------------------------------------------
    # Datetime comparisons
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("op,suffix,expected_col", [
        ("==", "eq", [True, False, False, True, None]),   # last row: NULL
        ("!=", "ne", [False, True, True, False, None]),
        (">", "gt", [False, False, True, False, None]),
        ("<", "lt", [False, True, False, False, None]),
        (">=", "ge", [True, False, True, True, None]),
        ("<=", "le", [True, True, False, True, None]),
    ])
    def test_datetime_compare(self, uploaded_ctx, sample_df, backend_config, op, suffix, expected_col):
        result = uploaded_ctx.compare("date_a", "date_b", op)
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, f"cmp_date_a_{suffix}_date_b")
        actual = res_df[new_col]
        for i, exp in enumerate(expected_col):
            if exp is None:
                assert pd.isna(actual.iloc[i]), f"Row {i} expected None"
            else:
                assert actual.iloc[i] == exp, f"Row {i} mismatch: expected {exp}, got {actual.iloc[i]}"
        # Pandas equivalent (for non-NULL rows)
        pd_compare = {
            "==": lambda a,b: a == b,
            "!=": lambda a,b: a != b,
            ">":  lambda a,b: a > b,
            "<":  lambda a,b: a < b,
            ">=": lambda a,b: a >= b,
            "<=": lambda a,b: a <= b,
        }[op](sample_df["date_a"], sample_df["date_b"])
        expected = sample_df.copy()
        expected[new_col] = pd_compare
        self._record_result(f"datetime_{op}", f'compare("date_a", "date_b", "{op}")',
                            sample_df, res_df, expected, backend_config["connection_type"],
                            pandas_call=f'(sample_df["date_a"] {op} sample_df["date_b"])')

    # ------------------------------------------------------------------
    # Expression parsing
    # ------------------------------------------------------------------
    def test_expression_numeric(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("val_a >= val_b")
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, "cmp_val_a_ge_val_b")
        expected = (sample_df["val_a"] >= sample_df["val_b"]).tolist()
        actual = res_df[new_col].astype(bool).tolist()
        assert actual == expected

    def test_expression_categorical(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("cat_a != cat_b")
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, "cmp_cat_a_ne_cat_b")
        actual = res_df[new_col]
        for i in range(len(sample_df)):
            a = sample_df["cat_a"].iloc[i]
            b = sample_df["cat_b"].iloc[i]
            if a is None or b is None:
                assert pd.isna(actual.iloc[i])
            else:
                assert actual.iloc[i] == (a != b)

    def test_expression_datetime(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("date_a < date_b")
        assert not result.get("is_error"), result.get("error_message")
        res_df = get_result_df(result)
        new_col = get_generated_col(result, "cmp_date_a_lt_date_b")
        # Row 0: 2023-01-01 < 2023-01-01? False
        # Row 1: 2023-02-01 < 2023-02-15 True
        # Row 2: 2023-03-01 < 2023-02-28 False
        # Row 3: 2023-04-01 < 2023-04-01 False
        # Row 4: NULL < something -> NULL
        expected_vals = [False, True, False, False, None]
        for i, exp in enumerate(expected_vals):
            if exp is None:
                assert pd.isna(res_df[new_col].iloc[i])
            else:
                assert res_df[new_col].iloc[i] == exp

    # ------------------------------------------------------------------
    # Error: invalid operator
    # ------------------------------------------------------------------
    def test_invalid_operator(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("val_a", "val_b", "?")
        assert result.get("is_error")
        assert "Invalid operator" in result["error_message"]

    # ------------------------------------------------------------------
    # Error: datatype mismatch
    # ------------------------------------------------------------------
    def test_dtype_mismatch(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.compare("val_a", "cat_a", "==")
        assert result.get("is_error")
        assert "Datatype mismatch" in result["error_message"] or "mismatch" in result["error_message"]

    # ------------------------------------------------------------------
    # Mutation safety
    # ------------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        # Original table should not contain the new comparison column
        original = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "cmp_val_a_ge_val_b" not in original.columns

        result = uploaded_ctx.compare("val_a >= val_b")
        res_df = get_result_df(result)
        assert "cmp_val_a_ge_val_b" in res_df.columns
        # Original unchanged
        after = get_result_df(uploaded_ctx.head(n=len(sample_df)))
        assert "cmp_val_a_ge_val_b" not in after.columns
