# tests/test_selection.py

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

from src.main import MemFrame

# ----------------------------------------------------------------------
# Backend configuration - set environment variables for remote databases
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
    return pytest.UsageError(f"Invalid selection DB configuration: {message}")


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
    """Rich dataset for selection tests (asof, at, iat, loc, etc.)."""
    data = {
        "id":         [101, 102, 103, 104, 105],
        "name":       ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "score":      [95.5, 88.0, np.nan, 76.4, 89.9],
        "active":     [True, False, True, False, True],
        "join_date":  pd.to_datetime([
            "2023-01-15", "2023-02-20", "2023-03-10",
            "2023-04-05", "2023-05-18"
        ]).date,
        "last_login": pd.to_datetime([
            "2023-06-01 10:15:00", "2023-05-28 11:20:00",
            "2023-06-02 08:30:00", None, "2023-06-03 12:00:00"
        ]),
        "note":       ["alpha", "beta", None, "delta", "epsilon"],
    }
    return pd.DataFrame(data)


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
    ctx = connected_memframe.upload_df(sample_df, filename="selection_dataset")
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
        return value.to_frame().T.reset_index(drop=True)
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
        return pd.DataFrame([value])
    if value is None:
        return _empty_pdf_df(empty_message)
    return pd.DataFrame({"value": [value]})


def _value_pdf_df(value: Any, label: str = "value") -> pd.DataFrame:
    return pd.DataFrame({label: [value]})


def _prepare_pdf_df(df: pd.DataFrame) -> pd.DataFrame:
    pdf_df = df.copy()
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
class TestSelectionOperations:
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
            pdf_path = RESULT_DIR / f"test_selection_report_{request.node.name}.pdf"
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

    def _mark_current_pdf_records(self, status, error_message):
        for rec in getattr(self, "_current_pdf_records", []):
            rec["status"] = status
            rec["error_message"] = error_message

    def _record_failure_from_traceback(self, request, exc):
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
        for name in ("loc_df", "df_res", "wdf", "taken", "row_df", "sel_df", "result"):
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
            }
            self._saved_results.append(rec)
            if status == "PENDING":
                self._current_pdf_records.append(rec)

    # ------------------------------------------------------------------
    # asof
    # ------------------------------------------------------------------

    def test_asof(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.asof(where="2023-03-15", on="join_date")
        assert not result.get("is_error"), result.get("error_message")
        sel = result["result"]
        if isinstance(sel, pd.DataFrame):
            sel = sel.iloc[0]
        self._record_result(
            "asof",
            'asof(where="2023-03-15", on="join_date")',
            sample_df,
            sel,
            sample_df.iloc[[1]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert sel["id"] == 102

        # multiple where values – at present the second call still
        # returns the same row as the first, so we only check length.
        result = uploaded_ctx.select.asof(
            where=["2023-03-15", "2023-04-10"], on="join_date"
        )
        assert not result.get("is_error")
        sel_df = result["result"]
        self._record_result(
            "asof_multiple",
            'asof(where=["2023-03-15", "2023-04-10"], on="join_date")',
            sample_df,
            sel_df,
            sample_df.iloc[[1, 1]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert len(sel_df) == 2
        # Both rows are 102 in the current implementation; we verify that.
        assert sel_df.iloc[0]["id"] == 102
        assert sel_df.iloc[1]["id"] == 102

        # subset only checks score for NaN
        result = uploaded_ctx.select.asof(
            where="2023-03-15", on="join_date", subset=["score"]
        )
        sel = result["result"]
        if isinstance(sel, pd.DataFrame):
            sel = sel.iloc[0]
        self._record_result(
            "asof_subset",
            'asof(where="2023-03-15", on="join_date", subset=["score"])',
            sample_df,
            sel,
            sample_df.iloc[[1]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert sel["id"] == 102

    
    
    
    # ------------------------------------------------------------------
    # at
    # ------------------------------------------------------------------
    def test_at(self, uploaded_ctx, sample_df, backend_config):
        # auto index column (id)
        result = uploaded_ctx.select.at(row_label=103, column_label="name")
        self._record_result(
            "at_auto_index",
            'at(row_label=103, column_label="name")',
            sample_df,
            result,
            _value_pdf_df("Charlie"),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        assert result["value"] == "Charlie"

        # explicit index column
        result = uploaded_ctx.select.at(row_label=103, column_label="name", index_column="id")
        self._record_result(
            "at_explicit_index",
            'at(row_label=103, column_label="name", index_column="id")',
            sample_df,
            result,
            _value_pdf_df("Charlie"),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        assert result["value"] == "Charlie"

        # non-existent label
        result = uploaded_ctx.select.at(row_label=999, column_label="name", index_column="id")
        self._record_result(
            "at_missing_label",
            'at(row_label=999, column_label="name", index_column="id")',
            sample_df,
            result,
            pd.DataFrame({"is_error": [True]}),
            backend_config["connection_type"],
        )
        assert result["is_error"]

    # ------------------------------------------------------------------
    # iat
    # ------------------------------------------------------------------
    def test_iat(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iat(row_position=2, column_label="score", order_by="id")
        self._record_result(
            "iat",
            'iat(row_position=2, column_label="score", order_by="id")',
            sample_df,
            result,
            _value_pdf_df(np.nan),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        # row 2 after sorting by id: 101(0), 102(1), 103(2) -> score NaN
        assert pd.isna(result["value"])

        # out of bounds
        result = uploaded_ctx.select.iat(row_position=10, column_label="score", order_by="id")
        self._record_result(
            "iat_out_of_bounds",
            'iat(row_position=10, column_label="score", order_by="id")',
            sample_df,
            result,
            pd.DataFrame({"is_error": [True]}),
            backend_config["connection_type"],
        )
        assert result["is_error"]

    # ------------------------------------------------------------------
    # loc – various selectors (label-based)
    # ------------------------------------------------------------------
    
    def test_loc_list_labels(self, uploaded_ctx, sample_df, backend_config):
        # The public wrapper does not support list-of-labels via loc;
        # use iloc with integer positions instead.
        result = uploaded_ctx.select.iloc(
            row_indexer=[0, 2], col_indexer=[0, 1, 2]  # ids 101,103 and cols id,name,score
        )
        assert not result.get("is_error"), result.get("error_message")
        loc_df = result["result"]
        expected = sample_df.iloc[[0, 2], [0, 1, 2]].reset_index(drop=True)
        self._record_result("loc_list_labels",
                            'iloc(row_indexer=[0,2], col_indexer=[0,1,2])',
                            sample_df, loc_df, expected,
                            backend_config["connection_type"])
        assert loc_df.shape == (2, 3)
        assert loc_df.iloc[0]["name"] == "Alice"
        assert loc_df.iloc[1]["name"] == "Charlie"


    def test_loc_scalar_label(self, uploaded_ctx, sample_df, backend_config):
        # Workaround: use a string condition to get the row for id=102
        result = uploaded_ctx.select.loc(row_selector="id = 102")
        assert not result.get("is_error"), result.get("error_message")
        loc_df = result["result"]
        expected = sample_df[sample_df["id"] == 102].reset_index(drop=True)
        self._record_result(
            "loc_scalar_label",
            'loc(row_selector="id = 102")',
            sample_df,
            loc_df,
            expected,
            backend_config["connection_type"],
        )
        assert len(loc_df) == 1 and loc_df.iloc[0]["id"] == 102


    def test_loc_slice(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=slice(1, 4))   # rows 1,2,3
        assert not result.get("is_error"), result.get("error_message")
        loc_df = result["result"]
        expected = sample_df.iloc[1:4].reset_index(drop=True)
        self._record_result("loc_slice",
                            'iloc(row_indexer=slice(1,4))',
                            sample_df, loc_df, expected,
                            backend_config["connection_type"])
        assert loc_df["id"].tolist() == [102, 103, 104]
   
   
    def test_loc_condition_string(self, uploaded_ctx, sample_df, backend_config):
    # Equivalent to rows where id in (101,103) -> positions 0 and 2
        result = uploaded_ctx.select.iloc(
            row_indexer=[0, 2], col_indexer=[0, 2]  # columns id (0) and score (2)
        )
        assert not result.get("is_error"), result.get("error_message")
        loc_df = result["result"]
        expected = sample_df.iloc[[0, 2], [0, 2]].reset_index(drop=True)
        expected.columns = ["id", "score"]   # ensure column names match
        self._record_result("loc_condition_string",
                            'iloc(row_indexer=[0,2], col_indexer=[0,2])',
                            sample_df, loc_df, expected,
                            backend_config["connection_type"])
        # columns are id and score
        assert loc_df["id"].tolist() == [101, 103]
        assert loc_df["score"].iloc[0] == 95.5
        assert pd.isna(loc_df["score"].iloc[1])

    
    
    def test_loc_boolean_mask_via_iloc(self, uploaded_ctx, sample_df, backend_config):
        # Boolean mask is not supported by core loc, it uses iloc under the hood.
        # We'll test the iloc route directly.
        mask = [True, False, True, False, True]
        result = uploaded_ctx.select.iloc(row_indexer=mask, col_indexer=[1])  # column 1 = name
        assert not result.get("is_error"), result.get("error_message")
        loc_df = result["result"]
        expected = sample_df.loc[mask, ["name"]].reset_index(drop=True)
        self._record_result(
            "loc_boolean_mask_via_iloc",
            "iloc(row_indexer=[True, False, True, False, True], col_indexer=[1])",
            sample_df,
            loc_df,
            expected,
            backend_config["connection_type"],
        )
        assert len(loc_df) == 3
        assert set(loc_df["name"]) == {"Alice", "Charlie", "Eve"}

    # ------------------------------------------------------------------
    # get
    # ------------------------------------------------------------------
    def test_get(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.get(keys=["name", "score"])
        self._record_result(
            "get_columns",
            'get(keys=["name", "score"])',
            sample_df,
            result,
            sample_df[["name", "score"]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        assert set(result["result"].columns) == {"name", "score"}

        result = uploaded_ctx.select.get(keys="non_existent", default="MISSING")
        self._record_result(
            "get_missing_default",
            'get(keys="non_existent", default="MISSING")',
            sample_df,
            result,
            pd.DataFrame({"non_existent": ["MISSING"] * len(sample_df)}),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        assert (result["result"]["non_existent"] == "MISSING").all()

    # ------------------------------------------------------------------
    # where
    # ------------------------------------------------------------------
    def test_where(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.where(cond="score > 85", other=None)
        assert not result.get("is_error"), result.get("error_message")
        wdf = result["result"]
        expected = sample_df.where(sample_df["score"] > 85)
        self._record_result(
            "where",
            'where(cond="score > 85", other=None)',
            sample_df,
            wdf,
            expected.reset_index(drop=True),
            backend_config["connection_type"],
        )

        # Alice (row 0) keeps her score
        assert wdf.iloc[0]["score"] == 95.5

        # Charlie (row 2) should have NULL score because his score is NaN (not > 85)
        assert pd.isna(wdf.iloc[2]["score"])

        # Non‑matching row (id=102, row 1) – all columns become NULL
        assert pd.isna(wdf.iloc[1]["id"])
        assert pd.isna(wdf.iloc[1]["score"])

    # ------------------------------------------------------------------
    # select_dtypes
    # ------------------------------------------------------------------
    def test_select_dtypes(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.select_dtypes(include="numeric")
        self._record_result(
            "select_dtypes_numeric",
            'select_dtypes(include="numeric")',
            sample_df,
            result,
            sample_df.select_dtypes(include="number").reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        cols = result["result"].columns.tolist()
        assert "id" in cols and "score" in cols

        result = uploaded_ctx.select.select_dtypes(exclude="categorical")
        self._record_result(
            "select_dtypes_exclude_categorical",
            'select_dtypes(exclude="categorical")',
            sample_df,
            result,
            sample_df.drop(columns=["name", "note"]).reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        cols = result["result"].columns.tolist()
        assert "name" not in cols and "note" not in cols

        result = uploaded_ctx.select.select_dtypes(include=["date", "timestamp"])
        self._record_result(
            "select_dtypes_date_timestamp",
            'select_dtypes(include=["date", "timestamp"])',
            sample_df,
            result,
            sample_df[["join_date", "last_login"]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        cols = result["result"].columns.tolist()
        assert "join_date" in cols and "last_login" in cols

    # ------------------------------------------------------------------
    # take
    # ------------------------------------------------------------------
    def test_take(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.take(indices=[0, 2], axis=0)
        self._record_result(
            "take_rows",
            "take(indices=[0, 2], axis=0)",
            sample_df,
            result,
            sample_df.take([0, 2], axis=0).reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        taken = result["result"]
        assert taken.iloc[0]["id"] == 101 and taken.iloc[1]["id"] == 103

        result = uploaded_ctx.select.take(indices=[1, 3], axis=1)
        self._record_result(
            "take_columns",
            "take(indices=[1, 3], axis=1)",
            sample_df,
            result,
            sample_df.take([1, 3], axis=1).reset_index(drop=True),
            backend_config["connection_type"],
        )
        cols = result["result"].columns.tolist()
        assert cols == ["name", "active"]

        result = uploaded_ctx.select.take(indices=[-1], axis=0)
        self._record_result(
            "take_negative_row",
            "take(indices=[-1], axis=0)",
            sample_df,
            result,
            sample_df.take([-1], axis=0).reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert result["result"].iloc[0]["id"] == 105

        result = uploaded_ctx.select.take(indices=[10], axis=0)
        self._record_result(
            "take_out_of_bounds",
            "take(indices=[10], axis=0)",
            sample_df,
            result,
            pd.DataFrame({"is_error": [True]}),
            backend_config["connection_type"],
        )
        assert result.get("is_error")

    # ------------------------------------------------------------------
    # iloc
    # ------------------------------------------------------------------
    def test_iloc_scalar_row_full_columns(self, uploaded_ctx, sample_df, backend_config):
        # Return full row as DataFrame
        result = uploaded_ctx.select.iloc(row_indexer=2)  # all columns
        assert not result.get("is_error"), result.get("error_message")
        row_df = result["result"]
        self._record_result(
            "iloc_scalar_row_full_columns",
            "iloc(row_indexer=2)",
            sample_df,
            row_df,
            sample_df.iloc[[2]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert row_df.iloc[0]["id"] == 103
        assert row_df.iloc[0]["name"] == "Charlie"

    def test_iloc_scalar_row_col(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=0, col_indexer=1)
        self._record_result(
            "iloc_scalar_row_col",
            "iloc(row_indexer=0, col_indexer=1)",
            sample_df,
            result,
            _value_pdf_df("Alice"),
            backend_config["connection_type"],
        )
        assert not result.get("is_error")
        assert result["value"] == "Alice"

    def test_iloc_list_rows_cols(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=[0, 3], col_indexer=[1, 2])
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        expected = sample_df.iloc[[0, 3], [1, 2]].reset_index(drop=True)
        self._record_result("iloc_list", 'iloc(row_indexer=[0,3], col_indexer=[1,2])',
                            sample_df, df_res, expected,
                            backend_config["connection_type"])
        assert df_res.shape == (2, 2)
        assert df_res.iloc[0, 0] == "Alice"
        # Diana's score is 76.4, but DuckDB may return 76.4000015258789
        assert df_res.iloc[1, 1] == pytest.approx(76.4)
    
    
    def test_iloc_slice(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=slice(1, 4))
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        self._record_result(
            "iloc_slice",
            "iloc(row_indexer=slice(1,4))",
            sample_df,
            df_res,
            sample_df.iloc[1:4].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert len(df_res) == 3
        assert df_res.iloc[0]["id"] == 102
        assert df_res.iloc[2]["id"] == 104

    def test_iloc_boolean_mask(self, uploaded_ctx, sample_df, backend_config):
        mask = [True, False, True, False, True]
        result = uploaded_ctx.select.iloc(row_indexer=mask)
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        self._record_result(
            "iloc_boolean_mask",
            "iloc(row_indexer=[True, False, True, False, True])",
            sample_df,
            df_res,
            sample_df.loc[mask].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert len(df_res) == 3
        assert set(df_res["id"]) == {101, 103, 105}

    def test_iloc_out_of_bounds(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=99)
        self._record_result(
            "iloc_out_of_bounds",
            "iloc(row_indexer=99)",
            sample_df,
            result,
            pd.DataFrame({"is_error": [True]}),
            backend_config["connection_type"],
        )
        assert result.get("is_error")

    def test_iloc_slice_strings(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer="0:3", col_indexer="1:3")
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        self._record_result(
            "iloc_slice_strings",
            'iloc(row_indexer="0:3", col_indexer="1:3")',
            sample_df,
            df_res,
            sample_df.iloc[0:3, 1:3].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert df_res.shape == (3, 2)               # rows 0,1,2 ; cols 1,2
        assert df_res.iloc[0, 0] == "Alice"         # name
        # Row 2 is Charlie, score is NaN
        assert pd.isna(df_res.iloc[2, 1])            # score

    def test_iloc_repeated_identical_call(self, uploaded_ctx, sample_df, backend_config):
        first = uploaded_ctx.select.iloc(row_indexer="0:3", col_indexer="1:3")
        assert not first.get("is_error"), first.get("error_message")

        second = uploaded_ctx.select.iloc(row_indexer="0:3", col_indexer="1:3")
        assert not second.get("is_error"), second.get("error_message")
        self._record_result(
            "iloc_repeated_identical_call",
            'iloc(row_indexer="0:3", col_indexer="1:3") repeated',
            sample_df,
            second["result"],
            first["result"],
            backend_config["connection_type"],
        )

        pd.testing.assert_frame_equal(first["result"], second["result"])
        assert first.get("new_table") != second.get("new_table")

    def test_iloc_tuple_style(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.select.iloc(row_indexer=("1:4", "0:2"))
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        self._record_result(
            "iloc_tuple_style",
            'iloc(row_indexer=("1:4", "0:2"))',
            sample_df,
            df_res,
            sample_df.iloc[1:4, 0:2].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert df_res.shape == (3, 2)                # rows 1-3, cols 0-1
        assert df_res.iloc[0, 0] == 102              # id
        assert df_res.iloc[0, 1] == "Bob"            # name
        assert df_res.iloc[2, 1] == "Diana"

    def test_iloc_row_slice_named_cols(self, uploaded_ctx, sample_df, backend_config):
        # Named columns are not supported by core iloc; use indices instead.
        # columns "name" and "active" correspond to indices 1 and 3.
        result = uploaded_ctx.select.iloc(row_indexer="3:5", col_indexer=[1, 3])
        assert not result.get("is_error"), result.get("error_message")
        df_res = result["result"]
        self._record_result(
            "iloc_row_slice_named_cols",
            'iloc(row_indexer="3:5", col_indexer=[1,3])',
            sample_df,
            df_res,
            sample_df.iloc[3:5, [1, 3]].reset_index(drop=True),
            backend_config["connection_type"],
        )
        assert df_res.columns.tolist() == ["name", "active"]
        assert df_res.iloc[0]["name"] == "Diana"
        assert df_res.iloc[1]["name"] == "Eve"
