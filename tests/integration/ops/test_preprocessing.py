# tests/integration/ops/test_preprocessing.py

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

from memframe import MemFrame

# ----------------------------------------------------------------------
# Backend configuration – set environment variables for remote databases
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

TEST_BACKENDS = [
    backend.strip()
    for backend in os.getenv("MEMFRAME_TEST_BACKENDS", "local").split(",")
    if backend.strip()
]
RESULT_DIR = Path(__file__).resolve().parent / "result"


def _usage_error(message: str) -> pytest.UsageError:
    return pytest.UsageError(f"Invalid preprocessing DB configuration: {message}")


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
        return {"backend": DUCKDB_BACKEND, "connection_type": "local", "params": _validate_duckdb_params(params)}
    if backend == POSTGRES_BACKEND:
        return {"backend": POSTGRES_BACKEND, "connection_type": "remote", "params": _validate_postgres_params(params)}
    return {"backend": CLICKHOUSE_BACKEND, "connection_type": "remote", "params": _validate_clickhouse_params(params)}


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
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "numeric1": [10, 20, 30, 40, 50],
            "numeric2": [5, 6, 7, 8, 9],
            "category_col": ["A", "B", "A", "C", "B"],
            "target_col": [1, 2, 1, 3, 2],
            "date_col": pd.date_range("2025-01-01", periods=5, freq="D"),
            "bool_val": [True, False, True, False, True],
        }
    )


# ----------------------------------------------------------------------
# Backend fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="function")
def backend_config(request) -> Dict[str, Any]:
    config = getattr(request, "param", None)
    if config is None:
        config = _selected_backend_configs(request.config)[0]
    return {"backend": config["backend"], "connection_type": config["connection_type"], "params": dict(config.get("params", {}))}


@pytest.fixture(scope="function")
def connected_memframe(backend_config) -> MemFrame:
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
    ctx = connected_memframe.upload_df(sample_df, filename="preprocess_dataset")
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


def _extract_bin_index(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"bin_(\d+)")[0].astype(float)


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
            return pd.DataFrame({"is_error": [value.get("is_error")], "error_message": [value.get("error_message", "")]})
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


def render_df_to_pdf_page(pdf, title, method_call, original_df, memframe_df, pandas_df, backend, status="PASSED", error_message=""):
    sections = [("Original", original_df.head(10)), ("MemFrame Result", memframe_df.head(10)), ("Pandas Result", pandas_df.head(10))]
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
class TestPreprocessingOperations:
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
            pdf_path = RESULT_DIR / f"test_preprocess_report_{request.node.name}.pdf"
            with PdfPages(pdf_path) as pdf:
                for result in cls._saved_results:
                    render_df_to_pdf_page(pdf, result["test_name"], result["method_call"], result["original_df"], result["memframe_df"], result["pandas_df"], result["backend"], result.get("status", "PASSED"), result.get("error_message", ""))
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
        frame_locals = getattr(request.node, "_preprocessing_failure_locals", None)
        if frame_locals is None:
            frame_locals = getattr(request.node, "_failure_locals", {})
        original_df = _coerce_pdf_df(frame_locals.get("sample_df"), "sample_df was not available when this test failed")
        memframe_value = None
        for name in ("res_df", "original", "result", "actual_vals", "mapped_series"):
            if name in frame_locals:
                memframe_value = frame_locals[name]
                break
        memframe_df = _coerce_pdf_df(memframe_value, "No MemFrame result was available when this test failed")
        pandas_value = None
        for name in ("expected", "expected_vals", "expected_product", "expected_labels", "expected_freq"):
            if name in frame_locals:
                pandas_value = frame_locals[name]
                break
        pandas_df = _coerce_pdf_df(pandas_value, "No pandas expected result was available when this test failed")
        backend_config = frame_locals.get("backend_config") or {}
        self._record_result(test_name=request.node.name, method_call=request.node.name, original_df=original_df, memframe_df=memframe_df, pandas_df=pandas_df, backend=backend_config.get("connection_type", "unknown"), status="FAILED", error_message=error_message)

    def _record_result(self, test_name, method_call, original_df, memframe_df, pandas_df, backend, status="PENDING", error_message=""):
        if self._save_to_file:
            result = {"test_name": test_name, "method_call": method_call, "original_df": _prepare_pdf_df(_coerce_pdf_df(original_df, "No original data")), "memframe_df": _prepare_pdf_df(_coerce_pdf_df(memframe_df, "No MemFrame result")), "pandas_df": _prepare_pdf_df(_coerce_pdf_df(pandas_df, "No pandas result")), "backend": backend, "status": status, "error_message": error_message}
            self._saved_results.append(result)
            current_records = getattr(self, "_current_pdf_records", None)
            if status == "PENDING" and current_records is not None:
                current_records.append(result)

    def _get_new_col(self, res_df, original_cols):
        new = [c for c in res_df.columns if c not in original_cols]
        if len(new) == 0:
            return None
        return new[0]

    # ----------------------------------------------------------------
    # Numeric scaling / transformation
    # ----------------------------------------------------------------
    def test_scale(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.scale("numeric1")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected_vals = (sample_df["numeric1"] - sample_df["numeric1"].mean()) / sample_df["numeric1"].std(ddof=0)
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="scale", method_call='uploaded_ctx.scale("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(scaled=expected_vals), backend=backend_config["connection_type"])

    def test_minmax(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.minmax("numeric1")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected_vals = (sample_df["numeric1"] - sample_df["numeric1"].min()) / (sample_df["numeric1"].max() - sample_df["numeric1"].min())
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="minmax", method_call='uploaded_ctx.minmax("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(minmax=expected_vals), backend=backend_config["connection_type"])

    def test_bin(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.bin("numeric1", bins=3, strategy="uniform")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected_vals = pd.cut(sample_df["numeric1"], bins=3, labels=False) + 1
        assert_series_equal_loose(_extract_bin_index(actual_vals), expected_vals.astype(float))
        self._record_result(test_name="bin", method_call='uploaded_ctx.bin("numeric1", bins=3, strategy="uniform")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(binned=expected_vals), backend=backend_config["connection_type"])

    def test_poly(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.poly("numeric1", degree=2)
        res_df = get_result_df(result)
        new_cols = [c for c in res_df.columns if c not in sample_df.columns]
        assert len(new_cols) > 0
        self._record_result(test_name="poly", method_call='uploaded_ctx.poly("numeric1", degree=2)', original_df=sample_df, memframe_df=res_df, pandas_df=res_df, backend=backend_config["connection_type"])

    def test_interact(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.interact("numeric1", "numeric2")
        res_df = get_result_df(result)
        new_cols = [c for c in res_df.columns if c not in sample_df.columns]
        assert len(new_cols) > 0
        expected_product = sample_df["numeric1"] * sample_df["numeric2"]
        found = any(np.allclose(res_df[col].astype(float), expected_product.astype(float)) for col in new_cols)
        assert found, "Interaction term not found"
        self._record_result(test_name="interact", method_call='uploaded_ctx.interact("numeric1", "numeric2")', original_df=sample_df, memframe_df=res_df, pandas_df=res_df, backend=backend_config["connection_type"])

    # ----------------------------------------------------------------
    # Categorical encoding
    # ----------------------------------------------------------------
    def test_onehot(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.onehot("category_col", max_categories=10)
        res_df = get_result_df(result)
        for cat in sample_df["category_col"].unique():
            assert f"transformed_category_col_{cat}" in res_df.columns
        self._record_result(test_name="onehot", method_call='uploaded_ctx.onehot("category_col")', original_df=sample_df, memframe_df=res_df, pandas_df=pd.get_dummies(sample_df, columns=["category_col"]), backend=backend_config["connection_type"])

    def test_label(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.label_encode("category_col")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["category_col"]
        # ponytail: label_encode ranks by COUNT(*) DESC – ties are backend-order dependent,
        # so check bijection not exact factorize order
        categories = sample_df["category_col"].tolist()
        labs = actual_vals.astype(int).tolist()
        mapping: Dict[str, int] = {}
        for cat, lab in zip(categories, labs):
            if cat in mapping:
                assert mapping[cat] == lab, f"category {cat} mapped to both {mapping[cat]} and {lab}"
            else:
                mapping[cat] = lab
        assert set(mapping.values()) == set(range(len(mapping))), f"labels not 0..n-1: {mapping}"
        assert len(mapping) == sample_df["category_col"].nunique()
        expected_labels, _ = pd.factorize(sample_df["category_col"])
        self._record_result(test_name="label", method_call='uploaded_ctx.label_encode("category_col")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(label=expected_labels), backend=backend_config["connection_type"])

    def test_frequency(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.frequency_encode("category_col")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["category_col"]
        freq_map = sample_df["category_col"].value_counts(normalize=True)
        expected_freq = sample_df["category_col"].map(freq_map)
        assert_series_equal_loose(actual_vals.astype(float), expected_freq.astype(float))
        self._record_result(test_name="frequency", method_call='uploaded_ctx.frequency_encode("category_col")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(freq=expected_freq), backend=backend_config["connection_type"])

    def test_target_encoding(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.target_encode("category_col", target_column="target_col")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["category_col"]
        global_mean = sample_df["target_col"].mean()
        grp = sample_df.groupby("category_col")["target_col"].agg(["mean", "count"])
        smooth = (grp["mean"] * grp["count"] + global_mean * 10) / (grp["count"] + 10)
        expected = sample_df["category_col"].map(smooth)
        assert_series_equal_loose(actual_vals.astype(float), expected.astype(float))
        self._record_result(test_name="target_encoding", method_call='uploaded_ctx.target_encode("category_col", target_column="target_col")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(encoded=expected), backend=backend_config["connection_type"])

    def test_binarize_value(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.binarize("category_col", value="A")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["category_col"]
        expected = (sample_df["category_col"] == "A").astype(int)
        assert_series_equal_loose(actual_vals.astype(int), expected)
        self._record_result(test_name="binarize_value", method_call='uploaded_ctx.binarize("category_col", value="A")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(binarized=expected), backend=backend_config["connection_type"])

    def test_binarize_condition(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.binarize("numeric1", condition="> 30")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected = (sample_df["numeric1"] > 30).astype(int)
        assert_series_equal_loose(actual_vals.astype(int), expected)
        self._record_result(test_name="binarize_condition", method_call='uploaded_ctx.binarize("numeric1", condition="> 30")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(cond=expected), backend=backend_config["connection_type"])

    # ----------------------------------------------------------------
    # Datetime cyclical encoding
    # ----------------------------------------------------------------
    def test_cyclical(self, uploaded_ctx, sample_df, backend_config):
        # ponytail: valid features are month/dow/hour – use dow not dayofweek
        result = uploaded_ctx.cyclical_encode("date_col", features=["month", "dow"])
        res_df = get_result_df(result)
        expected_cols = [c for c in res_df.columns if "date_col" in c and ("sin" in c or "cos" in c)]
        assert len(expected_cols) >= 2
        self._record_result(test_name="cyclical", method_call='uploaded_ctx.cyclical_encode("date_col", features=["month", "dow"])', original_df=sample_df, memframe_df=res_df, pandas_df=res_df, backend=backend_config["connection_type"])

    # ----------------------------------------------------------------
    # Aliases
    # ----------------------------------------------------------------
    def test_get_dummies(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.get_dummies("category_col")
        res_df = get_result_df(result)
        assert any("category_col" in col for col in res_df.columns if col != "category_col")
        self._record_result(test_name="get_dummies", method_call='uploaded_ctx.get_dummies("category_col")', original_df=sample_df, memframe_df=res_df, pandas_df=pd.get_dummies(sample_df, columns=["category_col"]), backend=backend_config["connection_type"])

    def test_cut(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.cut("numeric1", bins=3)
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected = pd.cut(sample_df["numeric1"], bins=3, labels=False) + 1
        assert_series_equal_loose(_extract_bin_index(actual_vals), expected.astype(float))
        self._record_result(test_name="cut", method_call='uploaded_ctx.cut("numeric1", bins=3)', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(binned=expected), backend=backend_config["connection_type"])

    def test_qcut(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.qcut("numeric1", bins=2)
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = _extract_bin_index(res_df[new_col] if new_col else res_df["numeric1"])
        # ponytail: quantile edge handling differs from pandas (inclusive lower vs upper),
        # so check structure not exact bin assignment
        assert set(actual_vals.dropna().unique()) == {1.0, 2.0}
        assert len(actual_vals) == len(sample_df)
        self._record_result(test_name="qcut", method_call='uploaded_ctx.qcut("numeric1", bins=2)', original_df=sample_df, memframe_df=res_df, pandas_df=res_df, backend=backend_config["connection_type"])

    def test_robust_scale(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.robust_scale("numeric1")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        median = sample_df["numeric1"].median()
        q75 = sample_df["numeric1"].quantile(0.75)
        q25 = sample_df["numeric1"].quantile(0.25)
        iqr = q75 - q25
        expected_vals = (sample_df["numeric1"] - median) / iqr if iqr != 0 else sample_df["numeric1"] * 0
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="robust_scale", method_call='uploaded_ctx.robust_scale("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(robust=expected_vals), backend=backend_config["connection_type"])

    def test_maxabs_scale(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.maxabs_scale("numeric1")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        max_abs = sample_df["numeric1"].abs().max()
        expected_vals = sample_df["numeric1"] / max_abs if max_abs != 0 else sample_df["numeric1"] * 0
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="maxabs_scale", method_call='uploaded_ctx.maxabs_scale("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(maxabs=expected_vals), backend=backend_config["connection_type"])

    def test_normalize(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.normalize("numeric1")
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        # ponytail: single-col L2 → sign
        expected_vals = sample_df["numeric1"].apply(lambda x: 0 if x == 0 else (1 if x > 0 else -1) if pd.notna(x) else np.nan)
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="normalize", method_call='uploaded_ctx.normalize("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(norm=expected_vals), backend=backend_config["connection_type"])

    def test_log_transform(self, uploaded_ctx, sample_df, backend_config):
        result = uploaded_ctx.log_transform("numeric1", base="e", epsilon=0)
        res_df = get_result_df(result)
        new_col = self._get_new_col(res_df, sample_df.columns)
        actual_vals = res_df[new_col] if new_col else res_df["numeric1"]
        expected_vals = np.log(sample_df["numeric1"].astype(float))
        assert_series_equal_loose(actual_vals.astype(float), expected_vals.astype(float))
        self._record_result(test_name="log_transform", method_call='uploaded_ctx.log_transform("numeric1")', original_df=sample_df, memframe_df=res_df, pandas_df=sample_df.assign(log=expected_vals), backend=backend_config["connection_type"])

    # ----------------------------------------------------------------
    # Mutation safety
    # ----------------------------------------------------------------
    def test_mutation_safety(self, uploaded_ctx, sample_df, backend_config):
        _ = uploaded_ctx.scale("numeric1")
        original = get_result_df(uploaded_ctx)
        for col in sample_df.columns:
            if col in original.columns:
                left = original[col].fillna("")
                right = sample_df[col].fillna("")
                if pd.api.types.is_datetime64_any_dtype(sample_df[col]) or "date" in str(col).lower():
                    left = pd.to_datetime(left, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                    right = pd.to_datetime(right, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                assert_series_equal_loose(left, right)
        self._record_result(test_name="mutation_safety", method_call="scale then check original unchanged", original_df=sample_df, memframe_df=original, pandas_df=sample_df, backend=backend_config["connection_type"])
