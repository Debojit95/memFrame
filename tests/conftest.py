import pytest


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)

    if report.when != "call" or call.excinfo is None:
        return

    frame_locals = {}
    tb = call.excinfo.value.__traceback__
    while tb:
        frame = tb.tb_frame
        if frame.f_code.co_name.startswith("test_"):
            frame_locals = dict(frame.f_locals)
        tb = tb.tb_next

    item._failure_locals = frame_locals
    module_name = getattr(item.module, "__name__", "")
    if module_name.endswith("test_stats"):
        item._stats_failure_locals = frame_locals
    elif module_name.endswith("test_cleaning"):
        item._cleaning_failure_locals = frame_locals


def pytest_addoption(parser):
    parser.addoption(
        "--save-to-file",
        action="store_true",
        default=False,
        help="Save test results (expected vs actual) to a PDF report"
    )
    parser.addoption(
        "--upload-type",
        dest="upload_type",
        choices=("csv", "parquet", "df"),
        default=None,
        help="Upload integration target for tests/test_upload.py: csv, parquet, or df",
    )
    parser.addoption(
        "--filepath",
        dest="filepath",
        default=None,
        help="Path to the source file for csv/parquet upload tests",
    )
    parser.addoption(
        "--db-backend",
        "--db-connection-type",
        dest="db_backend",
        choices=("duckdb", "postgres", "clickhouse"),
        default=None,
        help="Database backend for tests that support DB selection: duckdb, postgres, or clickhouse",
    )
    parser.addoption(
        "--db-params",
        "--db-connection-params",
        dest="db_params",
        default=None,
        help=(
            "JSON object with connection params. DuckDB accepts db_path. "
            "Postgres accepts host, port, user, password, database, and optional backend. "
            "ClickHouse accepts host, port, user, password, database, secure, timeout, and optional backend."
        ),
    )

@pytest.fixture(scope="session")
def save_to_file(request):
    return request.config.getoption("--save-to-file")
