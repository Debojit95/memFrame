import os
import tempfile
import uuid
from pathlib import Path


_RUN_ID = os.getenv("MEMFRAME_TEST_RUN_ID", f"{os.getpid()}-{uuid.uuid4().hex}")


def default_duckdb_test_path(suite_name: str) -> str:
    configured_path = os.getenv("MEMFRAME_DUCKDB_TEST_PATH")
    if configured_path:
        return configured_path

    root = Path(tempfile.gettempdir()) / "memframe-tests"
    root.mkdir(parents=True, exist_ok=True)
    return str(root / f"{suite_name}-{_RUN_ID}.duckdb")
