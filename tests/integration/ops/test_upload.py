from memframe import MemFrame
from memframe.core.ingestion.upload.base import Uploader


import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import pytest


def test_postgres_safe_cast_handles_float_alias() -> None:
    sql = Uploader()._build_safe_cast_postgres("score", "FLOAT")

    assert 'REPLACE(TRIM("score"::TEXT), \',\', \'\')::FLOAT' in sql
    assert 'AS "score"' in sql



def _parse_db_params(raw: str | None) -> Dict[str, Any]:
    if not raw:
        return {}

    try:
        params = json.loads(raw)
    except json.JSONDecodeError as exc:
        pytest.fail(f"--db-params must be valid JSON: {exc}")

    if not isinstance(params, dict):
        pytest.fail("--db-params must decode to a JSON object")

    return params


def _connection_config(db_backend: str | None, db_params: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    backend = (db_backend or db_params.get("backend") or "duckdb").lower()

    if backend not in {"duckdb", "postgres", "clickhouse"}:
        pytest.fail("--db-backend must be 'duckdb', 'postgres', or 'clickhouse'")

    params = dict(db_params)
    declared_backend = params.get("backend")
    if declared_backend and declared_backend != backend:
        pytest.fail("--db-backend and --db-params['backend'] must refer to the same backend")

    if backend == "postgres":
        params["backend"] = "postgres"
        missing = [key for key in ("host", "user", "password", "database") if key not in params]
        if missing:
            pytest.fail(f"Postgres upload tests require db params: {', '.join(missing)}")
        if "port" in params:
            params["port"] = int(params["port"])
        return "remote", params

    if backend == "clickhouse":
        params["backend"] = "clickhouse"
        missing = [key for key in ("host", "user", "password") if key not in params]
        if missing:
            pytest.fail(f"ClickHouse upload tests require db params: {', '.join(missing)}")
        if "port" in params:
            params["port"] = int(params["port"])
        return "remote", params

    params.pop("backend", None)
    return "local", params


def _requested_upload(request) -> tuple[str, Path | None]:
    upload_type = request.config.getoption("upload_type")
    if not upload_type:
        pytest.skip("Provide --upload-type to run upload integration tests")

    path_arg = request.config.getoption("filepath")
    if upload_type in {"csv", "parquet"}:
        if not path_arg:
            pytest.fail("--filepath is required for csv/parquet upload tests")

        path = Path(path_arg)
        if not path.exists():
            pytest.fail(f"Upload source file does not exist: {path}")
        return upload_type, path

    return upload_type, None


def _uploaded_data_id(ctx: Any) -> str:
    data_id = getattr(ctx, "_data_id", None)
    if not isinstance(data_id, str) or not data_id:
        pytest.fail("Upload did not return a ContextManager with a data_id")
    return data_id


def _expected_row_count(upload_type: str, path: Path | None) -> int:
    if upload_type == "df":
        return 3
    if upload_type == "csv":
        return len(pd.read_csv(path))
    if upload_type == "parquet":
        return len(pd.read_parquet(path))
    pytest.fail(f"Unknown upload type: {upload_type}")


async def _run_upload_test(
    upload_type: str,
    path: Path | None,
    connection_type: str,
    connection_params: Dict[str, Any],
) -> None:
    mf = MemFrame(connection_type=connection_type, connection_params=connection_params)
    await mf.aconnect()

    try:
        if upload_type == "csv":
            ctx = await mf.aupload_csv(path)
            expected_filename = path.name
        elif upload_type == "parquet":
            ctx = await mf.aupload_parquet(path)
            expected_filename = path.name
        elif upload_type == "df":
            df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
            expected_filename = "test_upload_df.csv"
            ctx = await mf.aupload_df(df, filename=expected_filename)
        else:
            pytest.fail(f"Unknown upload type: {upload_type}")

        data_id = _uploaded_data_id(ctx)
        tables = await mf.alist_tables()
        registry_entry = next((row for row in tables if row["data_id"] == data_id), None)

        assert registry_entry is not None, f"Uploaded data_id '{data_id}' is missing from list_tables()"
        assert registry_entry["filename"] == expected_filename
        assert await mf._backend.table_exists(f'{mf._backend.upload_schema}."{data_id}"')

        row_count = await mf._backend.fetchval(
            f"""
            SELECT row_count
            FROM {mf._backend.csv_registry_table}
            WHERE data_id = {mf._backend.placeholder(1)}
            """,
            data_id,
        )
        assert int(row_count) == _expected_row_count(upload_type, path)
    finally:
        await mf.aclose()


class TestUploadOperations:
    def test_upload(self, request):
        upload_type, path = _requested_upload(request)
        db_params = _parse_db_params(request.config.getoption("db_params"))
        connection_type, connection_params = _connection_config(
            request.config.getoption("db_backend"),
            db_params,
        )

        asyncio.run(
            _run_upload_test(
                upload_type=upload_type,
                path=path,
                connection_type=connection_type,
                connection_params=connection_params,
            )
        )


async def _upload_and_column_types(
    connection_params: Dict[str, Any],
    df: pd.DataFrame | None = None,
    dtypes: Dict[str, str] | None = None,
    csv_content: str | None = None,
) -> Dict[str, str]:
    import tempfile

    mf = MemFrame(connection_type="local", connection_params=connection_params)
    await mf.aconnect()
    try:
        if csv_content is not None:
            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
                f.write(csv_content)
                path = Path(f.name)
            ctx = await mf.aupload_csv(path, dtypes=dtypes)
        else:
            assert df is not None
            ctx = await mf.aupload_df(df, dtypes=dtypes)

        rows = mf._pool.conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=? ORDER BY ordinal_position",
            [mf._backend.upload_schema, ctx._data_id],
        ).fetchall()
        return {name: dtype for name, dtype in rows}
    finally:
        await mf.aclose()


def _memory_params() -> Dict[str, Any]:
    return {"backend": "duckdb", "db_path": f"memframe_test_{os.getpid()}.duckdb"}


class TestTypeInference:
    def test_csv_native_first(self):
        types = asyncio.run(
            _upload_and_column_types(
                _memory_params(),
                csv_content="id,name,score,dt\n1,alice,9.5,2024-01-01\n2,bob,8.0,2024-02-01\n",
            )
        )
        assert types["id"] == "BIGINT"
        assert types["score"] == "DOUBLE"
        assert types["dt"] == "DATE"

    def test_csv_override(self):
        types = asyncio.run(
            _upload_and_column_types(
                _memory_params(),
                csv_content="id,name,score\n1,alice,9.5\n2,bob,8.0\n",
                dtypes={"id": "TEXT", "score": "DOUBLE PRECISION"},
            )
        )
        assert types["id"] == "VARCHAR"
        assert types["score"] == "DOUBLE"

    def test_csv_incompatible_override_raises(self):
        from memframe.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError):
            asyncio.run(
                _upload_and_column_types(
                    _memory_params(),
                    csv_content="score\n9.5\n8.0\n",
                    dtypes={"score": "int64"},
                )
            )

    def test_df_override(self):
        df = pd.DataFrame({"A": [1, 2, 3], "B": [1.5, 2.5, 3.5]})
        types = asyncio.run(
            _upload_and_column_types(_memory_params(), df=df, dtypes={"A": "int64", "B": "float32"})
        )
        assert types["A"] == "BIGINT"
        assert types["B"] == "FLOAT"

    def test_df_mixed_column_falls_back(self):
        df = pd.DataFrame({"A": [1, 2, "x"], "B": ["y", "z", "w"]})
        types = asyncio.run(_upload_and_column_types(_memory_params(), df=df))
        assert types["A"] == "VARCHAR"
        assert types["B"] == "VARCHAR"

    def test_normalize_dtype_override(self):
        u = Uploader()
        assert u._normalize_dtype_override({"a": "int64"}) == {"a": "BIGINT"}
        assert u._normalize_dtype_override({"a": "BIGINT"}) == {"a": "BIGINT"}
        assert u._normalize_dtype_override({"a": "datetime64[ns, UTC]"}) == {"a": "TIMESTAMPTZ"}
        assert u._normalize_dtype_override({"a": "NUMERIC(10,2)"}) == {"a": "NUMERIC(10,2)"}
        with pytest.raises(Exception):
            u._normalize_dtype_override({"a": "BOGUS"})
