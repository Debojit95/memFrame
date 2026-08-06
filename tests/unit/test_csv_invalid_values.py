import asyncio

import pyarrow as pa

from memframe.core.ingestion.upload.strategies.csv import CsvUploadStrategy
from memframe.main import MemFrame

_SENTINEL = "__-333333333333333333333333333__"


def _write_csv(path, n_rows=100):
    lines = ["id,score,label"]
    for i in range(n_rows):
        score = _SENTINEL if i == n_rows - 1 else str(0.5 * i)
        lines.append(f"{i},{score},cat{i % 3}")
    path.write_text("\n".join(lines) + "\n")


def _table(m, data_id):
    return f'"{m._backend.upload_schema}"."{data_id}"'


def _count(m, sql):
    return asyncio.run(m._backend.fetchval(sql))


def test_csv_invalid_value_does_not_fail_upload(tmp_path):
    _write_csv(tmp_path / "bad.csv")

    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    try:
        asyncio.run(m.aconnect())
        ctx = asyncio.run(m.aupload_csv(str(tmp_path / "bad.csv")))
        table = _table(m, ctx._data_id)
        assert _count(m, f"SELECT COUNT(*) FROM {table}") == 100
    finally:
        asyncio.run(m.aclose())


def test_csv_fallback_preserves_unparseable_value_as_text(tmp_path, monkeypatch):
    _write_csv(tmp_path / "bad.csv")

    async def boom(self, file_path, encoding, sample_rows=5000):
        raise pa.ArrowInvalid("In CSV column #1: CSV conversion error to double")

    monkeypatch.setattr(CsvUploadStrategy, "_infer_types_from_csv", boom)

    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    try:
        asyncio.run(m.aconnect())
        ctx = asyncio.run(m.aupload_csv(str(tmp_path / "bad.csv")))
        table = _table(m, ctx._data_id)
        assert _count(m, f"SELECT COUNT(*) FROM {table}") == 100
        # The unparseable value is preserved as text (column stays TEXT),
        # not fatal, and no rows are lost or duplicated.
        kept = _count(m, f"SELECT COUNT(*) FROM {table} WHERE score = '{_SENTINEL}'")
        assert kept == 1
    finally:
        asyncio.run(m.aclose())