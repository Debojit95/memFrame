import asyncio

import pytest

from memframe.db_manager.connection import DuckDBPool
from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.connection import create_pool


@pytest.fixture
def pool():
    p = DuckDBPool(":memory:")
    asyncio.run(p.connect())
    try:
        yield p
    finally:
        asyncio.run(p.close())


def test_create_pool_duckdb():
    assert isinstance(create_pool(Backend.DUCKDB, {"db_path": ":memory:"}), DuckDBPool)


class TestDuckDBPool:
    def test_connect_sets_conn(self):
        import asyncio
        p = DuckDBPool(":memory:")
        asyncio.run(p.connect())
        assert p.conn is not None
        asyncio.run(p.close())
        assert p.conn is None

    def test_execute_and_fetch(self, pool):
        import asyncio
        asyncio.run(pool.execute("CREATE TABLE t (a INTEGER, b TEXT)"))
        asyncio.run(pool.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')"))
        rows = asyncio.run(pool.fetch("SELECT * FROM t ORDER BY a"))
        assert rows == [(1, "x"), (2, "y")]

    def test_fetchrow(self, pool):
        import asyncio
        asyncio.run(pool.execute("CREATE TABLE t (a INTEGER)"))
        asyncio.run(pool.execute("INSERT INTO t VALUES (42)"))
        row = asyncio.run(pool.fetchrow("SELECT a FROM t"))
        assert row == (42,)

    def test_fetchrow_empty(self, pool):
        import asyncio
        asyncio.run(pool.execute("CREATE TABLE t (a INTEGER)"))
        assert asyncio.run(pool.fetchrow("SELECT a FROM t")) is None

    def test_fetchval(self, pool):
        import asyncio
        assert asyncio.run(pool.fetchval("SELECT 1 + 1")) == 2

    def test_fetchval_empty(self, pool):
        import asyncio
        asyncio.run(pool.execute("CREATE TABLE t (a INTEGER)"))
        assert asyncio.run(pool.fetchval("SELECT a FROM t")) is None

    def test_parameterized_query(self, pool):
        import asyncio
        asyncio.run(pool.execute("CREATE TABLE t (a INTEGER, b TEXT)"))
        asyncio.run(pool.execute("INSERT INTO t VALUES (?, ?)", 1, "x"))
        row = asyncio.run(pool.fetchrow("SELECT b FROM t WHERE a = ?", 1))
        assert row == ("x",)

    def test_close_idempotent(self):
        import asyncio
        p = DuckDBPool(":memory:")
        asyncio.run(p.connect())
        asyncio.run(p.close())
        asyncio.run(p.close())
