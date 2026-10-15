"""Finding 11: ClickHouse non-finite literals must parse; PG size check must bind."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from memframe.db_manager.adapters.clickhouse import (
    _render_clickhouse_query,
    _to_clickhouse_literal,
)
from memframe.exceptions import ConfigurationError


class TestClickHouseNonFiniteLiterals:
    def test_nan_renders_cast(self):
        assert _to_clickhouse_literal(float("nan")) == "CAST('nan' AS Float64)"

    def test_inf_renders_cast(self):
        assert _to_clickhouse_literal(float("inf")) == "CAST('inf' AS Float64)"
        assert _to_clickhouse_literal(float("-inf")) == "CAST('-inf' AS Float64)"

    def test_huge_float_renders_cast(self):
        # str(1e999) is 'inf' — same broken bareword without the branch.
        assert _to_clickhouse_literal(1e999) == "CAST('inf' AS Float64)"

    def test_decimal_non_finite_raises(self):
        with pytest.raises(ConfigurationError, match="Non-finite Decimal"):
            _to_clickhouse_literal(Decimal("NaN"))
        with pytest.raises(ConfigurationError, match="Non-finite Decimal"):
            _to_clickhouse_literal(Decimal("Infinity"))

    def test_finite_values_unchanged(self):
        assert _to_clickhouse_literal(3) == "3"
        assert _to_clickhouse_literal(1.5) == "1.5"
        assert _to_clickhouse_literal(Decimal("1.5")) == "1.5"
        assert _to_clickhouse_literal(True) == "1"
        assert _to_clickhouse_literal(False) == "0"
        assert _to_clickhouse_literal(None) == "NULL"
        assert _to_clickhouse_literal("o'brien") == "'o\\'brien'"
        assert _to_clickhouse_literal(date(2024, 1, 2)) == "'2024-01-02'"
        assert _to_clickhouse_literal(datetime(2024, 1, 2, 3, 4, 5)) == "'2024-01-02 03:04:05'"

    def test_render_substitutes_cast(self):
        out = _render_clickhouse_query("SELECT * FROM t WHERE x > ?", [float("nan")])
        assert out == "SELECT * FROM t WHERE x > CAST('nan' AS Float64)"


class TestPostgresSizeCheckBinds:
    class _FakeConn:
        def __init__(self, calls):
            self.calls = calls

        async def fetch(self, sql, *args):
            self.calls.append((sql, args))
            return []

        async def fetchrow(self, sql, *args):
            self.calls.append((sql, args))
            if "pg_total_relation_size" in sql:
                return {"total_size": "8192 bytes", "table_size": "8192 bytes"}
            return None

    class _FakeAcquire:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *exc):
            return False

    class _FakePGPool:
        def __init__(self):
            self.calls = []
            self.pool = self

        async def _ensure(self):
            pass

        def acquire(self):
            from memframe.db_manager.adapters.postgresql import PostgresAdapter  # noqa

            return TestPostgresSizeCheckBinds._FakeAcquire(
                TestPostgresSizeCheckBinds._FakeConn(self.calls)
            )

        async def fetchval(self, sql, *args):
            self.calls.append((sql, args))
            return 5

    def test_size_uses_regclass_bind_param(self):
        import asyncio

        from memframe.db_manager.adapters.postgresql import PostgresAdapter

        pool = self._FakePGPool()
        adapter = PostgresAdapter(pool)
        info = asyncio.run(adapter.get_table_info("o'brien", "my schema"))

        size_calls = [c for c in pool.calls if "pg_total_relation_size" in c[0]]
        assert len(size_calls) == 1
        sql, args = size_calls[0]
        assert "$1::regclass" in sql
        assert "o'brien" not in sql  # name travels as a param, not text
        assert args == ('"my schema"."o\'brien"',)
        assert info["total_size"] == "8192 bytes"
        assert info["row_count"] == 5
