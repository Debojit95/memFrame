"""Finding 8: caller-supplied names must address themselves, never a rewrite."""

import asyncio

import pandas as pd
import pytest

from memframe.exceptions import ConfigurationError
from memframe.main import MemFrame


@pytest.fixture
def mf():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    try:
        yield m
    finally:
        asyncio.run(m.aclose())


def _register_odd_table(mf):
    asyncio.run(mf._backend.execute(
        'CREATE TABLE main."odd table" ("sale price" INTEGER, "a-b" INTEGER, "a_b" INTEGER); '
        'INSERT INTO main."odd table" VALUES (10, 1, 100), (20, 2, 200), (30, 3, 300);'
    ))
    registered = asyncio.run(mf.aregister_tables())
    assert "odd table" in {t["table_name"] for t in registered.get("main", [])}
    return next(t["data_id"] for t in registered["main"] if t["table_name"] == "odd table")


class TestSpacedNamesEndToEnd:
    def test_spaced_table_and_columns(self, mf):
        data_id = _register_odd_table(mf)
        ctx = asyncio.run(mf.aset_active(data_id))

        head = asyncio.run(ctx.ahead(n=10))
        assert set(head.columns) == {"sale price", "a-b", "a_b"}

        assert asyncio.run(ctx.amax("sale price")) == 30
        assert asyncio.run(ctx.amax("a-b")) == 3

    def test_dash_and_underscore_coexist(self, mf):
        data_id = _register_odd_table(mf)
        ctx = asyncio.run(mf.aset_active(data_id))

        # Pre-fix both addressed "a_b"; now each addresses its own column.
        assert asyncio.run(ctx.amax("a-b")) == 3
        assert asyncio.run(ctx.amax("a_b")) == 300

    def test_typo_names_the_passed_spelling(self, mf):
        data_id = _register_odd_table(mf)
        ctx = asyncio.run(mf.aset_active(data_id))

        # Loud failure naming the passed spelling — never a silent rewrite.
        with pytest.raises(Exception, match="saleprice"):
            asyncio.run(ctx.amax("saleprice"))

    def test_empty_column_fails_closed(self, mf):
        data_id = _register_odd_table(mf)
        ctx = asyncio.run(mf.aset_active(data_id))
        with pytest.raises(ConfigurationError):
            asyncio.run(ctx.amax(""))
