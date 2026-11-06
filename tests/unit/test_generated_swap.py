"""Postgres write-back must not drop the table a public view depends on."""

import asyncio

from memframe.core.analytix._generated import swap_table_into_source
from memframe.core.ingestion.datatype_detector import Backend


class _FakeDb:
    def __init__(self):
        self.sql = []

    def quote_identifier(self, name):
        return '"' + str(name).replace('"', '""') + '"'

    async def execute(self, sql, *args):
        self.sql.append(sql)

    async def get_column_types(self, table, schema):
        if table.endswith("__wide"):
            return {
                "id": "bigint",
                "_mf_idx": "integer",
                "w_mean": "double precision",
                "__rn": "bigint",
            }
        return {"id": "bigint", "_mf_idx": "integer"}


class _FakeBackend:
    backend = Backend.POSTGRES


def test_postgres_swap_updates_source_in_place():
    db = _FakeDb()
    asyncio.run(swap_table_into_source(
        db,
        _FakeBackend(),
        "memframe_upload",
        "abc__base",
        "abc__base__wide",
        generated=["w_mean"],
    ))
    joined = " ".join(db.sql)

    # The base relation (and therefore the public view) must survive.
    assert 'DROP TABLE "memframe_upload"."abc__base"' not in joined
    assert 'ADD COLUMN IF NOT EXISTS "w_mean" double precision' in joined
    assert (
        'UPDATE "memframe_upload"."abc__base" AS s '
        'SET "w_mean" = w."w_mean"'
    ) in joined
    assert 's."_mf_idx" = w."_mf_idx"' in joined
    assert 'DROP TABLE "memframe_upload"."abc__base__wide"' in joined
