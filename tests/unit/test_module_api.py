import asyncio

import pandas as pd
import pytest

import memframe as mf
from memframe import MemFrame
from memframe.exceptions import ConnectionNotReady


def test_connect_delegates_and_returns_instance():
    try:
        instance = mf.connect(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
        )

        assert isinstance(instance, MemFrame)
        assert mf.list_tables() == []

        ctx = mf.upload_df(pd.DataFrame({"a": [1, 2, 3]}), filename="module_api")
        assert ctx.head(2).shape == (2, 1)
        assert mf.list_tables()[0]["filename"] == "module_api"
    finally:
        mf.close()


def test_connect_accepts_deep_cache():
    try:
        instance = mf.connect(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
            deep_cache=True,
        )
        assert instance.deep_cache is True
    finally:
        mf.close()


def test_connect_replaces_previous_connection():
    try:
        first = mf.connect(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
        )
        mf.upload_df(pd.DataFrame({"a": [1]}), filename="first")

        second = mf.connect(
            connection_type="local",
            connection_params={"db_path": ":memory:"},
        )

        assert second is not first
        assert mf.list_tables() == []
    finally:
        mf.close()


def test_close_resets_default():
    mf.connect(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    mf.close()

    with pytest.raises(ConnectionNotReady):
        mf.list_tables()


def test_unknown_attribute_raises():
    with pytest.raises(AttributeError):
        mf.definitely_not_an_api


def test_dir_includes_delegated_methods():
    assert "upload_df" in dir(mf)


def test_aconnect_delegates():
    async def run():
        try:
            instance = await mf.aconnect(
                connection_type="local",
                connection_params={"db_path": ":memory:"},
            )
            assert isinstance(instance, MemFrame)

            ctx = await mf.aupload_df(
                pd.DataFrame({"a": [1, 2]}), filename="module_async"
            )
            assert (await ctx.ahead(1)).shape == (1, 1)
            assert await mf.alist_tables()
        finally:
            await mf.aclose()

    asyncio.run(run())


def test_class_api_still_works():
    instance = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    instance.connect()
    try:
        assert instance.list_tables() == []
    finally:
        instance.close()
