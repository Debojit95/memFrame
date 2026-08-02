import asyncio
import inspect

import pytest

from memframe.utils.async_sync import async_to_sync


async def _async_add(a, b):
    await asyncio.sleep(0)
    return a + b


async def _async_fail():
    await asyncio.sleep(0)
    raise ValueError("expected failure")


def test_wraps_async_and_returns_value():
    sync_add = async_to_sync(_async_add)
    assert sync_add(2, 3) == 5


def test_sync_call_outside_loop():
    assert async_to_sync(_async_add)(10, 20) == 30


def test_exception_propagation():
    sync_fail = async_to_sync(_async_fail)
    with pytest.raises(ValueError, match="expected failure"):
        sync_fail()


def test_preserves_function_name_and_signature():
    sync_add = async_to_sync(_async_add)
    assert sync_add.__name__ == "_async_add"
    assert str(inspect.signature(sync_add)) == "(a, b)"


def test_call_inside_running_loop_offloads_to_thread():
    """Calling the sync wrapper from within an async context must not deadlock."""
    sync_add = async_to_sync(_async_add)

    async def run():
        return await asyncio.to_thread(sync_add, 4, 6)

    assert asyncio.run(run()) == 10


def test_rejects_non_async_function():
    with pytest.raises(TypeError):
        async_to_sync(lambda x: x)
