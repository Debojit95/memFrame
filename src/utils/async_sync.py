import asyncio
import inspect
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Optional, ParamSpec, TypeVar, cast

_executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor()
P = ParamSpec("P")
R = TypeVar("R")


def async_to_sync(func: Callable[P, Awaitable[R]]) -> Callable[P, R]:
    """
    Decorator to wrap an async function and make it callable from sync code.

    Features:
    - Uses inspect.iscoroutinefunction (Python 3.14+ safe)
    - Handles running event loop
    - Uses ThreadPoolExecutor (no per-call thread spawn)
    - Proper exception propagation
    """

    if not inspect.iscoroutinefunction(func):
        raise TypeError("Decorator can only be applied to async functions")

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            # Check if we're already inside an event loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop → simplest & fastest path
            return asyncio.run(func(*args, **kwargs))

        # ⚠️ Running loop detected → offload to thread pool
        def runner():
            return asyncio.run(func(*args, **kwargs))

        future = _executor.submit(runner)
        return future.result()  # blocks until done (sync behavior)

    # Preserve call signature for editor/tooling introspection.
    wrapper.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return cast(Callable[P, R], wrapper)
