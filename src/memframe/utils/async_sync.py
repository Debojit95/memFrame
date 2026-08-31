import asyncio
import inspect
import threading
from functools import wraps
from typing import Awaitable, Callable, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")

# ponytail: one shared background event loop for every sync-API call. A fresh
# asyncio.run per call (the old behavior) tore down and rebuilt the asyncpg
# pool each time and used the DuckDB connection from a different thread per
# call. All sync work now serializes on this single thread. If sync throughput
# ever matters, batch calls on the async API instead of growing this.
_loop = asyncio.new_event_loop()
_thread = threading.Thread(target=_loop.run_forever, name="memframe-sync-loop", daemon=True)
_thread.start()


def async_to_sync(func: Callable[P, Awaitable[R]]) -> Callable[P, R]:
    """
    Decorator to wrap an async function and make it callable from sync code.

    The coroutine always runs on the shared background loop, so loop-affine
    resources (pools, connections, locks) are created and used on one loop
    for the lifetime of the process. Exceptions propagate to the caller.
    """

    if not inspect.iscoroutinefunction(func):
        raise TypeError("Decorator can only be applied to async functions")

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        coro = func(*args, **kwargs)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is _loop:
            # ponytail: we're ON the shared loop (a coroutine called the sync
            # form). Blocking here would deadlock; there is no way to return
            # synchronously. Raise instead of silently spinning a nested loop.
            coro.close()
            raise RuntimeError(
                f"Sync API '{func.__name__}' called from inside a coroutine "
                f"running on the memframe sync loop — use the async form "
                f"('a{func.__name__}') instead."
            )

        return asyncio.run_coroutine_threadsafe(coro, _loop).result()

    # Preserve call signature for editor/tooling introspection.
    wrapper.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return cast(Callable[P, R], wrapper)
