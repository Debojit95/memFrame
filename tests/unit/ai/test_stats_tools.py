import inspect

from memframe.wrappers.analytix.stats import StatsWrapper
from memframe_ai.tools import stats as stats_tools


def _async_a_methods(wrapper_cls):
    """Async `a*` methods on the wrapper, excluding dunder/specials."""
    return {
        name
        for name, member in inspect.getmembers(wrapper_cls, predicate=inspect.iscoroutinefunction)
        if name.startswith("a") and not name.startswith("__")
    }


def _fake_session():
    """Build a session-like object exposing only `wrappers.stats`.

    `tools(session)` only reads `session.wrappers.stats`, never calls the
    wrapper, so the wrapper instance never needs to be initialized.
    """

    class _FakeStatsWrapper:
        pass

    class _FakeWrappers:
        def __init__(self):
            # StatsWrapper.__new__ skips __init__; the test only inspects
            # tool function names, never invokes the wrapper.
            self.stats = StatsWrapper.__new__(StatsWrapper)

    class _FakeSession:
        wrappers = _FakeWrappers()

    return _FakeSession()


def test_stats_tool_covers_every_wrapper_async_method():
    wrapper_methods = _async_a_methods(StatsWrapper)
    assert wrapper_methods, "StatsWrapper exposes no async methods"

    tool_funcs = {f.__name__ for f in stats_tools.tools(_fake_session())}

    missing = []
    for wname in wrapper_methods:
        expected = wname[1:]  # strip 'a' prefix
        if expected not in tool_funcs:
            missing.append(f"StatsWrapper.{wname} → tool '{expected}' missing")

    assert not missing, "\n".join(missing)


def test_stats_tool_returns_callables_with_unique_names():
    tool_funcs = stats_tools.tools(_fake_session())
    assert len(tool_funcs) >= 37, f"expected ≥37 stats tools, got {len(tool_funcs)}"
    names = [f.__name__ for f in tool_funcs]
    assert len(names) == len(set(names)), f"duplicate tool names: {names}"