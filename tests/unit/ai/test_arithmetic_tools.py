import inspect

from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
from memframe_ai.tools import arithmetic as arithmetic_tools


def _async_a_methods(wrapper_cls):
    return {
        name
        for name, member in inspect.getmembers(wrapper_cls, predicate=inspect.iscoroutinefunction)
        if name.startswith("a") and not name.startswith("__")
    }


def _fake_session():
    class _FakeWrappers:
        def __init__(self):
            self.arithmetic = ArithmeticWrapper.__new__(ArithmeticWrapper)

    class _FakeSession:
        wrappers = _FakeWrappers()

    return _FakeSession()


def test_arithmetic_tool_covers_every_wrapper_async_method():
    wrapper_methods = _async_a_methods(ArithmeticWrapper)
    assert wrapper_methods, "ArithmeticWrapper exposes no async methods"

    tool_funcs = {f.__name__ for f in arithmetic_tools.tools(_fake_session())}

    missing = []
    for wname in wrapper_methods:
        expected = wname[1:]
        if expected not in tool_funcs:
            missing.append(f"ArithmeticWrapper.{wname} → tool '{expected}' missing")

    assert not missing, "\n".join(missing)


def test_arithmetic_tool_returns_callables_with_unique_names():
    tool_funcs = arithmetic_tools.tools(_fake_session())
    assert len(tool_funcs) >= 24, f"expected ≥24 arithmetic tools, got {len(tool_funcs)}"
    names = [f.__name__ for f in tool_funcs]
    assert len(names) == len(set(names)), f"duplicate tool names: {names}"