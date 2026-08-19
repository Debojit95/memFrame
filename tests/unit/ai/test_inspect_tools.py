import inspect

from memframe.wrappers.analytix.inspection import TableOpsWrapper
from memframe_ai.tools import inspect as inspect_tools


def _async_a_methods(wrapper_cls):
    return {
        name
        for name, member in inspect.getmembers(wrapper_cls, predicate=inspect.iscoroutinefunction)
        if name.startswith("a") and not name.startswith("__")
    }


def _fake_session():
    class _FakeWrappers:
        def __init__(self):
            self.inspection = TableOpsWrapper.__new__(TableOpsWrapper)

    class _FakeSession:
        wrappers = _FakeWrappers()

    return _FakeSession()


def test_inspect_tool_covers_every_wrapper_async_method():
    wrapper_methods = _async_a_methods(TableOpsWrapper)
    assert wrapper_methods, "TableOpsWrapper exposes no async methods"

    tool_funcs = {f.__name__ for f in inspect_tools.tools(_fake_session())}

    missing = []
    for wname in wrapper_methods:
        expected = wname[1:]
        if expected not in tool_funcs:
            missing.append(f"TableOpsWrapper.{wname} → tool '{expected}' missing")

    assert not missing, "\n".join(missing)


def test_inspect_tool_returns_callables_with_unique_names():
    tool_funcs = inspect_tools.tools(_fake_session())
    assert len(tool_funcs) >= 21, f"expected ≥21 inspection tools, got {len(tool_funcs)}"
    names = [f.__name__ for f in tool_funcs]
    assert len(names) == len(set(names)), f"duplicate tool names: {names}"