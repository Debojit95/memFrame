import pytest

from memframe.core.analytix._response import fail, ok, unwrap_response
from memframe.exceptions import OperationError


@pytest.mark.parametrize("value", [None, 1, 1.5, "value", {"plain": True}])
def test_unwrap_leaves_non_operation_values_unchanged(value):
    assert unwrap_response(value) is value


def test_unwrap_returns_operation_result():
    result = {"value": 1}

    assert unwrap_response(ok(result=result)) == result


def test_unwrap_returns_stream_iterator():
    iterator = object()

    assert unwrap_response(ok(iterator=iterator)) is iterator


def test_unwrap_raises_operation_error():
    with pytest.raises(OperationError, match="bad input"):
        unwrap_response(fail("bad input"))
