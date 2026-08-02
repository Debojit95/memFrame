import pytest

from memframe.exceptions import (
    MemFrameError,
    ConnectionNotReady,
    BackendNotSupported,
    DataNotFound,
    OperationError,
    ConfigurationError,
)


def _all_exceptions():
    return [
        ConnectionNotReady,
        BackendNotSupported,
        DataNotFound,
        OperationError,
        ConfigurationError,
    ]


@pytest.mark.parametrize("exc_cls", _all_exceptions())
def test_all_exceptions_inherit_from_memframe_error(exc_cls):
    assert issubclass(exc_cls, MemFrameError)
    assert issubclass(exc_cls, Exception)


@pytest.mark.parametrize("exc_cls", _all_exceptions())
def test_exception_instantiation_and_message(exc_cls):
    exc = exc_cls("boom")
    assert str(exc) == "boom"
    assert isinstance(exc, MemFrameError)


def test_memframe_error_is_base():
    assert issubclass(MemFrameError, Exception)


def test_catch_as_memframe_error():
    with pytest.raises(MemFrameError):
        raise ConnectionNotReady("Not connected")
