"""Central exception hierarchy for memFrame."""


class MemFrameError(Exception):
    """Base class for all memFrame exceptions."""


class ConnectionNotReady(MemFrameError):
    """Backend is not connected or its pool is not set."""


class BackendNotSupported(MemFrameError):
    """Unsupported database backend requested."""


class DataNotFound(MemFrameError):
    """Dataset, data_id, or registry entry not found."""


class OperationError(MemFrameError):
    """An analytics/plot operation failed."""


class ConfigurationError(MemFrameError):
    """Invalid connection parameters, inputs, or configuration."""
