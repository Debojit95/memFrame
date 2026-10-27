"""memFrame - Database-backed DataFrame operations.

The module itself is a facade over one process-global ``MemFrame`` instance::

    import memframe as mf
    mf.connect(connection_type="local", connection_params={"db_path": "db.duckdb"})
    ctx = mf.upload_df(df, filename="customers")

Any ``MemFrame`` attribute not defined here is forwarded to that default
instance. Use the ``MemFrame`` class directly when you need more than one
connection.
"""

import logging
from importlib.metadata import PackageNotFoundError, version as _package_version
from typing import Any, Dict, List, Optional

from .main import MemFrame
from .core.ingestion.datatype_detector import Backend
from .db_manager.setup import DatabaseBackend
from .db_manager.context import ContextManager
from .dashboard import DashboardManager

try:
    __version__ = _package_version("memframe")
except PackageNotFoundError:  # running from a source checkout
    __version__ = "0.0.0"

# ponytail: one global default instance for the module API. It is unconnected
# until connect()/aconnect(); construction performs no I/O, so importing the
# package still has no side effects. Use MemFrame(...) for parallel instances.
_default: MemFrame = MemFrame()


def connect(
    connection_type: str = "local",
    connection_params: Optional[Dict[str, Any]] = None,
    deep_cache: Optional[bool] = None,
) -> MemFrame:
    """Connect the module-level default instance (sync form).

    Replaces any previous default connection. Returns the connected instance
    so ``mf = memframe.connect(...)`` also works.
    """
    global _default
    close()
    instance = MemFrame(connection_type, connection_params, deep_cache)
    instance.connect()
    _default = instance
    return instance


async def aconnect(
    connection_type: str = "local",
    connection_params: Optional[Dict[str, Any]] = None,
    deep_cache: Optional[bool] = None,
) -> MemFrame:
    """Connect the module-level default instance (async form)."""
    global _default
    await aclose()
    instance = MemFrame(connection_type, connection_params, deep_cache)
    await instance.aconnect()
    _default = instance
    return instance


def close() -> None:
    """Close the default connection and leave a fresh unconnected instance."""
    global _default
    current = _default
    _default = MemFrame()
    current.close()


async def aclose() -> None:
    """Async form of :func:`close`."""
    global _default
    current = _default
    _default = MemFrame()
    await current.aclose()


def DataFrame(
    data: Any = None,
    index: Any = None,
    columns: Optional[List] = None,
    dtype: Any = None,
    copy: Optional[bool] = None,
    *,
    filename: Optional[str] = None,
    dtypes: Optional[Dict[str, str]] = None,
) -> ContextManager:
    """Pandas-style dataset constructor on the default connection.

    Mirrors ``pd.DataFrame(...)`` and uploads the result as a new table.
    ``MemFrame`` is the connection class; this is the dataset constructor.
    """
    return _default.DataFrame(
        data, index=index, columns=columns, dtype=dtype, copy=copy,
        filename=filename, dtypes=dtypes,
    )


async def aDataFrame(
    data: Any = None,
    index: Any = None,
    columns: Optional[List] = None,
    dtype: Any = None,
    copy: Optional[bool] = None,
    *,
    filename: Optional[str] = None,
    dtypes: Optional[Dict[str, str]] = None,
) -> ContextManager:
    """Async form of :func:`DataFrame`."""
    return await _default.aDataFrame(
        data, index=index, columns=columns, dtype=dtype, copy=copy,
        filename=filename, dtypes=dtypes,
    )


def __getattr__(name: str) -> Any:
    """Forward unknown module attributes to the default MemFrame instance."""
    if name.startswith("_"):
        raise AttributeError(name)
    return getattr(_default, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_default)))

# ponytail: libraries must not configure logging for the app. We set the
# package logger to WARNING and attach a NullHandler so no per-operation INFO
# lines leak to notebook stderr (Colab/Kaggle render those as error blocks)
# while real warnings/errors still propagate. Opt in with enable_logging().
_logger = logging.getLogger("memFrame")
_logger.setLevel(logging.WARNING)
_logger.addHandler(logging.NullHandler())


def enable_logging(level: int = logging.INFO) -> None:
    """Opt in to memFrame's operational logs (silent by default).

    Attaches a single stderr ``StreamHandler`` to the ``memFrame`` logger and
    sets its level. Covers ``memFrame.ai`` (the AI layer) too.
    """
    logger = logging.getLogger("memFrame")
    logger.setLevel(level)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)


__all__ = [
    "MemFrame",
    "Backend",
    "DatabaseBackend",
    "ContextManager",
    "DashboardManager",
    "enable_logging",
    "connect",
    "aconnect",
    "close",
    "aclose",
    "DataFrame",
    "aDataFrame",
]
