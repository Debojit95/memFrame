"""memFrame - Database-backed DataFrame operations."""

import logging

from .main import MemFrame
from .core.ingestion.datatype_detector import Backend
from .db_manager.setup import DatabaseBackend
from .db_manager.context import ContextManager
from .dashboard import DashboardManager

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
]
