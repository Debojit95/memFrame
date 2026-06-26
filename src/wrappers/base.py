"""Dynamic base wrapper that aggregates all wrapper classes.

Any class in this package whose name ends with ``Wrapper`` (except
``BaseWrapper`` itself) is automatically included in ``BaseWrapper``.
This keeps the base API extensible as new wrapper modules are added.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import TYPE_CHECKING, List, Tuple, Type, cast

WRAPPER_DIR = Path(__file__).resolve().parent
WRAPPER_PACKAGE = __name__.rsplit(".", 1)[0]


def _iter_wrapper_module_names() -> List[str]:
    module_names: List[str] = []
    for module_info in pkgutil.iter_modules([str(WRAPPER_DIR)]):
        name = module_info.name
        # Context-level facades are delegated from ContextManager, not MemFrame.
        if name.startswith("_") or name in {"base", "inspect", "cleaning", "arithmetic", "comparison",
            "cumulative", "datetime", "filtering", "groupby_cumulative", "groupby_window", "groupby_stats", "merging", "preprocessing", "reshape", "selection", "sorting", "stats", "window", "scatter"}:
            continue
        module_names.append(name)
    return sorted(module_names)


def _discover_wrapper_classes() -> List[Type]:
    classes: List[Type] = []
    seen_class_ids = set()

    for module_name in _iter_wrapper_module_names():
        module = importlib.import_module(f"{WRAPPER_PACKAGE}.{module_name}")

        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__:
                continue
            if cls.__name__ == "BaseWrapper":
                continue
            if cls.__name__.endswith("Wrapper"):
                cls_id = id(cls)
                if cls_id in seen_class_ids:
                    continue
                seen_class_ids.add(cls_id)
                classes.append(cls)

    # Stable order keeps MRO deterministic.
    return sorted(classes, key=lambda c: c.__name__)


def _build_base_wrapper() -> Type:
    wrapper_bases = tuple(_discover_wrapper_classes())
    if not wrapper_bases:
        wrapper_bases = (object,)

    return type(
        "BaseWrapper",
        wrapper_bases,
        {
            "__module__": __name__,
            "__doc__": (
                "Composed wrapper that exposes methods from all discovered "
                "wrapper classes in src.wrappers."
            ),
        },
    )


if TYPE_CHECKING:
    # IDE/type-checker visibility:
    # Keep these imports updated when you add new wrapper classes and want
    # static autocomplete for their method signatures on MemFrame/BaseWrapper.
    from src.wrappers.ops import OpsWrapper
    from src.wrappers.upload import UploadWrapper

    class BaseWrapper(OpsWrapper, UploadWrapper):
        """Typing-time base wrapper with concrete signatures for IDEs."""

        pass
else:
    BaseWrapper = cast(Type, _build_base_wrapper())


def available_wrappers() -> Tuple[str, ...]:
    """Return wrapper class names currently included in BaseWrapper."""
    return tuple(base.__name__ for base in BaseWrapper.__bases__ if base is not object)


def generate_typing_stub(stub_path: Path | None = None) -> Path:
    """
    Generate ``base.pyi`` so IDEs can suggest wrapper method signatures.

    Run this after adding new wrapper classes.
    """
    target = stub_path or (WRAPPER_DIR / "base.pyi")
    wrappers = _discover_wrapper_classes()

    lines: List[str] = [
        "from __future__ import annotations",
        "",
    ]

    for cls in wrappers:
        lines.append(f"from {cls.__module__} import {cls.__name__}")

    lines.append("")
    if wrappers:
        bases = ", ".join(cls.__name__ for cls in wrappers)
    else:
        bases = "object"

    lines.extend(
        [
            f"class BaseWrapper({bases}): ...",
            "",
            "def available_wrappers() -> tuple[str, ...]: ...",
            "",
        ]
    )

    target.write_text("\n".join(lines), encoding="utf-8")
    return target


__all__ = ["BaseWrapper", "available_wrappers", "generate_typing_stub"]
