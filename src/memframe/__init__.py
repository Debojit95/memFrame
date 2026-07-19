from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_main_path = Path(__file__).resolve().parent.parent / "main.py"
_spec = spec_from_file_location("_memframe_main", _main_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load MemFrame from {_main_path}")

_module = module_from_spec(_spec)
_spec.loader.exec_module(_module)

MemFrame = _module.MemFrame

__all__ = ["MemFrame"]
