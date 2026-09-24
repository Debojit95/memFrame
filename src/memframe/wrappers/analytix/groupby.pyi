# Inside GroupBy class:
from typing import Any


class GroupBy:
    _stats: Any
    _cum: Any
    _window: Any
    group_cols: list[str]

    def __init__(self, stats_builder: Any, cum_builder: Any, window_builder: Any) -> None: ...
    def __repr__(self) -> str: ...