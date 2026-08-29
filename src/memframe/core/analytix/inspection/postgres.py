from typing import Any, Dict, List, Optional

from memframe.core.analytix.inspection.base import GeneralTableOps


class PostgresTableOps(GeneralTableOps):
    """PostgreSQL backend — only the hooks that differ from DuckDB."""

    def _rowid_column(self) -> str:
        return "ctid"

    def _numeric_stat_exprs(self, col: str) -> Dict[str, str]:
        return {
            "q25": f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}")',
            "median": f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{col}")',
            "q75": f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}")',
            "std": f'STDDEV_SAMP("{col}")',
        }

    async def _apply_random_seed(self, random_state: Optional[int]) -> None:
        # ponytail: Postgres setseed(x) requires x ∈ [-1, 1]; pandas accepts
        # any int. Map deterministically so large seeds don't crash.
        if random_state is None:
            return
        norm = ((random_state % 2000) - 1000) / 1000.0
        await self._exec(f"SELECT setseed({norm})")

    def _row_value(self, row: Any, col: str, columns: List[str]) -> Any:
        return row[col]

    def _row_to_dict(self, row: Any, columns: List[str]) -> Dict[str, Any]:
        return dict(row)

    def _row_to_list(self, row: Any, columns: List[str]) -> List[Any]:
        return [row[c] for c in columns]
