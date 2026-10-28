from __future__ import annotations

from typing import Any, Dict, List, Optional

from memframe.core.analytix.index.base import DataIndexOps
from memframe.core.analytix._response import fail


class DuckDBIndexOps(DataIndexOps):
    """DuckDB ART indexes plus constraint-backed indexes from the catalog."""

    backend = "duckdb"
    index_name_limit = 255

    async def _index_rows(
        self, schema: str, table: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        sql = (
            "SELECT schema_name, index_name, table_name, is_unique, sql "
            "FROM duckdb_indexes() WHERE schema_name = ?"
        )
        args: List[Any] = [schema]
        if table is not None:
            sql += " AND table_name = ?"
            args.append(table)
        sql += " ORDER BY index_name"
        return await self.db.fetch(sql, *args)

    @staticmethod
    def _row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": row["index_name"],
            "schema": row["schema_name"],
            "table": row["table_name"],
            "columns": None,
            "unique": bool(row["is_unique"]),
            "definition": row["sql"],
            "source": "index",
            "droppable": True,
            "backend": "duckdb",
        }

    async def create_index(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = True,
        **options: Any,
    ) -> Dict[str, Any]:
        unsupported = {
            "expression": options.get("expression") is not None,
            "method": options.get("method") is not None,
            "where": options.get("where") is not None,
            "include": options.get("include") is not None,
            "concurrently": bool(options.get("concurrently")),
            "index_type": options.get("index_type") is not None,
            "granularity": options.get("granularity") not in (None, 1),
            "materialize": options.get("materialize") not in (None, True),
        }
        for key, is_set in unsupported.items():
            if is_set:
                return self._unsupported(key)

        cols = self._as_columns(columns)
        available = await self._available_columns(table, schema)
        invalid = self._validate_columns(cols, available)
        if invalid:
            return invalid

        index_name = self._resolve_name(name, table, cols)
        rows = await self._index_rows(schema)
        for row in rows:
            if row["index_name"] != index_name:
                continue
            if row["table_name"] == table:
                if if_not_exists:
                    return self._ok("Index already exists", self._row(row))
                return self._error(
                    f"Index '{index_name}' already exists on '{table}'"
                )
            return self._error(
                f"Index '{index_name}' already exists on table "
                f"'{row['table_name']}' in schema '{schema}'"
            )

        unique_sql = "UNIQUE " if unique else ""
        col_sql = ", ".join(self._q(c) for c in cols)
        if_not = "IF NOT EXISTS " if if_not_exists else ""
        sql = (
            f"CREATE {unique_sql}INDEX {if_not}{self._q(index_name)} "
            f"ON {self._qualified(table, schema)} ({col_sql})"
        )
        try:
            await self.db.execute(sql)
        except Exception as exc:
            return self._error(f"create_index error: {exc}")

        refreshed = await self._index_rows(schema, table)
        for row in refreshed:
            if row["index_name"] == index_name:
                return self._ok(
                    f"Created index '{index_name}' on '{table}'",
                    self._row(row),
                )
        return self._ok(
            f"Created index '{index_name}' on '{table}'",
            {
                "name": index_name,
                "schema": schema,
                "table": table,
                "columns": cols,
                "unique": unique,
                "definition": sql,
                "source": "index",
                "droppable": True,
                "backend": "duckdb",
            },
        )

    async def drop_index(
        self,
        table: str,
        schema: str,
        name: str,
        if_exists: bool = True,
        **options: Any,
    ) -> Dict[str, Any]:
        if options.get("concurrently") not in (None, False):
            return self._unsupported("concurrently")

        index_name = self._fit_name(name)
        rows = await self._index_rows(schema)
        row = next((r for r in rows if r["index_name"] == index_name), None)
        if row is None:
            if if_exists:
                return self._ok(
                    f"Index '{index_name}' does not exist",
                    {"name": index_name, "dropped": False},
                )
            return self._error(f"Index '{index_name}' does not exist")
        if row["table_name"] != table:
            return self._error(
                f"Index '{index_name}' belongs to table '{row['table_name']}', "
                f"not '{table}'"
            )

        sql = (
            f"DROP INDEX IF EXISTS "
            f"{self._q(schema)}.{self._q(index_name)}"
        )
        try:
            await self.db.execute(sql)
        except Exception as exc:
            return self._error(f"drop_index error: {exc}")
        return self._ok(
            f"Dropped index '{index_name}'",
            {"name": index_name, "table": table, "schema": schema, "dropped": True},
        )

    async def list_indexes(self, table: str, schema: str) -> Dict[str, Any]:
        rows = await self._index_rows(schema, table)
        result = [self._row(row) for row in rows]
        try:
            constraints = await self.db.fetch(
                "SELECT constraint_name, constraint_type, constraint_column_names "
                "FROM duckdb_constraints() "
                "WHERE schema_name = ? AND table_name = ? "
                "AND constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
                "ORDER BY constraint_name",
                schema,
                table,
            )
        except Exception:
            constraints = []
        for row in constraints:
            result.append(
                {
                    "name": row.get("constraint_name"),
                    "schema": schema,
                    "table": table,
                    "columns": list(row.get("constraint_column_names") or []),
                    "unique": True,
                    "definition": None,
                    "source": "constraint",
                    "constraint_type": row.get("constraint_type"),
                    "droppable": False,
                    "backend": "duckdb",
                }
            )
        return self._ok(f"Indexes on '{table}'", result)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _unsupported(self, option: str) -> Dict[str, Any]:
        return self._error(f"DuckDB does not support '{option}' for indexes")

    def _error(self, message: str) -> Dict[str, Any]:
        return fail(message)
