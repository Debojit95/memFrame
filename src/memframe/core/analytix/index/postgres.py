from __future__ import annotations

from typing import Any, Dict, List, Optional

from memframe.core.analytix.index.base import DataIndexOps


class PostgresIndexOps(DataIndexOps):
    """PostgreSQL native indexes via CREATE/DROP INDEX and pg_index."""

    backend = "postgres"
    index_name_limit = 63
    _METHODS = {"btree", "hash", "gin", "gist", "brin"}

    async def _catalog(
        self, schema: str, table: Optional[str] = None, name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT i.relname AS index_name,
                   t.relname AS table_name,
                   ix.indisunique AS is_unique,
                   ix.indisvalid AS is_valid,
                   am.amname AS method,
                   pg_get_indexdef(ix.indexrelid) AS definition,
                   con.contype AS constraint_type
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            LEFT JOIN pg_constraint con ON con.conindid = ix.indexrelid
            WHERE n.nspname = $1
        """
        args: List[Any] = [schema]
        if table is not None:
            sql += " AND t.relname = $2"
            args.append(table)
        if name is not None:
            sql += f" AND i.relname = ${len(args) + 1}"
            args.append(name)
        sql += " ORDER BY i.relname"
        try:
            return await self.db.fetch(sql, *args)
        except Exception:
            return []

    @staticmethod
    def _row(row: Dict[str, Any], schema: str) -> Dict[str, Any]:
        constraint = row.get("constraint_type")
        return {
            "name": row["index_name"],
            "schema": schema,
            "table": row["table_name"],
            "columns": None,
            "unique": bool(row["is_unique"]),
            "valid": bool(row["is_valid"]),
            "method": row["method"],
            "definition": row["definition"],
            "source": "constraint" if constraint else "index",
            "constraint_type": constraint,
            "droppable": constraint is None,
            "backend": "postgres",
        }

    async def create_index(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = True,
        method: Optional[str] = None,
        where: Optional[str] = None,
        include: Optional[List[str]] = None,
        concurrently: bool = False,
        **options: Any,
    ) -> Dict[str, Any]:
        unsupported = {
            "expression": options.get("expression") is not None,
            "index_type": options.get("index_type") is not None,
            "granularity": options.get("granularity") not in (None, 1),
            "materialize": options.get("materialize") not in (None, True),
        }
        for key, is_set in unsupported.items():
            if is_set:
                return self._error(f"PostgreSQL does not support '{key}' for indexes")

        cols = self._as_columns(columns)
        available = await self._available_columns(table, schema)
        invalid = self._validate_columns(cols, available)
        if invalid:
            return invalid

        method_sql = (method or "").strip().lower()
        if method_sql and method_sql not in self._METHODS:
            return self._error(
                f"Unsupported index method '{method}' "
                f"(choose from {', '.join(sorted(self._METHODS))})"
            )

        include_cols = self._as_columns(include) if include is not None else []
        if include_cols:
            missing = [c for c in include_cols if c not in available]
            if missing:
                return self._error(f"INCLUDE columns not found: {missing}", missing)

        index_name = self._resolve_name(name, table, cols)
        existing = await self._catalog(schema, name=index_name)
        if existing:
            row = existing[0]
            if row["table_name"] == table and if_not_exists:
                return self._ok(
                    f"Index '{index_name}' already exists",
                    self._row(row, schema),
                )
            if row["table_name"] != table:
                return self._error(
                    f"Index '{index_name}' already exists on table "
                    f"'{row['table_name']}' in schema '{schema}'"
                )
            return self._error(
                f"Index '{index_name}' already exists on '{table}'"
            )

        unique_sql = "UNIQUE " if unique else ""
        conc_sql = "CONCURRENTLY " if concurrently else ""
        if_not = "IF NOT EXISTS " if if_not_exists else ""
        using_sql = f"USING {method_sql} " if method_sql else ""
        col_sql = ", ".join(self._q(c) for c in cols)
        include_sql = (
            f" INCLUDE ({', '.join(self._q(c) for c in include_cols)})"
            if include_cols else ""
        )
        where_sql = f" WHERE {where}" if where else ""
        # PostgreSQL forbids a schema-qualified index name in CREATE INDEX;
        # the index is created in the table's schema.
        sql = (
            f"CREATE {unique_sql}INDEX {conc_sql}{if_not}"
            f"{self._q(index_name)} "
            f"ON {self._qualified(table, schema)} {using_sql}"
            f"({col_sql}){include_sql}{where_sql}"
        )
        try:
            await self.db.execute(sql)
        except Exception as exc:
            return self._error(f"create_index error: {exc}")

        refreshed = await self._catalog(schema, name=index_name)
        if refreshed:
            return self._ok(
                f"Created index '{index_name}' on '{table}'",
                self._row(refreshed[0], schema),
            )
        return self._ok(
            f"Created index '{index_name}' on '{table}'",
            {
                "name": index_name,
                "schema": schema,
                "table": table,
                "columns": cols,
                "unique": unique,
                "method": method_sql or "btree",
                "definition": sql,
                "source": "index",
                "droppable": True,
                "backend": "postgres",
            },
        )

    async def drop_index(
        self,
        table: str,
        schema: str,
        name: str,
        if_exists: bool = True,
        concurrently: bool = False,
        **options: Any,
    ) -> Dict[str, Any]:
        index_name = self._fit_name(name)
        rows = await self._catalog(schema, name=index_name)
        if not rows:
            if if_exists:
                return self._ok(
                    f"Index '{index_name}' does not exist",
                    {"name": index_name, "dropped": False},
                )
            return self._error(f"Index '{index_name}' does not exist")
        row = rows[0]
        if row["table_name"] != table:
            return self._error(
                f"Index '{index_name}' belongs to table '{row['table_name']}', "
                f"not '{table}'"
            )
        if row.get("constraint_type"):
            return self._error(
                f"Index '{index_name}' backs a constraint and cannot be dropped "
                "with DROP INDEX; drop the constraint instead"
            )

        conc_sql = "CONCURRENTLY " if concurrently else ""
        sql = (
            f"DROP INDEX {conc_sql}IF EXISTS "
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
        rows = await self._catalog(schema, table=table)
        return self._ok(
            f"Indexes on '{table}'",
            [self._row(row, schema) for row in rows],
        )

    @staticmethod
    def _error(message: str) -> Dict[str, Any]:
        from memframe.core.analytix._response import fail

        return fail(message)
