from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from memframe.core.analytix.index.base import DataIndexOps
from memframe.core.analytix.index.clickhouse_ddl import (
    ensure_setting,
    replace_table_name,
    rewrite_order_by,
)
from memframe.core.analytix._response import fail


class ClickHouseIndexOps(DataIndexOps):
    """ClickHouse data-skipping indexes and physical ORDER BY rebuilds."""

    backend = "clickhouse"
    index_name_limit = 255

    _MERGETREE_ENGINES = {
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "GraphiteMergeTree",
    }
    _INDEX_TYPE_RE = re.compile(
        r"^(minmax|bloom_filter|text|set\(\d+\)|"
        r"ngrambf_v1\(\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\)|"
        r"tokenbf_v1\(\d+\s*,\s*\d+\s*,\s*\d+\))$",
        re.IGNORECASE,
    )

    def __init__(self, db):
        super().__init__(db)
        self._server_version_cache: Optional[tuple] = None

    async def _server_version(self) -> tuple:
        if self._server_version_cache is None:
            try:
                raw = await self.db.fetchval("SELECT version()")
                parts = str(raw).split(".")
                self._server_version_cache = (
                    int(parts[0]), int(parts[1])
                ) if len(parts) >= 2 else ()
            except Exception:
                self._server_version_cache = ()
        return self._server_version_cache

    async def _projections(
        self, table: str, schema: str
    ) -> List[Dict[str, Any]]:
        try:
            return await self.db.fetch(
                "SELECT name, type, sorting_key, query "
                "FROM system.projections "
                "WHERE database = ? AND table = ? ORDER BY name",
                schema,
                table,
            )
        except Exception:
            return []

    async def _table_meta(self, table: str, schema: str) -> Dict[str, Any]:
        rows = await self.db.fetch(
            "SELECT engine, sorting_key, primary_key, partition_key "
            "FROM system.tables WHERE database = ? AND name = ?",
            schema,
            table,
        )
        return rows[0] if rows else {}

    async def _skipping_indexes(
        self, table: str, schema: str
    ) -> List[Dict[str, Any]]:
        return await self.db.fetch(
            "SELECT name, type_full, expr, granularity "
            "FROM system.data_skipping_indices "
            "WHERE database = ? AND table = ? ORDER BY name",
            schema,
            table,
        )

    async def create_index(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = True,
        expression: Optional[str] = None,
        index_type: Optional[str] = None,
        granularity: int = 1,
        materialize: bool = True,
        **options: Any,
    ) -> Dict[str, Any]:
        if unique:
            return self._error(
                "ClickHouse has no unique secondary indexes; "
                "use ReplacingMergeTree or application-level deduplication"
            )
        for key in ("method", "where", "include", "concurrently"):
            if options.get(key) not in (None, False):
                return self._error(f"ClickHouse does not support '{key}' for indexes")

        kind = (index_type or "").strip().lower()
        if kind in ("", "projection", "projection_index"):
            if granularity not in (None, 1):
                return self._error(
                    "granularity applies only to ClickHouse skipping indexes"
                )
            return await self.create_projection(
                table,
                schema,
                columns=columns,
                expression=expression,
                name=name,
                if_not_exists=if_not_exists,
                materialize=materialize,
            )
        return await self._create_skipping_index(
            table,
            schema,
            columns=columns,
            name=name,
            if_not_exists=if_not_exists,
            expression=expression,
            index_type=index_type,
            granularity=granularity,
            materialize=materialize,
        )

    async def _create_skipping_index(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        name: Optional[str] = None,
        if_not_exists: bool = True,
        expression: Optional[str] = None,
        index_type: Optional[str] = None,
        granularity: int = 1,
        materialize: bool = True,
    ) -> Dict[str, Any]:
        cols = self._as_columns(columns)
        if expression and cols:
            return self._error("Pass either `columns` or `expression`, not both")
        if expression is not None and not str(expression).strip():
            return self._error("`expression` cannot be empty")

        if expression is None:
            if len(cols) != 1:
                return self._error(
                    "ClickHouse skipping indexes take one column or an explicit "
                    "`expression` (for example tuple(\"a\", \"b\"))"
                )
            available = await self._available_columns(table, schema)
            invalid = self._validate_columns(cols, available)
            if invalid:
                return invalid
            index_expr = self._q(cols[0])
        else:
            index_expr = str(expression)

        if isinstance(granularity, bool):
            return self._error("granularity must be a positive integer")
        try:
            granularity = int(granularity)
        except (TypeError, ValueError):
            return self._error("granularity must be a positive integer")
        if granularity < 1:
            return self._error("granularity must be a positive integer")

        index_type = (index_type or "minmax").strip()
        if not self._INDEX_TYPE_RE.match(index_type):
            return self._error(
                f"Unsupported ClickHouse index type '{index_type}'"
            )

        meta = await self._table_meta(table, schema)
        if not meta:
            return self._error(f"Table '{table}' not found in '{schema}'")
        engine = str(meta.get("engine") or "")
        if engine not in self._MERGETREE_ENGINES:
            return self._error(
                f"Indexes require a local MergeTree engine; '{table}' uses {engine!r}"
            )

        index_name = self._resolve_name(name, table, cols, expression)
        existing = await self._skipping_indexes(table, schema)
        for row in existing:
            if row["name"] == index_name:
                if if_not_exists:
                    return self._ok(
                        f"Index '{index_name}' already exists",
                        self._row(row),
                    )
                return self._error(
                    f"Index '{index_name}' already exists on '{table}'"
                )
        for row in await self._projections(table, schema):
            if row["name"] == index_name:
                return self._error(
                    f"Name '{index_name}' is already used by a projection"
                )

        sql = (
            f"ALTER TABLE {self._qualified(table, schema)} "
            f"ADD INDEX IF NOT EXISTS {self._q(index_name)} {index_expr} "
            f"TYPE {index_type} GRANULARITY {granularity}"
        )
        try:
            await self.db.execute(sql)
            if materialize:
                await self.db.execute(
                    f"ALTER TABLE {self._qualified(table, schema)} "
                    f"MATERIALIZE INDEX {self._q(index_name)} "
                    f"SETTINGS mutations_sync = 1"
                )
        except Exception as exc:
            return self._error(f"create_index error: {exc}")

        refreshed = await self._skipping_indexes(table, schema)
        for row in refreshed:
            if row["name"] == index_name:
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
                "expression": expression or (cols[0] if cols else None),
                "type": index_type,
                "granularity": granularity,
                "materialized": materialize,
                "source": "skipping",
                "droppable": True,
                "backend": "clickhouse",
            },
        )

    async def create_projection(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        expression: Optional[str] = None,
        name: Optional[str] = None,
        if_not_exists: bool = True,
        materialize: bool = True,
    ) -> Dict[str, Any]:
        version = await self._server_version()
        if version and version < (25, 5):
            return self._error(
                "Projection indexes require ClickHouse >= 25.5; "
                "pass index_type='minmax'/'bloom_filter' for a skipping index "
                "or upgrade the server"
            )

        cols = self._as_columns(columns)
        if expression is not None and cols:
            return self._error("Pass either `columns` or `expression`, not both")
        if expression is not None:
            if not str(expression).strip():
                return self._error("`expression` cannot be empty")
            order_sql = str(expression).strip()
        else:
            if not cols:
                return self._error("Provide at least one column for the projection")
            available = await self._available_columns(table, schema)
            invalid = self._validate_columns(cols, available)
            if invalid:
                return invalid
            order_sql = "(" + ", ".join(self._q(c) for c in cols) + ")"

        meta = await self._table_meta(table, schema)
        if not meta:
            return self._error(f"Table '{table}' not found in '{schema}'")
        engine = str(meta.get("engine") or "")
        if engine not in self._MERGETREE_ENGINES:
            return self._error(
                f"Projections require a local MergeTree engine; '{table}' uses {engine!r}"
            )

        projection_name = self._resolve_name(name, table, cols, expression)
        projections = await self._projections(table, schema)
        for row in projections:
            if row["name"] == projection_name:
                if if_not_exists:
                    return self._ok(
                        f"Projection '{projection_name}' already exists",
                        self._projection_row(row, schema, table),
                    )
                return self._error(
                    f"Projection '{projection_name}' already exists on '{table}'"
                )
        for row in await self._skipping_indexes(table, schema):
            if row["name"] == projection_name:
                return self._error(
                    f"Name '{projection_name}' is already used by a skipping index"
                )

        sql = (
            f"ALTER TABLE {self._qualified(table, schema)} "
            f"ADD PROJECTION IF NOT EXISTS {self._q(projection_name)} "
            f"(SELECT _part_offset ORDER BY {order_sql})"
        )
        try:
            await self.db.execute(sql)
            if materialize:
                await self.db.execute(
                    f"ALTER TABLE {self._qualified(table, schema)} "
                    f"MATERIALIZE PROJECTION {self._q(projection_name)} "
                    f"SETTINGS mutations_sync = 1"
                )
        except Exception as exc:
            return self._error(f"create_projection error: {exc}")

        refreshed = await self._projections(table, schema)
        for row in refreshed:
            if row["name"] == projection_name:
                return self._ok(
                    f"Created projection '{projection_name}' on '{table}'",
                    self._projection_row(row, schema, table),
                )
        return self._ok(
            f"Created projection '{projection_name}' on '{table}'",
            {
                "name": projection_name,
                "schema": schema,
                "table": table,
                "columns": cols,
                "expression": expression,
                "source": "projection_index",
                "materialized": materialize,
                "droppable": True,
                "backend": "clickhouse",
            },
        )

    async def drop_projection(
        self,
        table: str,
        schema: str,
        name: str,
        if_exists: bool = True,
    ) -> Dict[str, Any]:
        projection_name = self._fit_name(name)
        rows = await self._projections(table, schema)
        if not any(row["name"] == projection_name for row in rows):
            if if_exists:
                return self._ok(
                    f"Projection '{projection_name}' does not exist",
                    {"name": projection_name, "dropped": False},
                )
            return self._error(f"Projection '{projection_name}' does not exist")

        try:
            await self.db.execute(
                f"ALTER TABLE {self._qualified(table, schema)} "
                f"DROP PROJECTION IF EXISTS {self._q(projection_name)} "
                f"SETTINGS mutations_sync = 1"
            )
        except Exception as exc:
            return self._error(f"drop_projection error: {exc}")
        return self._ok(
            f"Dropped projection '{projection_name}'",
            {
                "name": projection_name,
                "table": table,
                "schema": schema,
                "source": "projection",
                "dropped": True,
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
            return self._error("ClickHouse does not support concurrent drops")

        index_name = self._fit_name(name)
        projection_rows = await self._projections(table, schema)
        if any(row["name"] == index_name for row in projection_rows):
            return await self.drop_projection(
                table, schema, index_name, if_exists=if_exists
            )

        rows = await self._skipping_indexes(table, schema)
        if not any(row["name"] == index_name for row in rows):
            if if_exists:
                return self._ok(
                    f"Index '{index_name}' does not exist",
                    {"name": index_name, "dropped": False},
                )
            return self._error(f"Index '{index_name}' does not exist")

        try:
            await self.db.execute(
                f"ALTER TABLE {self._qualified(table, schema)} "
                f"DROP INDEX IF EXISTS {self._q(index_name)} "
                f"SETTINGS mutations_sync = 1"
            )
        except Exception as exc:
            return self._error(f"drop_index error: {exc}")
        return self._ok(
            f"Dropped index '{index_name}'",
            {"name": index_name, "table": table, "schema": schema, "dropped": True},
        )

    async def list_indexes(self, table: str, schema: str) -> Dict[str, Any]:
        projections = await self._projections(table, schema)
        result = [
            self._projection_row(row, schema, table) for row in projections
        ]

        rows = await self._skipping_indexes(table, schema)
        result.extend(self._row(row) for row in rows)

        meta = await self._table_meta(table, schema)
        if meta:
            result.append(
                {
                    "name": None,
                    "schema": schema,
                    "table": table,
                    "source": "order_by",
                    "engine": meta.get("engine"),
                    "sorting_key": meta.get("sorting_key"),
                    "primary_key": meta.get("primary_key"),
                    "partition_key": meta.get("partition_key"),
                    "droppable": False,
                    "backend": "clickhouse",
                }
            )
        return self._ok(f"Indexes on '{table}'", result)

    async def set_order_by(
        self,
        table: str,
        schema: str,
        columns: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        dry_run: bool = True,
        keep_backup: bool = False,
        verify: str = "count",
        **options: Any,
    ) -> Dict[str, Any]:
        cols = self._as_columns(columns)
        if not cols:
            return self._error("Provide at least one ORDER BY column")
        available = await self._available_columns(table, schema)
        invalid = self._validate_columns(cols, available)
        if invalid:
            return invalid

        pk_cols = self._as_columns(primary_key) if primary_key is not None else None
        if pk_cols:
            missing = [c for c in pk_cols if c not in available]
            if missing:
                return self._error(f"Primary-key columns not found: {missing}", missing)
            if cols[: len(pk_cols)] != pk_cols:
                return self._error(
                    "PRIMARY KEY must be a prefix of ORDER BY"
                )

        if verify not in (None, "count"):
            return self._error(
                "Only verify='count' (or None) is supported for set_order_by"
            )

        meta = await self._table_meta(table, schema)
        if not meta:
            return self._error(f"Table '{table}' not found in '{schema}'")
        engine = str(meta.get("engine") or "")
        if engine not in self._MERGETREE_ENGINES:
            return self._error(
                f"ORDER BY rebuild requires a local MergeTree engine; "
                f"'{table}' uses {engine!r}"
            )

        ddl_rows = await self.db.fetch(
            "SELECT create_table_query FROM system.tables "
            "WHERE database = ? AND name = ?",
            schema,
            table,
        )
        if not ddl_rows:
            return self._error(f"Could not read CREATE TABLE for '{table}'")
        ddl = ddl_rows[0].get("create_table_query") or ""

        order_sql = "(" + ", ".join(self._q(c) for c in cols) + ")"
        pk_sql = (
            "(" + ", ".join(self._q(c) for c in pk_cols) + ")"
            if pk_cols else None
        )
        try:
            rebuilt_ddl = rewrite_order_by(ddl, order_sql, pk_sql)
            if any(
                "nullable" in str(available.get(c, "")).lower() for c in cols
            ):
                # memFrame uploads columns as Nullable; CH requires this
                # table setting to permit a Nullable sorting key.
                rebuilt_ddl = ensure_setting(rebuilt_ddl, "allow_nullable_key", "1")
        except Exception as exc:
            return self._error(f"Could not rewrite CREATE TABLE: {exc}")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        temp_table = self._fit_name(f"{table}__rebuild_{stamp}")
        backup_table = self._fit_name(f"{table}__old_{stamp}")
        temp_qualified = f"{self._q(schema)}.{self._q(temp_table)}"
        try:
            temp_ddl = replace_table_name(rebuilt_ddl, temp_qualified)
        except Exception as exc:
            return self._error(f"Could not rewrite table name: {exc}")

        qualified = self._qualified(table, schema)
        try:
            rows_before = await self.db.fetchval(f"SELECT count() FROM {qualified}")
        except Exception:
            rows_before = None
        try:
            bytes_on_disk = await self.db.fetchval(
                "SELECT sum(bytes_on_disk) FROM system.parts "
                "WHERE database = ? AND table = ? AND active",
                schema,
                table,
            ) or 0
        except Exception:
            bytes_on_disk = 0

        plan = {
            "table": table,
            "schema": schema,
            "old_order_by": meta.get("sorting_key"),
            "new_order_by": tuple(cols),
            "primary_key": tuple(pk_cols) if pk_cols else None,
            "rows": rows_before,
            "bytes_on_disk": bytes_on_disk,
            "ddl": temp_ddl,
            "temp_table": temp_table,
            "backup_table": None,
            "rebuilt": False,
        }
        if dry_run:
            return self._ok("Dry run: ORDER BY rebuild not executed", plan)

        columns_sql = ", ".join(self._q(c) for c in available)
        try:
            await self.db.execute(temp_ddl)
            await self.db.execute(
                f"INSERT INTO {temp_qualified} ({columns_sql}) "
                f"SELECT {columns_sql} FROM {qualified}"
            )
            if verify == "count" and rows_before is not None:
                rows_after = await self.db.fetchval(
                    f"SELECT count() FROM {temp_qualified}"
                ) or 0
                if int(rows_after) != int(rows_before):
                    await self.db.execute(f"DROP TABLE IF EXISTS {temp_qualified}")
                    return self._error(
                        f"Row count mismatch after copy: {rows_before} -> {rows_after}"
                    )
            await self.db.execute(
                f"RENAME TABLE {qualified} TO {self._q(schema)}.{self._q(backup_table)}, "
                f"{temp_qualified} TO {qualified}"
            )
        except Exception as exc:
            try:
                await self.db.execute(f"DROP TABLE IF EXISTS {temp_qualified}")
            except Exception:
                pass
            return self._error(f"set_order_by error: {exc}")

        if keep_backup:
            plan["backup_table"] = backup_table
        else:
            try:
                await self.db.execute(
                    f"DROP TABLE {self._q(schema)}.{self._q(backup_table)}"
                )
            except Exception as exc:
                plan["backup_warning"] = (
                    f"New ORDER BY is live, but old table "
                    f"'{backup_table}' could not be dropped: {exc}"
                )
        plan.update({"rebuilt": True, "rows": rows_before})
        return self._ok(
            f"Rebuilt '{table}' with ORDER BY {tuple(cols)}",
            plan,
        )

    @staticmethod
    def _row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": row["name"],
            "type": row.get("type_full"),
            "expression": row.get("expr"),
            "granularity": row.get("granularity"),
            "source": "skipping",
            "droppable": True,
            "backend": "clickhouse",
        }

    @staticmethod
    def _projection_row(
        row: Dict[str, Any],
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict[str, Any]:
        query = str(row.get("query") or "")
        is_index = "_part_offset" in query or query.strip().upper().startswith("INDEX")
        return {
            "name": row["name"],
            "schema": schema,
            "table": table,
            "source": "projection_index" if is_index else "projection",
            "projection_type": row.get("type"),
            "sorting_key": row.get("sorting_key"),
            "definition": query,
            "droppable": True,
            "backend": "clickhouse",
        }

    @staticmethod
    def _error(message: str, involved_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        return fail(message, involved_cols)
