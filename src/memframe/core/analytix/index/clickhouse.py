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
        rows = await self._skipping_indexes(table, schema)
        result = [self._row(row) for row in rows]

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
    def _error(message: str, involved_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        return fail(message, involved_cols)
