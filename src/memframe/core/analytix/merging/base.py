# ruff: noqa: E741
"""Merge/join/concat core — shared SQL engine.

``DataMergeOps`` (this module) holds the backend-agnostic implementation and the
DuckDB/PostgreSQL-flavoured dialect hooks. ClickHouse overrides those hooks in
``clickhouse.py``; the factory in ``factory.py`` picks the subclass by adapter.

Construct via :func:`make_merge_ops` rather than instantiating directly.
"""

from typing import Any, Dict, List, Optional, Union
import traceback
from datetime import datetime, timezone

import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class DataMergeOps:
    """
    Core merge/join operations (SQL JOIN based).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # -----------------------------
    # Helpers
    # -----------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    def _qualified(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    def _unique_alias(self, alias: str, used: set[str]) -> str:
        safe_alias = SQLIdentifierSanitizer.sanitize(alias)
        candidate = safe_alias
        suffix = 1
        while candidate.lower() in used:
            candidate = SQLIdentifierSanitizer.sanitize(f"{safe_alias}_{suffix}")
            suffix += 1
        used.add(candidate.lower())
        return candidate

    def _success(self, msg, df, **extra):
        return {
            "is_error": False,
            "message": msg,
            "error_message": None,
            "result": df,
            **extra,
        }

    def _error(self, msg):
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
        }

    async def _fetch_data(self, table: str, schema: str):
        rows = await self._fetch(f"SELECT * FROM {self._qualified(table, schema)}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_in_chunks(
        self,
        table: str,
        schema: str,
        chunk_size: int,
        backend=None,
    ):
        # ponytail: iterator may be consumed after cache has moved the
        # transient table from the upload schema to the transient schema.
        # Try the original location first, fall back to transient on miss.
        def _quals():
            yield self._qualified(table, schema)
            if backend is not None:
                t_schema = getattr(backend, "transient_schema", None)
                if t_schema and t_schema != schema:
                    yield self._qualified(table, t_schema)

        offset = 0

        while True:
            rows = None
            last_exc = None
            for qualified in _quals():
                try:
                    rows = await self._fetch(
                        f"SELECT * FROM {qualified} LIMIT {chunk_size} OFFSET {offset}"
                    )
                    break
                except Exception as exc:
                    # ponytail: only fall back on "table does not exist"
                    if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
                        last_exc = exc
                        continue
                    raise
            if rows is None:
                if last_exc is not None:
                    raise last_exc
                break
            if not rows:
                break
            yield pd.DataFrame([dict(r) for r in rows])
            offset += chunk_size

    async def _generate_transient_table_name(self, base_table, backend, data_id):
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{(max_op or 0) + 1}"

    async def _resolve_output_table_name(
        self,
        base_table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)

        if backend is not None and data_id:
            candidate = await self._generate_transient_table_name(
                safe_base,
                backend,
                data_id,
            )
        else:
            candidate = f"{safe_base}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    # -----------------------------
    # Backend dialect hooks
    # (DuckDB/PostgreSQL defaults; ClickHouse overrides in clickhouse.py)
    # -----------------------------
    def _auto_cast_join_columns(self, l_col: str, r_col: str, l_type, r_type):
        """Cast a timestamp key against a date key so the join type-checks."""
        if "TIMESTAMP" in str(l_type).upper() and "DATE" in str(r_type).upper():
            l_col = f"CAST({l_col} AS DATE)"
        elif "DATE" in str(l_type).upper() and "TIMESTAMP" in str(r_type).upper():
            r_col = f"CAST({r_col} AS DATE)"
        return l_col, r_col

    def _create_table_as(self, schema: str, new_table: str, select_sql: str) -> str:
        return (
            f"CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table)} AS\n"
            f"{select_sql}"
        )

    # -----------------------------
    # MAIN MERGE
    # -----------------------------
    async def merge(
        self,
        left_table: str,
        right_table: str,
        schema: str,
        right_schema: Optional[str] = None,
        how: str = "inner",
        on: Optional[Union[str, List[str]]] = None,
        left_on: Optional[Union[str, List[str]]] = None,
        right_on: Optional[Union[str, List[str]]] = None,
        suffixes: tuple = ("_x", "_y"),
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:

        try:
            if backend is None or data_id is None:
                return self._error("backend and data_id required")

            if chunk_size is not None and chunk_size <= 0:
                return self._error("chunk_size must be > 0")

            # -----------------------------
            # Normalize keys
            # -----------------------------
            if on:
                left_on = right_on = on

            if not left_on or not right_on:
                return self._error("Must provide 'on' or both 'left_on' and 'right_on'")

            if isinstance(left_on, str):
                left_on = [left_on]
            if isinstance(right_on, str):
                right_on = [right_on]

            if len(left_on) != len(right_on):
                return self._error("left_on and right_on must match length")

            # -----------------------------
            # Sanitize keys
            # -----------------------------
            left_keys = [SQLIdentifierSanitizer.sanitize(c) for c in left_on]
            right_keys = [SQLIdentifierSanitizer.sanitize(c) for c in right_on]

            # -----------------------------
            # JOIN TYPE
            # -----------------------------
            join_map = {
                "inner": "INNER JOIN",
                "left": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "outer": "FULL OUTER JOIN",
                "cross": "CROSS JOIN",
            }

            if how not in join_map and how not in ("left_anti", "right_anti"):
                return self._error(f"Unsupported join type: {how}")

            # -----------------------------
            # TABLES
            # -----------------------------
            left_q = self._qualified(left_table, schema)
            right_q = self._qualified(right_table, right_schema or schema)

            # -----------------------------
            # SELECT columns with suffix handling
            # -----------------------------
            safe_left_table = SQLIdentifierSanitizer.sanitize(left_table)
            safe_right_table = SQLIdentifierSanitizer.sanitize(right_table)
            safe_schema = SQLIdentifierSanitizer.sanitize(schema)
            safe_right_schema = SQLIdentifierSanitizer.sanitize(right_schema or schema)

            left_names = list(
                (
                    await self.db.get_column_types(
                        safe_left_table,
                        safe_schema,
                    )
                ).keys()
            )
            right_names = list(
                (
                    await self.db.get_column_types(
                        safe_right_table,
                        safe_right_schema,
                    )
                ).keys()
            )

            if not left_names:
                return self._error(f"No columns found in left table: {left_table}")
            if not right_names:
                return self._error(f"No columns found in right table: {right_table}")

            overlap = set(left_names) & set(right_names)

            select_parts = []
            used_aliases = set()

            for col in left_names:
                safe = SQLIdentifierSanitizer.sanitize(col)
                alias = col + suffixes[0] if col in overlap else col
                alias_safe = self._unique_alias(alias, used_aliases)
                select_parts.append(
                    f'l.{self.db.quote_identifier(safe)} AS {self.db.quote_identifier(alias_safe)}'
                )

            for col in right_names:
                safe = SQLIdentifierSanitizer.sanitize(col)
                alias = col + suffixes[1] if col in overlap else col
                alias_safe = self._unique_alias(alias, used_aliases)
                select_parts.append(
                    f'r.{self.db.quote_identifier(safe)} AS {self.db.quote_identifier(alias_safe)}'
                )

            select_sql = ", ".join(select_parts)

            # -----------------------------
            # JOIN CONDITION
            # -----------------------------
            if how == "cross":
                join_sql = "CROSS JOIN"
                condition_sql = ""
            else:
                left_types_raw = await self.db.get_column_types(safe_left_table, safe_schema)
                right_types_raw = await self.db.get_column_types(safe_right_table, safe_right_schema)

                left_types = {k.lower(): v for k, v in left_types_raw.items()}
                right_types = {k.lower(): v for k, v in right_types_raw.items()}

                conditions = []

                for l, r in zip(left_keys, right_keys):
                    l_type = left_types.get(l.lower())
                    r_type = right_types.get(r.lower())

                    l_col = f'l.{self.db.quote_identifier(l)}'
                    r_col = f'r.{self.db.quote_identifier(r)}'

                    l_col, r_col = self._auto_cast_join_columns(l_col, r_col, l_type, r_type)

                    conditions.append(f"{l_col} = {r_col}")

                condition_sql = " AND ".join(conditions)

            # -----------------------------
            # ANTI JOIN HANDLING
            # -----------------------------
            if how == "left_anti":
                join_sql = "LEFT JOIN"
                where_clause = f"WHERE r.{self.db.quote_identifier(right_keys[0])} IS NULL"
            elif how == "right_anti":
                join_sql = "RIGHT JOIN"
                where_clause = f"WHERE l.{self.db.quote_identifier(left_keys[0])} IS NULL"
            else:
                join_sql = join_map.get(how)
                where_clause = ""

            # -----------------------------
            # CREATE TABLE
            # -----------------------------
            new_table = await self._resolve_output_table_name(
                base_table=left_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
            )

            select_body = f"""SELECT {select_sql}
                    FROM {left_q} l
                    {join_sql} {right_q} r
                    {"ON " + condition_sql if condition_sql else ""}
                    {where_clause}"""

            create_sql = self._create_table_as(schema, new_table, select_body)

            await self._exec(create_sql)

            if chunk_size is None:
                merged_df = await self._fetch_data(new_table, schema)

                return self._success(
                    f"{how} merge completed",
                    merged_df,
                    new_table=new_table,
                    how=how,
                    left_on=left_on,
                    right_on=right_on,
                )

            async def iterator():
                async for chunk in self._fetch_in_chunks(
                    new_table,
                    schema,
                    chunk_size,
                    backend=backend,
                ):
                    yield chunk

            return {
                "is_error": False,
                "message": f"{how} merge completed (streaming)",
                "error_message": None,
                "iterator": iterator(),
                "chunk_size": chunk_size,
                "new_table": new_table,
                "how": how,
                "left_on": left_on,
                "right_on": right_on,
            }

        except Exception as e:
            return self._error(f"merge error: {str(e)}\n{traceback.format_exc()}")

    # -----------------------------
    # MAIN JOIN
    # -----------------------------
    async def join(
        self,
        left_table: str,
        right_table: str,
        schema: str,
        right_schema: Optional[str] = None,
        how: str = "left",
        on: Optional[Union[str, List[str]]] = None,
        lsuffix: str = "",
        rsuffix: str = "",
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:

        try:
            if backend is None or data_id is None:
                return self._error("backend and data_id required")

            if chunk_size is not None and chunk_size <= 0:
                return self._error("chunk_size must be > 0")

            # -----------------------------
            # TABLES
            # -----------------------------
            left_q = self._qualified(left_table, schema)
            right_q = self._qualified(right_table, right_schema or schema)

            safe_left_table = SQLIdentifierSanitizer.sanitize(left_table)
            safe_right_table = SQLIdentifierSanitizer.sanitize(right_table)
            safe_schema = SQLIdentifierSanitizer.sanitize(schema)
            safe_right_schema = SQLIdentifierSanitizer.sanitize(right_schema or schema)

            # -----------------------------
            # COLUMN FETCH
            # -----------------------------
            left_types_raw = await self.db.get_column_types(safe_left_table, safe_schema)
            right_types_raw = await self.db.get_column_types(safe_right_table, safe_right_schema)

            left_cols = list(left_types_raw.keys())
            right_cols = list(right_types_raw.keys())

            if not left_cols:
                return self._error(f"No columns found in left table: {left_table}")
            if not right_cols:
                return self._error(f"No columns found in right table: {right_table}")

            # -----------------------------
            # KEY RESOLUTION
            # -----------------------------
            if on is None:
                common = list(set(left_cols) & set(right_cols))
                if not common:
                    return self._error("No common columns for join")
                left_on = right_on = common
            else:
                if isinstance(on, str):
                    on = [on]
                left_on = right_on = on

            # sanitize keys
            left_keys = [SQLIdentifierSanitizer.sanitize(c) for c in left_on]
            right_keys = [SQLIdentifierSanitizer.sanitize(c) for c in right_on]

            # -----------------------------
            # JOIN TYPE
            # -----------------------------
            join_map = {
                "inner": "INNER JOIN",
                "left": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "outer": "FULL OUTER JOIN",
                "cross": "CROSS JOIN",
            }

            if how not in join_map and how not in ("left_anti", "right_anti"):
                return self._error(f"Unsupported join type: {how}")

            # -----------------------------
            # SELECT CLAUSE (SUFFIX HANDLING)
            # -----------------------------
            overlap = set(left_cols) & set(right_cols)

            select_parts = []
            used_aliases = set()

            for col in left_cols:
                safe = SQLIdentifierSanitizer.sanitize(col)
                alias = col + lsuffix if col in overlap else col
                alias_safe = self._unique_alias(alias, used_aliases)

                select_parts.append(
                    f'l.{self.db.quote_identifier(safe)} AS {self.db.quote_identifier(alias_safe)}'
                )

            for col in right_cols:
                safe = SQLIdentifierSanitizer.sanitize(col)
                if col in overlap:
                    alias = col + (rsuffix if rsuffix else "_right")
                else:
                    alias = col
                alias_safe = self._unique_alias(alias, used_aliases)

                select_parts.append(
                    f'r.{self.db.quote_identifier(safe)} AS {self.db.quote_identifier(alias_safe)}'
                )

            select_sql = ", ".join(select_parts)

            # -----------------------------
            # JOIN CONDITION
            # -----------------------------
            if how == "cross":
                join_sql = "CROSS JOIN"
                condition_sql = ""
            else:
                # normalize type dict keys
                left_types = {k.lower(): v for k, v in left_types_raw.items()}
                right_types = {k.lower(): v for k, v in right_types_raw.items()}

                conditions = []

                for l, r in zip(left_keys, right_keys):
                    l_type = left_types.get(l.lower())
                    r_type = right_types.get(r.lower())

                    l_col = f'l.{self.db.quote_identifier(l)}'
                    r_col = f'r.{self.db.quote_identifier(r)}'

                    l_col, r_col = self._auto_cast_join_columns(l_col, r_col, l_type, r_type)

                    conditions.append(f"{l_col} = {r_col}")

                condition_sql = " AND ".join(conditions)

            # -----------------------------
            # ANTI JOIN
            # -----------------------------
            if how == "left_anti":
                join_sql = "LEFT JOIN"
                where_clause = f"WHERE r.{self.db.quote_identifier(right_keys[0])} IS NULL"
            elif how == "right_anti":
                join_sql = "RIGHT JOIN"
                where_clause = f"WHERE l.{self.db.quote_identifier(left_keys[0])} IS NULL"
            else:
                join_sql = join_map.get(how)
                where_clause = ""

            # -----------------------------
            # CREATE TABLE
            # -----------------------------
            new_table = await self._resolve_output_table_name(
                base_table=left_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
            )

            select_body = f"""SELECT {select_sql}
                    FROM {left_q} l
                    {join_sql} {right_q} r
                    {"ON " + condition_sql if condition_sql else ""}
                    {where_clause}"""

            create_sql = self._create_table_as(schema, new_table, select_body)

            await self._exec(create_sql)

            # -----------------------------
            # RETURN
            # -----------------------------
            if chunk_size is None:
                df = await self._fetch_data(new_table, schema)

                return self._success(
                    f"{how} join completed",
                    df,
                    new_table=new_table,
                    how=how,
                    on=on,
                )

            async def iterator():
                async for chunk in self._fetch_in_chunks(
                    new_table,
                    schema,
                    chunk_size,
                    backend=backend,
                ):
                    yield chunk

            return {
                "is_error": False,
                "message": f"{how} join completed (streaming)",
                "error_message": None,
                "iterator": iterator(),
                "chunk_size": chunk_size,
                "new_table": new_table,
                "how": how,
                "on": on,
            }

        except Exception as e:
            return self._error(f"join error: {str(e)}\n{traceback.format_exc()}")

    # -----------------------------
    # MAIN CONCAT
    # -----------------------------
    async def concat(
        self,
        tables: List[str],
        schema: str,
        table_schemas: Optional[List[str]] = None,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:

        try:
            if backend is None or data_id is None:
                return self._error("backend and data_id required")

            if not tables or len(tables) < 2:
                return self._error("Need at least 2 tables to concat")

            if axis not in (0, 1):
                return self._error("axis must be 0 or 1")

            if join not in ("outer", "inner"):
                return self._error("join must be 'outer' or 'inner'")

            safe_schema = SQLIdentifierSanitizer.sanitize(schema)
            if table_schemas is not None and len(table_schemas) != len(tables):
                return self._error("table_schemas must match tables in length")
            # ponytail: inputs may span schemas (e.g. a merged transient table
            # plus an upload table); the output lives in `schema` (the left side).
            schemas = (
                [SQLIdentifierSanitizer.sanitize(s) for s in table_schemas]
                if table_schemas is not None
                else [safe_schema] * len(tables)
            )

            # -----------------------------
            # FETCH COLUMN METADATA
            # -----------------------------
            table_cols = {}
            for t, t_schema in zip(tables, schemas):
                safe_t = SQLIdentifierSanitizer.sanitize(t)
                cols = await self.db.get_column_types(safe_t, t_schema)
                table_cols[t] = list(cols.keys())

            # -----------------------------
            # AXIS = 0 (ROW CONCAT)
            # -----------------------------
            if axis == 0:

                if join == "outer":
                    all_cols = sorted(set().union(*table_cols.values()))
                else:
                    all_cols = sorted(set.intersection(*map(set, table_cols.values())))

                if not all_cols:
                    return self._error("No columns available after join resolution")

                select_statements = []

                for t, t_schema in zip(tables, schemas):
                    qualified = self._qualified(t, t_schema)
                    cols = table_cols[t]

                    select_parts = []
                    for col in all_cols:
                        safe = SQLIdentifierSanitizer.sanitize(col)

                        if col in cols:
                            select_parts.append(f"{self.db.quote_identifier(safe)}")
                        else:
                            select_parts.append(f"NULL AS {self.db.quote_identifier(safe)}")

                    select_sql = ", ".join(select_parts)

                    select_statements.append(f"SELECT {select_sql} FROM {qualified}")

                union_sql = "\nUNION ALL\n".join(select_statements)

                # -----------------------------
                # IGNORE INDEX (row_number)
                # -----------------------------
                if ignore_index:
                    final_sql = f"""
                        SELECT ROW_NUMBER() OVER () AS __index__, *
                        FROM (
                            {union_sql}
                        )
                    """
                else:
                    final_sql = union_sql

            # -----------------------------
            # AXIS = 1 (COLUMN CONCAT)
            # -----------------------------
            else:
                # assign row_number to each table
                subqueries = []
                output_select_parts = []
                used_aliases = set()
                row_key = "__memframe_concat_rn"

                for i, t in enumerate(tables):
                    qualified = self._qualified(t, schemas[i])
                    cols = table_cols[t]

                    select_cols = ", ".join(
                        f"{self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(c))}"
                        for c in cols
                    )

                    subqueries.append(f"""
                        (
                            SELECT
                                ROW_NUMBER() OVER () AS {self.db.quote_identifier(row_key)},
                                {select_cols}
                            FROM {qualified}
                        ) t{i}
                    """)

                    for col in cols:
                        safe = SQLIdentifierSanitizer.sanitize(col)
                        alias_safe = self._unique_alias(safe, used_aliases)
                        output_select_parts.append(
                            f"t{i}.{self.db.quote_identifier(safe)} AS {self.db.quote_identifier(alias_safe)}"
                        )

                # JOIN them
                join_sql = subqueries[0]

                for i in range(1, len(subqueries)):
                    if join == "outer":
                        join_type = "FULL OUTER JOIN"
                    else:
                        join_type = "INNER JOIN"

                    join_sql = f"""
                        {join_sql}
                        {join_type} {subqueries[i]}
                        ON t0.{self.db.quote_identifier(row_key)} = t{i}.{self.db.quote_identifier(row_key)}
                    """

                final_sql = f"""
                    SELECT {", ".join(output_select_parts)}
                    FROM {join_sql}
                """

            # -----------------------------
            # CREATE TABLE
            # -----------------------------
            new_table = await self._resolve_output_table_name(
                base_table=tables[0],
                schema=schema,
                backend=backend,
                data_id=data_id,
            )

            create_sql = self._create_table_as(schema, new_table, final_sql)

            await self._exec(create_sql)

            # -----------------------------
            # RETURN
            # -----------------------------
            if chunk_size is None:
                df = await self._fetch_data(new_table, schema)

                return self._success(
                    f"concat axis={axis} completed",
                    df,
                    new_table=new_table,
                    axis=axis,
                    join=join,
                )

            async def iterator():
                async for chunk in self._fetch_in_chunks(
                    new_table,
                    schema,
                    chunk_size,
                    backend=backend,
                ):
                    yield chunk

            return {
                "is_error": False,
                "message": f"concat axis={axis} completed (streaming)",
                "error_message": None,
                "iterator": iterator(),
                "chunk_size": chunk_size,
                "new_table": new_table,
                "axis": axis,
                "join": join,
            }

        except Exception as e:
            return self._error(f"concat error: {str(e)}\n{traceback.format_exc()}")
