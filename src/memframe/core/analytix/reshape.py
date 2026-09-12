from typing import List, Union
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class ReshapingOps:

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # -------------------------
    # helpers
    # -------------------------
    async def _exec(self, sql: str):
        return await self.db.execute(sql)

    async def _fetch(self, sql: str):
        return await self.db.fetch(sql)

    def _qualified_table(self, table: str, schema: str):
        return f'{self.db.quote_identifier(schema)}.{self.db.quote_identifier(table)}'

    def _quote_identifier(self, identifier: str) -> str:
        return '"' + str(identifier).replace('"', '""') + '"'

    def _quote_literal(self, value) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _safe_numeric_expr(self, column: str) -> str:
        col_q = self._quote_identifier(column)
        if isinstance(self.db, PostgresAdapter):
            numeric_pattern = r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$"
            return (
                f"CASE WHEN trim({col_q}::text) ~ {self._quote_literal(numeric_pattern)} "
                f"THEN {col_q}::DOUBLE PRECISION ELSE NULL END"
            )
        if isinstance(self.db, DuckDBAdapter):
            return f"TRY_CAST({col_q} AS DOUBLE)"
        if isinstance(self.db, ClickHouseAdapter):
            # Cast to String first to support both numeric and string columns safely
            return f"toFloat64OrNull(toString({col_q}))"
        raise self._unsupported_backend_error()

    async def _get_table_columns(self, table: str, schema: str) -> List[str]:
        if isinstance(self.db, ClickHouseAdapter):
            rows = await self._fetch(
                f"""
                SELECT name AS column_name
                FROM system.columns
                WHERE table = {self._quote_literal(table)}
                  AND database = {self._quote_literal(schema)}
                ORDER BY position
                """
            )
        else:
            rows = await self._fetch(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = {self._quote_literal(table)}
                  AND table_schema = {self._quote_literal(schema)}
                ORDER BY ordinal_position
                """
            )
        return [r["column_name"] for r in rows]

    async def _fetch_data(self, table, schema, columns="*"):

        if columns == "*":
            cols = "*"
        else:
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            cols = ", ".join(self.db.quote_identifier(c) for c in sanitized)

        q = self._qualified_table(table, schema)
        rows = await self._fetch(f"SELECT {cols} FROM {q}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_in_chunks(self, table, schema, chunk_size, columns="*", backend=None):
        # ponytail: iterator may be consumed after cache has moved the
        # transient table from the upload schema to the transient schema.
        # Try the original location first, fall back to transient on miss.
        def _quals():
            yield self._qualified_table(table, schema)
            if backend is not None:
                t_schema = getattr(backend, "transient_schema", None)
                if t_schema and t_schema != schema:
                    yield self._qualified_table(table, t_schema)

        offset = 0

        while True:
            rows = None
            last_exc = None
            for qualified in _quals():
                try:
                    rows = await self._fetch(
                        f"SELECT {columns} FROM {qualified} LIMIT {chunk_size} OFFSET {offset}"
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
        return f"{base_table}__op_{max_op+1}"

    def _success(self, msg, df=None, **extra):
        return {
            "is_error": False,
            "message": msg,
            "error_message": None,
            "result": df,
            **extra,
        }

    def _error(self, msg):
        return {"is_error": True, "message": "", "error_message": msg}

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for reshape operation: {self.db.__class__.__name__}"
        )


    async def explode(self, table: str, schema: str, columns: Union[str, List[str]],
                      backend, data_id, chunk_size=None):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                if isinstance(columns, str):
                    columns_ = [columns]
                else:
                    columns_ = columns

                safe_cols = [SQLIdentifierSanitizer.sanitize(c) for c in columns_]
                all_cols = await self._get_table_columns(table, schema)
                base_cols = [
                    SQLIdentifierSanitizer.sanitize(c)
                    for c in all_cols
                    if SQLIdentifierSanitizer.sanitize(c) not in safe_cols
                ]

                if isinstance(self.db, DuckDBAdapter):
                    list_exprs = []
                    unnest_exprs = []

                    for col in safe_cols:
                        list_expr = f"""
                        CASE
                            WHEN "{col}" IS NULL THEN []
                            WHEN starts_with(trim("{col}"), '[') THEN
                                str_split(
                                    replace(
                                        replace(
                                            replace(
                                                replace("{col}", '[', ''),
                                            ']', ''),
                                        '''', ''),
                                    '"', ''),
                                    ','
                                )
                            ELSE list_value("{col}")
                        END
                        """
                        list_exprs.append(list_expr)
                        unnest_exprs.append(f"UNNEST({list_expr}) AS \"{col}\"")

                    exclude_cols = ", ".join(f'"{c}"' for c in safe_cols)
                    unnest_sql = ",\n".join(unnest_exprs)

                    explode_sql = f"""
                    CREATE TABLE "{schema}"."{new_table}" AS
                    SELECT
                        base.* EXCLUDE ({exclude_cols}),
                        {unnest_sql}
                    FROM {q} AS base
                    """
                elif isinstance(self.db, PostgresAdapter):
                    lateral_parts = []
                    exploded_select_cols = []
                    
                    base_select = ", ".join(
                        f'base.{self._quote_identifier(c)}' for c in base_cols
                    )

                    for col in safe_cols:
                        col_q = self._quote_identifier(col)
                        part = f"""
                        unnest(
                            CASE
                                WHEN base.{col_q} IS NULL THEN ARRAY[]::text[]
                                WHEN base.{col_q}::text LIKE '[%' THEN
                                    string_to_array(
                                        replace(
                                            replace(
                                                replace(
                                                    replace(base.{col_q}::text, '[', ''),
                                                ']', ''),
                                            '''', ''),
                                        '"', ''),
                                        ','
                                    )
                                ELSE ARRAY[base.{col_q}::text]
                            END
                        ) AS {col_q}
                        """
                        lateral_parts.append(part)
                        exploded_select_cols.append(f"exploded.{col_q}")

                    lateral_sql = ",\n".join(lateral_parts)
                    select_sql = ", ".join(
                        [part for part in (base_select, ", ".join(exploded_select_cols)) if part]
                    )

                    explode_sql = f"""
                    CREATE TABLE "{schema}"."{new_table}" AS
                    SELECT
                        {select_sql}
                    FROM {q} AS base,
                    LATERAL (
                        SELECT {lateral_sql}
                    ) AS exploded
                    """
                elif isinstance(self.db, ClickHouseAdapter):
                    array_aliases = []
                    for col in safe_cols:
                        col_q = self._quote_identifier(col)
                        # Use nested replaceAll to safely strip brackets and quotes without regex
                        clean_expr = f"replaceAll(replaceAll(replaceAll(replaceAll(CAST(base.{col_q} AS String), '[', ''), ']', ''), '\\'', ''), '\"', '')"
                        
                        array_expr = f"""
                        CASE
                            WHEN base.{col_q} IS NULL THEN emptyArrayString()
                            WHEN startsWith(trim(CAST(base.{col_q} AS String)), '[') THEN
                                arrayMap(x -> trim(x), splitByString(',', {clean_expr}))
                            ELSE [CAST(base.{col_q} AS String)]
                        END
                        """
                        array_aliases.append(f"{array_expr} AS {col}_arr")
                    
                    # To handle multiple columns with different array lengths safely,
                    # we generate an index array up to the max length and extract elements by index.
                    if len(safe_cols) == 1:
                        idx_expr = f"range(1, length({safe_cols[0]}_arr) + 1)"
                    else:
                        idx_expr = f"range(1, greatest({', '.join([f'length({col}_arr)' for col in safe_cols])}) + 1)"
                        
                    array_aliases.append(f"arrayJoin({idx_expr}) AS arr_idx")
                    
                    sub_select = []
                    if base_cols:
                        sub_select.append(", ".join(f"base.{self._quote_identifier(c)}" for c in base_cols))
                    sub_select.extend(array_aliases)
                    
                    select_cols = []
                    if base_cols:
                        select_cols.append(", ".join(f"sub.{self._quote_identifier(c)}" for c in base_cols))
                    for col in safe_cols:
                        # Elements out of bounds will default to '' (empty string), matching DuckDB/Postgres padding
                        select_cols.append(f"sub.{col}_arr[sub.arr_idx] AS {self._quote_identifier(col)}")
                        
                    explode_sql = f"""
                    CREATE TABLE "{schema}"."{new_table}" AS
                    SELECT
                        {", ".join(select_cols)}
                    FROM (
                        SELECT
                            {", ".join(sub_select)}
                        FROM {q} AS base
                    ) AS sub
                    """
                else:
                    raise self._unsupported_backend_error()

                await self._exec(explode_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return {
                    "is_error": False,
                    "message": f"explode complete on {columns}",
                    "result": df,
                    "new_table": new_table,
                }

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return {
                "is_error": True,
                "error_message": str(e),
            }
    
    async def melt(
        self,
        table,
        schema,
        id_vars: Union[List[str], None],
        value_vars: Union[List[str], None],
        backend,
        data_id,
        var_name="variable",
        value_name="value",
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                all_cols = await self._get_table_columns(table, schema)

                id_vars = id_vars or []
                id_vars = [SQLIdentifierSanitizer.sanitize(c) for c in id_vars]

                if value_vars is None:
                    value_vars = [c for c in all_cols if c not in id_vars]
                else:
                    value_vars = [SQLIdentifierSanitizer.sanitize(c) for c in value_vars]

                if not value_vars:
                    return self._error("value_vars cannot be empty")

                for col in id_vars + value_vars:
                    if col not in all_cols:
                        return self._error(f"Column '{col}' does not exist")

                if value_name in all_cols:
                    return self._error(
                        f"value_name '{value_name}' already exists in table"
                    )

                id_select = ", ".join(f'"{c}"' for c in id_vars) if id_vars else ""

                unions = []

                for col in value_vars:
                    select_parts = []

                    if id_select:
                        select_parts.append(id_select)

                    select_parts.append(f"'{col}' AS \"{var_name}\"")
                    select_parts.append(f'"{col}" AS "{value_name}"')

                    select_sql = f"""
                    SELECT {", ".join(select_parts)}
                    FROM {q}
                    """

                    unions.append(select_sql)

                final_sql = "\nUNION ALL\n".join(unions)

                create_sql = f"""
                CREATE TABLE "{schema}"."{new_table}" AS
                {final_sql}
                """

                await self._exec(create_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return self._success(
                    "melt complete",
                    df,
                    new_table=new_table,
                    id_vars=id_vars,
                    value_vars=value_vars,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))


    async def pivot(
        self,
        table,
        schema,
        index,
        columns,
        values,
        backend,
        data_id,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                if isinstance(index, str):
                    index = [index]
                if isinstance(columns, str):
                    columns = [columns]
                if isinstance(values, str):
                    values = [values]

                index = [SQLIdentifierSanitizer.sanitize(c) for c in index]
                columns = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
                values = [SQLIdentifierSanitizer.sanitize(c) for c in values]

                dup_check_sql = f"""
                SELECT {", ".join(self._quote_identifier(c) for c in index + columns)}, COUNT(*) as cnt
                FROM {q}
                GROUP BY {", ".join(self._quote_identifier(c) for c in index + columns)}
                HAVING COUNT(*) > 1
                LIMIT 1
                """

                dup = await self._fetch(dup_check_sql)
                if dup:
                    return self._error(
                        "Duplicate index/column combinations found. Use pivot_table instead."
                    )

                distinct_sql = f"""
                SELECT DISTINCT {", ".join(self._quote_identifier(c) for c in columns)}
                FROM {q}
                """

                distinct_rows = await self._fetch(distinct_sql)

                if not distinct_rows:
                    return self._error("No distinct column values found")

                select_exprs = []

                for val_col in values:
                    for row in distinct_rows:
                        conditions = []
                        col_suffix = []

                        for c in columns:
                            v = row[c]
                            if v is None:
                                conditions.append(f'{self._quote_identifier(c)} IS NULL')
                                col_suffix.append("NULL")
                            else:
                                conditions.append(f'{self._quote_identifier(c)} = {self._quote_literal(v)}')
                                col_suffix.append(str(v))

                        col_name = "_".join(col_suffix)

                        case_expr = f"""
                        MAX(
                            CASE WHEN {" AND ".join(conditions)}
                            THEN {self._quote_identifier(val_col)}
                            END
                        ) AS {self._quote_identifier(f"{val_col}_{col_name}")}
                        """

                        select_exprs.append(case_expr)

                final_sql = f"""
                CREATE TABLE "{schema}"."{new_table}" AS
                SELECT
                    {", ".join(f'"{c}"' for c in index)},
                    {", ".join(select_exprs)}
                FROM {q}
                GROUP BY {", ".join(f'"{c}"' for c in index)}
                """

                await self._exec(final_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return self._success(
                    "pivot complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))


    async def pivot_table(
        self,
        table,
        schema,
        index=None,
        columns=None,
        values=None,
        aggfunc="mean",
        fill_value=None,
        backend=None,
        data_id=None,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                def normalize(x):
                    if x is None:
                        return []
                    if isinstance(x, str):
                        return [x]
                    return x

                index = [SQLIdentifierSanitizer.sanitize(c) for c in normalize(index)]
                columns = [SQLIdentifierSanitizer.sanitize(c) for c in normalize(columns)]
                values = normalize(values)

                if isinstance(values, str):
                    values = [values]

                values = [SQLIdentifierSanitizer.sanitize(c) for c in values]

                if not values:
                    return self._error("values must be provided")

                if isinstance(aggfunc, str):
                    agg_map = {v: aggfunc for v in values}
                elif isinstance(aggfunc, dict):
                    agg_map = aggfunc
                else:
                    return self._error("Unsupported aggfunc type")

                if columns:
                    distinct_sql = f"""
                    SELECT DISTINCT {", ".join(self._quote_identifier(c) for c in columns)}
                    FROM {q}
                    """
                    distinct_rows = await self._fetch(distinct_sql)
                else:
                    distinct_rows = [{}]

                select_exprs = []

                for val_col in values:
                    func = agg_map.get(val_col, "mean").upper()
                    if func == "MEAN":
                        func = "AVG"

                    for row in distinct_rows:
                        conditions = []
                        suffix = []

                        for c in columns:
                            v = row[c]
                            if v is None:
                                conditions.append(f'{self._quote_identifier(c)} IS NULL')
                                suffix.append("NULL")
                            else:
                                conditions.append(f'{self._quote_identifier(c)} = {self._quote_literal(v)}')
                                suffix.append(str(v))

                        col_suffix = "_".join(suffix) if suffix else ""
                        value_expr = self._safe_numeric_expr(val_col)

                        expr = f"""
                        {func}(
                            CASE WHEN {" AND ".join(conditions) if conditions else "TRUE"}
                            THEN {value_expr}
                            END
                        )
                        """

                        if fill_value is not None:
                            expr = f"COALESCE({expr}, {fill_value})"

                        alias = f"{val_col}_{col_suffix}" if col_suffix else val_col

                        select_exprs.append(f"{expr} AS {self._quote_identifier(alias)}")

                group_by = ", ".join(f'"{c}"' for c in index) if index else ""

                final_sql = f"""
                CREATE TABLE "{schema}"."{new_table}" AS
                SELECT
                    {group_by + ',' if group_by else ''}
                    {", ".join(select_exprs)}
                FROM {q}
                """

                if group_by:
                    final_sql += f"\nGROUP BY {group_by}"

                await self._exec(final_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return self._success(
                    "pivot_table complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))


    async def crosstab(
        self,
        table,
        schema,
        index,
        columns,
        values=None,
        aggfunc=None,
        margins=False,
        margins_name="All",
        dropna=True,
        normalize=False,
        backend=None,
        data_id=None,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                def norm(x):
                    if x is None:
                        return []
                    if isinstance(x, str):
                        return [x]
                    return x

                index = [SQLIdentifierSanitizer.sanitize(c) for c in norm(index)]
                columns = [SQLIdentifierSanitizer.sanitize(c) for c in norm(columns)]

                if values is None:
                    value_cols = [None]
                else:
                    if isinstance(values, str):
                        value_cols = [values]
                    else:
                        value_cols = values

                    value_cols = [
                        SQLIdentifierSanitizer.sanitize(v) for v in value_cols
                    ]

                distinct_sql = f"""
                SELECT DISTINCT {", ".join(self._quote_identifier(c) for c in columns)}
                FROM {q}
                """
                distinct_rows = await self._fetch(distinct_sql)

                if not distinct_rows:
                    return self._error("No column values found")

                select_exprs = []

                for val_col in value_cols:
                    if val_col is None:
                        alias_prefix = "count"
                    else:
                        func = (aggfunc or "SUM").upper()
                        if func == "MEAN":
                            func = "AVG"
                        if func in ["SUM", "AVG"]:
                            value_expr = self._safe_numeric_expr(val_col)
                        else:
                            value_expr = self._quote_identifier(val_col)
                        alias_prefix = val_col

                    for row in distinct_rows:
                        conditions = []
                        suffix = []

                        for c in columns:
                            v = row[c]
                            if v is None:
                                conditions.append(f'{self._quote_identifier(c)} IS NULL')
                                suffix.append("NULL")
                            else:
                                conditions.append(f'{self._quote_identifier(c)} = {self._quote_literal(v)}')
                                suffix.append(str(v))

                        col_suffix = "_".join(suffix)
                        conditions_str = " AND ".join(conditions)
                        
                        if val_col is None:
                            if isinstance(self.db, ClickHouseAdapter):
                                filtered_expr = f"countIf({conditions_str})"
                            else:
                                filtered_expr = f"COUNT(*) FILTER (WHERE {conditions_str})"
                        else:
                            if isinstance(self.db, ClickHouseAdapter):
                                ch_func_map = {"SUM": "sumIf", "AVG": "avgIf", "MIN": "minIf", "MAX": "maxIf"}
                                ch_func = ch_func_map.get(func, "sumIf")
                                filtered_expr = f"{ch_func}({value_expr}, {conditions_str})"
                            else:
                                filtered_expr = f"{func}({value_expr}) FILTER (WHERE {conditions_str})"

                        if normalize in (True, "all"):
                            if val_col is None:
                                denominator = f"(SELECT COUNT(*) FROM {q})"
                            else:
                                denominator = f"(SELECT NULLIF(SUM({value_expr}), 0) FROM {q})"
                            
                            if isinstance(self.db, ClickHouseAdapter):
                                filtered_expr = f"toFloat64({filtered_expr}) / NULLIF({denominator}, 0)"
                            else:
                                filtered_expr = f"CAST(({filtered_expr}) AS DOUBLE PRECISION) / NULLIF({denominator}, 0)"

                        expr = f"""
                        {filtered_expr} AS {self._quote_identifier(f"{alias_prefix}_{col_suffix}")}
                        """

                        select_exprs.append(expr)

                group_by = ", ".join(f'"{c}"' for c in index)

                base_sql = f"""
                SELECT
                    {group_by},
                    {", ".join(select_exprs)}
                FROM {q}
                GROUP BY {group_by}
                """

                await self._exec(
                    f'CREATE TABLE "{schema}"."{new_table}" AS {base_sql}'
                )

                if margins:
                    col_names = [
                        expr.split(' AS "')[-1].strip().rstrip('"')
                        for expr in select_exprs
                    ]
                    margin_sum_sql = ", ".join(
                        f'SUM("{c}")' for c in col_names
                    )

                    margin_sql = f"""
                    INSERT INTO "{schema}"."{new_table}"
                    SELECT
                        '{margins_name}',
                        {margin_sum_sql}
                    FROM "{schema}"."{new_table}"
                    """
                    await self._exec(margin_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return self._success(
                    "crosstab complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))

    async def transpose(
        self,
        table,
        schema,
        backend,
        data_id,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                cols = await self._get_table_columns(table, schema)

                if not cols:
                    return self._error("No columns found")

                if isinstance(self.db, ClickHouseAdapter):
                    # ClickHouse HTTP is stateless, so TEMPORARY tables don't persist across requests.
                    # We use a single query with arrayJoin to avoid intermediate tables.
                    
                    # 1. Build arrays of column names and Nullable(String) casted values
                    col_names_arr = "[" + ", ".join(f"'{c}'" for c in cols) + "]"
                    col_vals_arr = "[" + ", ".join(f'CAST({self._quote_identifier(c)} AS Nullable(String))' for c in cols) + "]"
                    
                    # 2. Build the unpivot subquery
                    unpivot_sql = f"""
                    SELECT
                        tupleElement(zipped, 1) AS column_name,
                        __rowid__,
                        tupleElement(zipped, 2) AS value
                    FROM (
                        SELECT
                            __rowid__,
                            arrayJoin(arrayZip({col_names_arr}, {col_vals_arr})) AS zipped
                        FROM (
                            SELECT
                                ROW_NUMBER() OVER () AS __rowid__,
                                {", ".join(self._quote_identifier(c) for c in cols)}
                            FROM {q}
                        )
                    )
                    """
                    
                    # 3. Fetch distinct row ids to build the pivot
                    row_ids = await self._fetch(f"SELECT DISTINCT __rowid__ FROM ({unpivot_sql}) ORDER BY __rowid__")
                    
                    if not row_ids:
                        return self._error("No rows found")
                        
                    select_exprs = []
                    for r in row_ids:
                        rid = r["__rowid__"]
                        select_exprs.append(f"""
                        MAX(
                            CASE WHEN __rowid__ = {rid}
                            THEN value
                            END
                        ) AS "{rid}"
                        """)
                        
                    final_sql = f"""
                    CREATE TABLE "{schema}"."{new_table}" AS
                    SELECT
                        column_name,
                        {", ".join(select_exprs)}
                    FROM (
                        {unpivot_sql}
                    )
                    GROUP BY column_name
                    """
                    
                    await self._exec(final_sql)
                
                else:
                    # DuckDB and Postgres use temporary tables
                    temp_with_id = f"{table}__with_rowid"

                    await self._exec(f"""
                    CREATE TEMP TABLE "{temp_with_id}" AS
                    SELECT
                        ROW_NUMBER() OVER () AS __rowid__,
                        *
                    FROM {q}
                    """)

                    unions = []

                    for col in cols:
                        col_safe = SQLIdentifierSanitizer.sanitize(col)

                        unions.append(f"""
                        SELECT
                            '{col}' AS column_name,
                            __rowid__,
                            "{col_safe}" AS value
                        FROM "{temp_with_id}"
                        """)

                    unpivot_sql = "\nUNION ALL\n".join(unions)

                    temp_unpivot = f"{table}__unpivot"

                    await self._exec(f"""
                    CREATE TEMP TABLE "{temp_unpivot}" AS
                    {unpivot_sql}
                    """)

                    row_ids = await self._fetch(f"""
                        SELECT DISTINCT __rowid__
                        FROM "{temp_unpivot}"
                        ORDER BY __rowid__
                    """)

                    if not row_ids:
                        return self._error("No rows found")

                    select_exprs = []

                    for r in row_ids:
                        rid = r["__rowid__"]

                        expr = f"""
                        MAX(
                            CASE WHEN __rowid__ = {rid}
                            THEN value
                            END
                        ) AS "{rid}"
                        """

                        select_exprs.append(expr)

                    final_sql = f"""
                    CREATE TABLE "{schema}"."{new_table}" AS
                    SELECT
                        column_name,
                        {", ".join(select_exprs)}
                    FROM "{temp_unpivot}"
                    GROUP BY column_name
                    """

                    await self._exec(final_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                df = await self._fetch_data(new_table, schema)

                return self._success(
                    "transpose complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))
    
    async def rank(
        self,
        table,
        schema,
        columns: Union[str, List[str]],
        method="average",
        na_option="keep",
        ascending=True,
        pct=False,
        backend=None,
        data_id=None,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                if isinstance(columns, str):
                    columns = [columns]

                columns = [SQLIdentifierSanitizer.sanitize(c) for c in columns]

                order = "ASC" if ascending else "DESC"

                if na_option == "top":
                    nulls = "NULLS FIRST"
                elif na_option == "bottom":
                    nulls = "NULLS LAST"
                else:
                    nulls = ""

                rank_exprs = []
                rank_cols = []
                for col in columns:

                    order_clause = f'"{col}" {order} {nulls}'.strip()

                    if method == "min":
                        base_rank = f"RANK() OVER (ORDER BY {order_clause})"

                    elif method == "dense":
                        base_rank = f"DENSE_RANK() OVER (ORDER BY {order_clause})"

                    elif method == "first":
                        base_rank = f"ROW_NUMBER() OVER (ORDER BY {order_clause})"

                    elif method == "max":
                        base_rank = f"""
                        RANK() OVER (ORDER BY {order_clause})
                        + COUNT(*) OVER (PARTITION BY "{col}") - 1
                        """

                    elif method == "average":
                        base_rank = f"""
                        (
                            RANK() OVER (ORDER BY {order_clause}) +
                            RANK() OVER (ORDER BY {order_clause})
                            + COUNT(*) OVER (PARTITION BY "{col}") - 1
                        ) / 2.0
                        """

                    else:
                        return self._error(f"Unsupported method: {method}")

                    if pct:
                        if isinstance(self.db, ClickHouseAdapter):
                            base_rank = f"toFloat64(({base_rank})) / NULLIF(COUNT(*) OVER (), 0)"
                        else:
                            base_rank = f"CAST(({base_rank}) AS DOUBLE PRECISION) / NULLIF(COUNT(*) OVER (), 0)"

                    if na_option == "keep":
                        final_expr = f"""
                        CASE
                            WHEN "{col}" IS NULL THEN NULL
                            ELSE {base_rank}
                        END AS "{col}_rank"
                        """
                    else:
                        final_expr = f"{base_rank} AS \"{col}_rank\""

                    rank_exprs.append(final_expr)
                    rank_cols.append(f"{col}_rank")

                final_sql = f"""
                CREATE TABLE "{schema}"."{new_table}" AS
                SELECT
                    *,
                    {", ".join(rank_exprs)}
                FROM {q}
                """

                await self._exec(final_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                involved_cols = columns + rank_cols
                safe_cols = [SQLIdentifierSanitizer.sanitize(c) for c in involved_cols]

                df = await self._fetch_data(new_table, schema, columns=safe_cols)

                return self._success(
                    "rank complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))


    async def groupby_rank(
        self,
        table,
        schema,
        groupby: Union[str, List[str]],
        columns: Union[str, List[str]],
        method="average",
        ascending=True,
        na_option="keep",
        pct=False,
        backend=None,
        data_id=None,
        chunk_size=None,
    ):
        try:
            if isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                new_table = await self._generate_transient_table_name(
                    table, backend, data_id
                )

                q = self._qualified_table(table, schema)

                if isinstance(groupby, str):
                    groupby = [groupby]
                if isinstance(columns, str):
                    columns = [columns]

                groupby = [SQLIdentifierSanitizer.sanitize(c) for c in groupby]
                columns = [SQLIdentifierSanitizer.sanitize(c) for c in columns]

                partition_clause = ", ".join(f'"{c}"' for c in groupby)
                order_dir = "ASC" if ascending else "DESC"

                if na_option == "top":
                    nulls = "NULLS FIRST"
                elif na_option == "bottom":
                    nulls = "NULLS LAST"
                else:
                    nulls = ""

                rank_exprs = []
                rank_cols = []
                for col in columns:

                    order_clause = f'"{col}" {order_dir} {nulls}'.strip()

                    if method == "min":
                        base_rank = f"""
                        RANK() OVER (
                            PARTITION BY {partition_clause}
                            ORDER BY {order_clause}
                        )
                        """

                    elif method == "dense":
                        base_rank = f"""
                        DENSE_RANK() OVER (
                            PARTITION BY {partition_clause}
                            ORDER BY {order_clause}
                        )
                        """

                    elif method == "first":
                        base_rank = f"""
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_clause}
                            ORDER BY {order_clause}
                        )
                        """

                    elif method == "max":
                        base_rank = f"""
                        RANK() OVER (
                            PARTITION BY {partition_clause}
                            ORDER BY {order_clause}
                        )
                        + COUNT(*) OVER (
                            PARTITION BY {partition_clause}, "{col}"
                        ) - 1
                        """

                    elif method == "average":
                        base_rank = f"""
                        (
                            RANK() OVER (
                                PARTITION BY {partition_clause}
                                ORDER BY {order_clause}
                            )
                            +
                            RANK() OVER (
                                PARTITION BY {partition_clause}
                                ORDER BY {order_clause}
                            )
                            +
                            COUNT(*) OVER (
                                PARTITION BY {partition_clause}, "{col}"
                            ) - 1
                        ) / 2.0
                        """

                    else:
                        return self._error(f"Unsupported method: {method}")

                    if pct:
                        if isinstance(self.db, ClickHouseAdapter):
                            base_rank = f"toFloat64(({base_rank})) / NULLIF(COUNT(*) OVER (PARTITION BY {partition_clause}), 0)"
                        else:
                            base_rank = f"({base_rank}) / NULLIF(COUNT(*) OVER (PARTITION BY {partition_clause}), 0)"

                    if na_option == "keep":
                        final_expr = f"""
                        CASE
                            WHEN "{col}" IS NULL THEN NULL
                            ELSE {base_rank}
                        END AS "{col}_rank"
                        """
                    else:
                        final_expr = f"{base_rank} AS \"{col}_rank\""

                    rank_exprs.append(final_expr)
                    rank_cols.append(f"{col}_rank")

                final_sql = f"""
                CREATE TABLE "{schema}"."{new_table}" AS
                SELECT
                    *,
                    {", ".join(rank_exprs)}
                FROM {q}
                """

                await self._exec(final_sql)

                if chunk_size:
                    async def iterator():
                        async for c in self._fetch_in_chunks(
                            new_table, schema, chunk_size,
                            backend=backend,
                        ):
                            yield c

                    return {
                        "is_error": False,
                        "iterator": iterator(),
                        "new_table": new_table,
                    }

                involved_cols = columns + groupby + rank_cols
                safe_cols = [SQLIdentifierSanitizer.sanitize(c) for c in involved_cols]

                df = await self._fetch_data(new_table, schema, columns=safe_cols)

                return self._success(
                    "groupby rank complete",
                    df,
                    new_table=new_table,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error(str(e))