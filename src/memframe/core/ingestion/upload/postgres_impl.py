import csv
import io
from typing import Any, Dict, List

import pyarrow as pa


class PostgresUploadImpl:
    """PostgreSQL-specific upload method bodies, extracted from the monolithic Uploader."""

    # ── Table creation: Typed ──────────────────────────────────
    async def _create_final_table_typed_postgres(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {table_name} ({", ".join(col_defs)})')

    # ── Table creation: All TEXT (Legacy fallback) ─────────────────
    async def _create_final_table_all_text_postgres(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        await self.execute(f'CREATE TABLE {table_name} ({col_defs})')

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table_postgres(self, table_name: str, arrow_table: pa.Table) -> None:
        await self._insert_arrow_table_postgres_impl(table_name, arrow_table, arrow_table.schema.names)

    async def _insert_arrow_table_postgres_impl(self, final_table: str, arrow_table: pa.Table, columns: List[str]) -> None:
        full_table = arrow_table.rename_columns(columns)
        # ponytail: csv.writer emits `""` (quoted empty) for single-column-None
        # rows, which Postgres COPY CSV parses as the empty-string literal (not
        # NULL) → numeric columns crash with `invalid input syntax for type
        # double precision: ""`. Substitute `\N` (Postgres TEXT-format NULL
        # convention) and tell Postgres `null='\N'`.
        NULL_MARKER = "\\N"
        with io.BytesIO() as buf:
            text_writer = io.TextIOWrapper(buf, encoding="utf-8", write_through=True, newline="")
            writer = csv.writer(text_writer, quoting=csv.QUOTE_MINIMAL)
            for batch in full_table.to_batches(max_chunksize=10000):
                cols_data = [list(batch.column(j).to_pylist()) for j in range(batch.num_columns)]
                for i in range(batch.num_rows):
                    row = [
                        NULL_MARKER if cols_data[j][i] is None else cols_data[j][i]
                        for j in range(batch.num_columns)
                    ]
                    writer.writerow(row)
            text_writer.flush()
            text_writer.detach()
            buf.seek(0)
            schema_name, raw_table = self._split_qualified_table_name(final_table)
            await self._backend.pool.copy_to_table(
                raw_table,
                source=buf,
                columns=columns,
                schema_name=schema_name,
                format="csv",
                header=False,
                encoding="UTF8",
                null=NULL_MARKER,
            )

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample_postgres(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        rows = await self._backend.pool.fetch(f"SELECT {col_str} FROM {table_name} LIMIT {limit}")
        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pa.Table.from_pydict(data)

    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place_postgres(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        schema_name, raw_table = self._split_qualified_table_name(final_table)
        tmp_table = f'{schema_name}."{raw_table}_tmp"'

        col_defs = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            col_defs.append(f'"{col}" {target_type}')
        await self.execute(f'CREATE TABLE {tmp_table} ({", ".join(col_defs)})')

        select_parts = []
        for col in columns:
            target_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(self._build_safe_cast_postgres(col, target_type))
        await self.execute(f'INSERT INTO {tmp_table} SELECT {", ".join(select_parts)} FROM {final_table}')

        await self.drop_table(final_table)
        await self.execute(f'ALTER TABLE {tmp_table} RENAME TO {self._quote_identifier(raw_table)}')

    # ── Safe casting for Postgres ──────────────────────────────
    def _build_safe_cast_postgres(self, col: str, target_type: str) -> str:
        base = target_type.split("(")[0].upper()
        col_quoted = f'"{col}"'
        # Cast to TEXT first so TRIM works on both text and already-typed columns
        txt = f"{col_quoted}::TEXT"
        if base in ("SMALLINT", "INTEGER", "BIGINT"):
            bounds = {
                "SMALLINT": (-32768, 32767),
                "INTEGER": (-2147483648, 2147483647),
                "BIGINT": (-9223372036854775808, 9223372036854775807)
            }
            min_val, max_val = bounds[base]
            return f"""
                CASE
                    WHEN TRIM({txt}) ~ '^-?[0-9]+$' AND TRIM({txt})::NUMERIC BETWEEN {min_val} AND {max_val} THEN
                        TRIM({txt})::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        elif base in ("NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE PRECISION"):
            return f"""
                CASE
                    WHEN TRIM({txt}) ~ '^-?[0-9]*\\.?[0-9]+$' THEN
                        REPLACE(TRIM({txt}), ',', '')::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        elif base == "BOOLEAN":
            return f"""
                CASE
                    WHEN UPPER(TRIM({txt})) IN ('TRUE','T','YES','Y','1','ON') THEN TRUE
                    WHEN UPPER(TRIM({txt})) IN ('FALSE','F','NO','N','0','OFF','') THEN FALSE
                    ELSE NULL
                END AS "{col}"
            """
        elif base == "DATE":
            return f"""
                CASE
                    WHEN TRIM({txt}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}' THEN
                        TRIM({txt})::DATE
                    ELSE NULL
                END AS "{col}"
            """
        elif base in ("TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
            return f"""
                CASE
                    WHEN TRIM({txt}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}[ T][0-9]{{1,2}}:[0-9]{{1,2}}' THEN
                        TRIM({txt})::{target_type}
                    ELSE NULL
                END AS "{col}"
            """
        else:
            return f'{col_quoted} AS "{col}"'
