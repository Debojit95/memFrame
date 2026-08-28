from typing import Any, Dict, List

import pyarrow as pa


class ClickHouseUploadImpl:
    """ClickHouse-specific upload method bodies, extracted from the monolithic Uploader."""

    def _postgres_type_to_clickhouse(self, pg_type: str) -> str:
        base = pg_type.split("(")[0].upper()
        mapping = {
            "TEXT": "String",
            "VARCHAR": "String",
            "CHAR": "String",
            "INTEGER": "Int32",
            "INT": "Int32",
            "BIGINT": "Int64",
            "SMALLINT": "Int16",
            "NUMERIC": "Decimal(38, 10)",
            "DECIMAL": "Decimal(38, 10)",
            "REAL": "Float32",
            "FLOAT": "Float32",
            "FLOAT4": "Float32",
            "DOUBLE": "Float64",
            "FLOAT8": "Float64",
            "DOUBLE PRECISION": "Float64",
            "BOOLEAN": "UInt8",
            "BOOL": "UInt8",
            "DATE": "Date",
            "TIMESTAMP": "DateTime",
            "DATETIME": "DateTime",
        }
        return mapping.get(base, "String")

    def _build_safe_cast_clickhouse(self, col: str, pg_type: str) -> str:
        ch_type = self._postgres_type_to_clickhouse(pg_type)
        col_q = f"`{col}`"
        if ch_type == "String":
            return f"{col_q} AS `{col}`"
        # to*OrNull functions only accept String arguments; wrap with toString
        # to handle columns that ClickHouse native ingestion already typed.
        if ch_type == "Int32":
            return f"toInt32OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "Int64":
            return f"toInt64OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "Int16":
            return f"toInt16OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "Float32":
            return f"toFloat32OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "Float64":
            return f"toFloat64OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "UInt8":
            return f"toUInt8OrNull(toString({col_q})) AS `{col}`"
        if ch_type == "Date":
            return f"toDateOrNull(toString({col_q})) AS `{col}`"
        if ch_type == "DateTime":
            return f"toDateTimeOrNull(toString({col_q})) AS `{col}`"
        if "Decimal" in ch_type:
            return f"toDecimal64OrNull(toString({col_q}), 10) AS `{col}`"
        return f"toString({col_q}) AS `{col}`"

    # ── Table creation: Typed ─────────────────────────────────
    async def _create_final_table_typed_clickhouse(self, table_name: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {table_name} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    # ── Table creation: All TEXT (Legacy fallback) ────────────
    async def _create_final_table_all_text_clickhouse(self, table_name: str, columns: List[str]) -> None:
        col_defs = ", ".join(f"`{col}` String" for col in columns)
        await self.execute(
            f"CREATE TABLE {table_name} ({col_defs}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

    # ── PyArrow Stream Upload ───────────────────────────────────
    async def _insert_arrow_table_clickhouse(self, table_name: str, arrow_table: pa.Table) -> None:
        # Chunk large tables to avoid ClickHouse memory limits
        for batch in arrow_table.to_batches(max_chunksize=100000):
            await self._backend.insert_arrow_table(table_name, pa.Table.from_batches([batch]))

    # ── Sampling ────────────────────────────────────────────────
    async def _fetch_arrow_sample_clickhouse(self, table_name: str, columns: List[str], limit: int) -> pa.Table:
        col_str = ", ".join(self._quote_identifier(c) for c in columns)
        res = await self._backend.pool.client.query(f"SELECT {col_str} FROM {table_name} LIMIT {limit}")
        data = {col: [row[i] for row in res.result_rows] for i, col in enumerate(res.column_names)}
        return pa.Table.from_pydict(data)

    # ── Table Casting ───────────────────────────────────────────
    async def _cast_table_in_place_clickhouse(self, final_table: str, columns: List[str], schema: Dict[str, Dict[str, Any]]) -> None:
        schema_name, raw_table = self._split_qualified_table_name(final_table)
        tmp_table = f"`{schema_name}`.`{raw_table}_tmp`"

        col_defs = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            ch_type = self._postgres_type_to_clickhouse(pg_type)
            col_defs.append(f"`{col}` Nullable({ch_type})")
        await self.execute(
            f"CREATE TABLE {tmp_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

        select_parts = []
        for col in columns:
            pg_type = schema.get(col, {}).get("postgres_type", "TEXT")
            select_parts.append(self._build_safe_cast_clickhouse(col, pg_type))
        await self.execute(f"INSERT INTO {tmp_table} SELECT {', '.join(select_parts)} FROM {final_table}")

        await self.drop_table(final_table)
        await self.execute(f"RENAME TABLE {tmp_table} TO {final_table}")
