import json
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

import httpx

from .base import DatabaseAdapter

logger = logging.getLogger("memFrame")


@dataclass
class ClickHouseQueryResult:
    result_rows: List[tuple]
    column_names: List[str]

    @property
    def first_row(self) -> Optional[tuple]:
        return self.result_rows[0] if self.result_rows else None

    @property
    def first_item(self) -> Any:
        row = self.first_row
        return row[0] if row else None


class HttpxClickHouseClient:
    """Minimal async ClickHouse HTTP client for this project's backend API."""

    def __init__(
        self,
        host: str,
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: Optional[str] = None,
        secure: bool = False,
        timeout: float = 300.0,
    ) -> None:
        scheme = "https" if secure else "http"
        self.database = database
        self._base_url = f"{scheme}://{host}:{port}"
        self._auth = (username, password)
        self._timeout = timeout

    async def close(self) -> None:
        return None

    async def command(
        self, query: str, parameters: Optional[Sequence[Any]] = None
    ) -> None:
        await self._post(self._render_query(query, parameters))

    async def query(
        self, query: str, parameters: Optional[Sequence[Any]] = None
    ) -> ClickHouseQueryResult:
        rendered = self._render_query(query, parameters)
        response = await self._post(self._with_json_format(rendered))
        payload = response.json()
        meta = payload.get("meta", [])
        column_names = [m["name"] for m in meta]
        rows = [
            self._normalize_result_row(row, column_names)
            for row in payload.get("data", [])
        ]
        return ClickHouseQueryResult(rows, column_names)

    async def insert(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        database: Optional[str] = None,
        column_names: Optional[Sequence[str]] = None,
    ) -> None:
        if not rows:
            return
        if not column_names:
            raise ValueError("column_names are required for ClickHouse inserts")

        target_database = database or self.database
        if not target_database:
            raise ValueError("ClickHouse inserts require a database-qualified table")

        qualified_table = self._quote_qualified_table(target_database, table)
        columns = ", ".join(self._quote_identifier(column) for column in column_names)
        query = f"INSERT INTO {qualified_table} ({columns}) FORMAT JSONEachRow"
        lines = (
            json.dumps(
                dict(zip(column_names, row)),
                default=self._json_default,
                ensure_ascii=False,
            )
            for row in rows
        )
        await self._post(query, data="\n".join(lines))

    async def insert_arrow(
        self,
        table: str,
        arrow_table: Any,
        database: Optional[str] = None,
    ) -> None:
        """Insert a PyArrow table using ClickHouse's columnar ArrowStream format."""
        if arrow_table.num_rows == 0:
            return

        target_database = database or self.database
        if not target_database:
            raise ValueError("ClickHouse inserts require a database-qualified table")

        import pyarrow.ipc as ipc

        buffer = io.BytesIO()
        with ipc.new_stream(buffer, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        qualified_table = self._quote_qualified_table(target_database, table)
        columns = ", ".join(
            self._quote_identifier(name) for name in arrow_table.column_names
        )
        await self._post(
            f"INSERT INTO {qualified_table} ({columns}) FORMAT ArrowStream",
            data=buffer.getvalue(),
        )

    async def _post(
        self, query: str, data: Optional[Any] = None
    ) -> httpx.Response:
        params: Dict[str, str] = {}
        if self.database:
            params["database"] = self.database

        if data is None:
            content = query.encode("utf-8")
        else:
            params["query"] = query
            content = data if isinstance(data, bytes) else data.encode("utf-8")

        try:
            async with httpx.AsyncClient(
                base_url=self._base_url,
                auth=self._auth,
                timeout=self._timeout,
            ) as client:
                response = await client.post(
                    "/",
                    params=params,
                    content=content,
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text.strip()
            message = f"ClickHouse HTTP {exc.response.status_code}"
            if detail:
                message = f"{message}: {detail}"
            raise RuntimeError(message) from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"ClickHouse HTTP request failed: {exc!r}") from exc
        return response

    def _with_json_format(self, query: str) -> str:
        if " format " in f" {query.lower()} ":
            return query
        return f"{query.rstrip().rstrip(';')} FORMAT JSONCompact"

    def _normalize_result_row(self, row: Any, column_names: Sequence[str]) -> tuple:
        if isinstance(row, dict):
            if column_names:
                return tuple(row.get(column) for column in column_names)
            return tuple(row.values())
        return tuple(row)

    def _render_query(
        self, query: str, parameters: Optional[Sequence[Any]] = None
    ) -> str:
        if not parameters:
            return query

        rendered = query
        for value in parameters:
            placeholder_index = rendered.find("?")
            if placeholder_index == -1:
                raise ValueError("Too many parameters for ClickHouse query")
            rendered = (
                rendered[:placeholder_index]
                + self._to_clickhouse_literal(value)
                + rendered[placeholder_index + 1 :]
            )
        if "?" in rendered:
            raise ValueError("Not enough parameters for ClickHouse query")
        return rendered

    def _to_clickhouse_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float, Decimal)):
            return str(value)
        if isinstance(value, (datetime, date)):
            encoded = (
                value.isoformat(sep=" ")
                if isinstance(value, datetime)
                else value.isoformat()
            )
            return self._quote_string(encoded)
        return self._quote_string(str(value))

    def _quote_string(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _quote_identifier(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    def _quote_qualified_table(self, database: str, table: str) -> str:
        clean_database = database.strip("`\"")
        clean_table = table.strip("`\"")
        return (
            f"{self._quote_identifier(clean_database)}."
            f"{self._quote_identifier(clean_table)}"
        )

    def _json_default(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return (
                value.isoformat(sep=" ")
                if isinstance(value, datetime)
                else value.isoformat()
            )
        if isinstance(value, Decimal):
            return str(value)
        type_name = type(value).__name__
        raise TypeError(f"Object of type {type_name} is not JSON serializable")


class ClickHouseAdapter(DatabaseAdapter):
    """
    Async ClickHouse adapter using ClickHouse's HTTP API via httpx.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.host = host
        self.port = port or 8123
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.client: Optional[Any] = None

    async def connect(self) -> None:
        self.client = HttpxClickHouseClient(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
            timeout=self.timeout,
        )
        logger.info(
            f"ClickHouse adapter connected to {self.host}:{self.port}"
        )

    async def close(self) -> None:
        if self.client:
            await self.client.close()
            self.client = None

    async def _ensure_client(self) -> None:
        if self.client is None:
            await self.connect()

    async def execute(self, sql: str, *args) -> Any:
        await self._ensure_client()
        await self.client.command(sql, parameters=args if args else None)

    async def fetch(self, sql: str, *args) -> List[Any]:
        await self._ensure_client()
        result = await self.client.query(sql, parameters=args if args else None)
        return self._rows_to_dicts(result)

    async def fetchval(self, sql: str, *args) -> Any:
        await self._ensure_client()
        result = await self.client.query(sql, parameters=args if args else None)
        return result.first_item

    async def fetchrow(self, sql: str, *args) -> Any:
        await self._ensure_client()
        result = await self.client.query(sql, parameters=args if args else None)
        rows = self._rows_to_dicts(result)
        return rows[0] if rows else None

    def _rows_to_dicts(self, result: ClickHouseQueryResult) -> List[Dict[str, Any]]:
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    async def insert_rows(
        self,
        table_name: str,
        rows: List[List[Any]],
        columns: List[str],
    ) -> None:
        await self._ensure_client()
        clean = table_name.replace("`", "").replace('"', "")
        if "." in clean:
            database, table = clean.split(".", 1)
        else:
            database = self.database
            table = clean
        await self.client.insert(table, rows, database=database, column_names=columns)

    async def get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        rows = await self.fetch(
            """
            SELECT name, type
            FROM system.columns
            WHERE database = ? AND table = ?
            ORDER BY position
            """,
            schema,
            table,
        )
        return {row["name"]: row["type"] for row in rows}

    async def get_table_info(self, table: str, schema: str) -> Dict[str, Any]:
        count_sql = f"SELECT count() FROM `{schema}`.`{table}`"
        row_count = await self.fetchval(count_sql)

        columns = await self.get_column_types(table, schema)
        return {
            "table_name": table,
            "row_count": row_count or 0,
            "column_count": len(columns),
            "total_size": "N/A (ClickHouse)",
            "table_size": "N/A (ClickHouse)",
            "columns": columns,
        }

    async def table_exists(self, table: str, schema: str) -> bool:
        result = await self.fetchval(
            "SELECT count() FROM system.tables WHERE database = ? AND name = ?",
            schema,
            table,
        )
        return bool(result and result > 0)

    def placeholder(self, index: int = 1) -> str:
        return "?"

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    async def fetch_iter(self, sql: str, *args, chunk_size: int = 1000):
        """
        Async streaming iterator over query results using LIMIT/OFFSET pagination.
        """
        offset = 0
        while True:
            paginated_sql = f"{sql.rstrip().rstrip(';')} LIMIT {chunk_size} OFFSET {offset}"
            rows = await self.fetch(paginated_sql, *args)
            if not rows:
                break
            for row in rows:
                yield row
            offset += chunk_size
