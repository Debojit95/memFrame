from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import pandas as pd
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok

class DataStatsOps:

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    async def _fetch_data(self, table: str, schema: str, columns: Any='*', limit: Optional[int]=None) -> pd.DataFrame:
        """Return a DataFrame sample of the table for the response."""
        qualified = self._qualified_table(table, schema)
        if columns is None or (isinstance(columns, str) and columns.strip() == '*'):
            column_clause = '*'
        elif isinstance(columns, (list, tuple)):
            if not columns or (len(columns) == 1 and str(columns[0]).strip() == '*'):
                column_clause = '*'
            else:
                sanitized_cols = [SQLIdentifierSanitizer.sanitize(str(col), allow_qualified=False) for col in columns]
                column_clause = ', '.join((self.db.quote_identifier(col) for col in sanitized_cols))
        else:
            safe_col = SQLIdentifierSanitizer.sanitize(str(columns), allow_qualified=False)
            column_clause = self.db.quote_identifier(safe_col)
        limit_clause = f' LIMIT {int(limit)}' if limit is not None else ''
        rows = await self._fetch(f'SELECT {column_clause} FROM {qualified}{limit_clause}')
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    def _success_response(self, message: str, involved_cols: Optional[List[str]]=None, generated_cols: Optional[List[str]]=None, result: Any=None, **extra) -> Dict[str, Any]:
        return ok(message, involved_cols, generated_cols, result, **extra)

    def _error_response(self, error_message: str, involved_cols: List[str]=None, generated_cols: List[str]=None) -> Dict[str, Any]:
        return fail(error_message, involved_cols, generated_cols)

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(f'Unsupported database backend for stats operation: {self.db.__class__.__name__}')

    async def _generate_result_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self.db.fetchval(f'SELECT COALESCE(MAX(opidx), 0) FROM {backend.transient_registry_table} WHERE data_id = {backend.placeholder(1)}', data_id)
        next_op = (max_op or 0) + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f'{safe_base}__op_{next_op}'

    async def _resolve_output_table_name(self, table: str, schema: str, backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        if new_table:
            candidate = SQLIdentifierSanitizer.sanitize(new_table)
        elif backend is not None and data_id:
            candidate = await self._generate_result_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f'{candidate}_{dedupe_idx}')
            dedupe_idx += 1
        return output_table

    async def _materialize_query_as_table(self, query: str, table: str, schema: str, backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> str:
        """Create a transient result table from a SELECT query and return its name."""
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        output_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f'CREATE TABLE {qualified_target} AS {query}')
        return output_table
