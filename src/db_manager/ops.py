import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    pass


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class OpsManager:
    
    async def _alist_tables(self) -> List[Dict[str, str]]:
        """
        Returns all uploaded tables from registry.csv_registry
        Output: [{data_id, filename}]
        """
        if not self._backend:
            raise RuntimeError("Not connected.")

        rows = await self._backend.fetch(
            f"""
            SELECT data_id, filename
            FROM {self._backend.csv_registry_table}
            WHERE is_upload_success = TRUE
            ORDER BY uploaded_at DESC
            """
        )

        return [{"data_id": r[0], "filename": r[1]} for r in rows]

    async def _aset_active(self, data_id: str) -> str:  
        table_name = self._backend.get_upload_table_name(data_id)
        if not await self._backend.table_exists(table_name):
            raise ValueError(f"Table for data_id '{data_id}' does not exist")
        self._active_id = data_id
        logger.info(f"Active CSV set to {data_id}")
        return data_id

    async def _aget_active_table(self) -> Optional[str]:
        return self._active_id

    async def _adelete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:
        """
        Delete a dataset by data_id OR filename.

        This will:
        - Drop upload table
        - Drop all transient tables
        - Remove entries from registry tables
        """
        if not self._backend:
            raise RuntimeError("Not connected.")

        if not data_id and not filename:
            raise ValueError("Provide either data_id or filename")

        # -------------------------------
        # Step 1: Resolve data_id
        # -------------------------------
        if not data_id:
            row = await self._backend.fetch_one(
                f"""
                SELECT data_id
                FROM {self._backend.csv_registry_table}
                WHERE filename = {self._placeholder(1)}
                """,
                filename,
            )
            if not row:
                raise ValueError(f"No table found for filename: {filename}")
            data_id = row[0]

        # -------------------------------
        # Step 2: Get upload table name
        # -------------------------------
        row = await self._backend.fetch_one(
            f"""
            SELECT table_name
            FROM {self._backend.csv_registry_table}
            WHERE data_id = {self._placeholder(1)}
            """,
            data_id,
        )
        if not row:
            raise ValueError(f"No table found for data_id: {data_id}")

        upload_table = row[0]

        # -------------------------------
        # Step 3: Drop transient tables
        # -------------------------------
        transient_rows = await self._backend.fetch(
            f"""
            SELECT generated_table_name
            FROM {self._backend.transient_registry_table}
            WHERE data_id = {self._placeholder(1)}
            """,
            data_id,
        )

        for t in transient_rows:
            await self._backend.drop_table(t[0])

        # -------------------------------
        # Step 4: Drop upload table
        # -------------------------------
        await self._backend.drop_table(upload_table)

        # -------------------------------
        # Step 5: Clean registries
        # -------------------------------
        await self._backend.execute(
            f"""
            DELETE FROM {self._backend.csv_registry_table}
            WHERE data_id = {self._placeholder(1)}
            """,
            data_id,
        )

        await self._backend.execute(
            f"""
            DELETE FROM {self._backend.transient_registry_table}
            WHERE data_id = {self._placeholder(1)}
            """,
            data_id,
        )

        # -------------------------------
        # Step 6: Reset active
        # -------------------------------
        if self._active_id == data_id:
            self._active_id = None

        logger.info(f"Deleted dataset {data_id}")

    async def _arecord_operation(self, data_id: str, operation_type: str, generated_table_name: str) -> int:
        max_op = await self._backend.fetch_val(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.transient_registry_table} WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        opidx = max_op + 1
        await self._backend.execute(
            f"""
            INSERT INTO {self._backend.transient_registry_table} (data_id, opidx, generated_table_name, operation_type)
            VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, {self._placeholder(4)})
            """,
            data_id,
            opidx,
            generated_table_name,
            operation_type,
        )
        logger.info(f"Recorded operation {opidx} ({operation_type}) for {data_id} -> {generated_table_name}")
        return opidx

    async def _arecord_method_call( self, data_id: str, class_name: str, method_name: str, args: tuple, kwargs: dict, generated_table_name: Optional[str] = None) -> int:
        """
        Log a method call into transient_registry. If the call generates a table,
        pass its name as `generated_table_name`.
        """
        if not self._backend:
            raise RuntimeError("Not connected.")

        max_op = await self._backend.fetch_val(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        opidx = max_op + 1

        await self._backend.execute(
            f"""
            INSERT INTO {self._backend.transient_registry_table}
                (data_id, opidx, operation_type,
                class_name, method_name, args, kwargs,
                generated_table_name)               -- included
            VALUES
                ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)},
                {self._placeholder(4)}, {self._placeholder(5)},
                {self._placeholder(6)}, {self._placeholder(7)},
                {self._placeholder(8)})
            """,
            data_id,
            opidx,
            "method_call",
            class_name,
            method_name,
            json.dumps(args),
            json.dumps(kwargs),
            generated_table_name,                     # NULL or actual name
        )
        logger.debug(
            f"Recorded method call {class_name}.{method_name} "
            f"→ data_id={data_id}, opidx={opidx}"
            + (f", table={generated_table_name}" if generated_table_name else "")
        )
        return opidx
    
    async def _alist_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if data_id is None:
            data_id = self._active_id
            if data_id is None:
                raise ValueError("No data_id provided and no active CSV set.")

        rows = await self._backend.fetch(
            f"""
            SELECT opidx, operation_type, generated_table_name, created_at
            FROM {self._backend.transient_registry_table}
            WHERE data_id = {self._placeholder(1)}
            ORDER BY opidx
            """,
            data_id,
        )
        return [
            {"opidx": r[0], "operation_type": r[1], "table_name": r[2], "created_at": r[3]}
            for r in rows
        ]

    async def _aretrieve_operation(self, data_id: str, opidx: int) -> str:
        row = await self._backend.fetch_one(
            f"SELECT generated_table_name FROM {self._backend.transient_registry_table} WHERE data_id = {self._placeholder(1)} AND opidx = {self._placeholder(2)}",
            data_id,
            opidx,
        )
        if not row:
            raise ValueError(f"Operation {opidx} not found for data_id {data_id}")
        return row[0]
