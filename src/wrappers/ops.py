from src.db_manager.ops import OpsManager
from src.utils.async_sync import async_to_sync


import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional



if TYPE_CHECKING:
    pass


logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    
    
    
class OpsWrapper(OpsManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    
    # Wrappers
    async def alist_tables(self)-> List[Dict[str, str]]:
        return await super()._alist_tables()

    @async_to_sync
    async def list_tables(self)-> List[Dict[str, str]]:
        return await self.alist_tables()

    async def aset_active(self, data_id: str)-> str:
        return await super()._aset_active(data_id)
    
    @async_to_sync
    async def set_active(self, data_id: str)-> str:
        return await self.aset_active(data_id)

    async def aget_active_table(self) -> Optional[str]:
        return await super()._aget_active_table()

    @async_to_sync
    async def get_active_table(self) -> Optional[str]:
        return await self.aget_active_table()
    
    
    async def adelete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:      
        return await super()._adelete_table(data_id, filename) 
    
    @async_to_sync
    async def delete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:      
        return await self.adelete_table(data_id, filename) 
    
    
    async def alist_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return await super()._alist_operations(data_id)
    
    @async_to_sync
    async def list_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.alist_operations(data_id)

    
    async def aretrieve_operation(self, data_id: str, opidx: int) -> str:
        return await super()._aretrieve_operation(data_id,opidx)
    
    @async_to_sync
    async def retrieve_operation(self, data_id: str, opidx: int) -> str:
        return await self.aretrieve_operation(data_id,opidx)
