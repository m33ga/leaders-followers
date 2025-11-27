import asyncio
from typing import Dict, Optional


class KeyValueStore:
    def __init__(self):
        self._store: Dict[str, str] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[str]:
        return self._store.get(key)
    
    async def set(self, key: str, value: str) -> None:
        self._store[key] = value
    
    async def exists(self, key: str) -> bool:
        return key in self._store
    
    async def all(self) -> Dict[str, str]:
        return self._store.copy()
