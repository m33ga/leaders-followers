import asyncio
from typing import Dict, Optional


class KeyValueStore:
    def __init__(self):
        self._store: Dict[str, str] = {}
        self._timestamps: Dict[str, float] = {}

    async def get(self, key: str) -> Optional[str]:
        return self._store.get(key)
    
    async def set(self, key: str, value: str, timestamp: float = None) -> bool:
        """Set key-value pair with timestamp. Returns True if updated, False if ignored."""
        current_ts = self._timestamps.get(key, 0)
        if timestamp is None or timestamp > current_ts:
            self._store[key] = value
            if timestamp is not None:
                self._timestamps[key] = timestamp
            return True
        return False
    
    async def exists(self, key: str) -> bool:
        return key in self._store
    
    async def all(self) -> Dict[str, str]:
        return self._store.copy()
