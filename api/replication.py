import asyncio
import random
import httpx
from typing import List, Tuple
from api.schemas import ReplicationRequest
from api.config import config


class ReplicationService:
    def __init__(self, follower_urls: List[str], min_delay_ms: int, max_delay_ms: int):
        self.follower_urls = follower_urls
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        self._client = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create a shared HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client
    
    async def _replicate_to_follower(
        self, 
        follower_url: str, 
        key: str, 
        value: str,
        timestamp: float
    ) -> Tuple[bool, str]:
        """
        Replicate a key-value pair to a single follower with random delay.
        
        Returns:
            Tuple of (success, follower_id)
        """
        delay_ms = random.uniform(self.min_delay_ms, self.max_delay_ms)
        await asyncio.sleep(delay_ms / 1000.0)
        
        try:
            client = await self._get_client()
            request = ReplicationRequest(key=key, value=value, timestamp=timestamp)
            response = await client.post(
                f"{follower_url}/replicate",
                json=request.model_dump(),
            )
            
            if response.status_code == 200:
                data = response.json()
                return True, data.get("follower_id", follower_url)
            else:
                return False, follower_url
        except Exception as e:
            print(f"Replication to {follower_url} failed: {e}")
            return False, follower_url
    
    async def replicate(self, key: str, value: str, timestamp: float, quorum: int) -> Tuple[bool, int]:
        """
        Replicate to followers concurrently with semi-synchronous replication.
        Uses asyncio.wait to return as soon as quorum is met (N out of M tasks).
        
        Args:
            key: The key to replicate
            value: The value to replicate
            timestamp: UTC timestamp for this write
            quorum: Number of successful replications needed
        
        Returns:
            Tuple of (success, count of successful replications)
        """
        if not self.follower_urls:
            return True, 0
        
        tasks = [
            asyncio.create_task(self._replicate_to_follower(url, key, value, timestamp))
            for url in self.follower_urls
        ]
        
        successful_count = 0
        failed_count = 0
        pending = set(tasks)
        
        while pending and successful_count < quorum:
            # Wait for at least one task to complete
            done, pending = await asyncio.wait(
                pending, 
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in done:
                try:
                    success, follower_id = task.result()
                    if success:
                        successful_count += 1
                        if successful_count >= quorum:
                            return True, successful_count
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    print(f"Task failed with exception: {e}")

        return successful_count >= quorum, successful_count


replication_service = ReplicationService(
    follower_urls=config.FOLLOWER_URLS,
    min_delay_ms=config.MIN_DELAY_MS,
    max_delay_ms=config.MAX_DELAY_MS,
) if config.is_leader() else None
