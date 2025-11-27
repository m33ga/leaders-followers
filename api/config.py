import os
from typing import List


class Config:
    ROLE: str = os.getenv("ROLE", "follower")
    NODE_ID: str = os.getenv("NODE_ID", "unknown")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    FOLLOWER_URLS: List[str] = os.getenv("FOLLOWER_URLS", "").split(",") if os.getenv("FOLLOWER_URLS") else []
    WRITE_QUORUM: int = int(os.getenv("WRITE_QUORUM", "1"))
    MIN_DELAY_MS: int = int(os.getenv("MIN_DELAY_MS", "0"))
    MAX_DELAY_MS: int = int(os.getenv("MAX_DELAY_MS", "1000"))
    
    @classmethod
    def is_leader(cls) -> bool:
        return cls.ROLE.lower() == "leader"
    
    @classmethod
    def is_follower(cls) -> bool:
        return cls.ROLE.lower() == "follower"


config = Config()
