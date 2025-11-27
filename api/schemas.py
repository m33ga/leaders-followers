from pydantic import BaseModel, Field
from typing import Optional


class KeyValue(BaseModel):
    """Model for key-value pair."""
    key: str
    value: str


class WriteRequest(BaseModel):
    """Request model for write operations."""
    key: str
    value: str


class WriteResponse(BaseModel):
    """Response model for write operations."""
    success: bool
    message: Optional[str] = None
    replicated_count: Optional[int] = None


class ReadResponse(BaseModel):
    """Response model for read operations."""
    success: bool
    key: str
    value: Optional[str] = None
    message: Optional[str] = None


class ReplicationRequest(BaseModel):
    """Request model for replication to followers."""
    key: str
    value: str
    timestamp: float


class ReplicationResponse(BaseModel):
    """Response model for replication acknowledgment."""
    success: bool
    follower_id: str


class AllDataResponse(BaseModel):
    """Response model for getting all key-value pairs."""
    success: bool
    data: dict[str, str]
    count: int
