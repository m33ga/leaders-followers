from fastapi import APIRouter
from api.schemas import ReplicationRequest, ReplicationResponse, ReadResponse
from api.store import KeyValueStore
from api.config import config

router = APIRouter()
store = KeyValueStore()


@router.post("/replicate", response_model=ReplicationResponse)
async def replicate(request: ReplicationRequest):
    """
    Replication endpoint - receives write operations from leader.
    Only available on follower nodes.
    """
    await store.set(request.key, request.value)
    
    return ReplicationResponse(
        success=True,
        follower_id=config.NODE_ID
    )


@router.get("/read/{key}", response_model=ReadResponse)
async def read(key: str):
    """
    Read endpoint - available on follower.
    Reads from local store.
    """
    value = await store.get(key)
    
    if value is not None:
        return ReadResponse(
            success=True,
            key=key,
            value=value
        )
    else:
        return ReadResponse(
            success=False,
            key=key,
            message="Key not found"
        )


@router.get("/all")
async def get_all():
    """Get all key-value pairs from the store."""
    data = await store.all()
    return {
        "success": True,
        "data": data,
        "count": len(data)
    }


@router.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "role": "follower",
        "node_id": config.NODE_ID
    }
