from fastapi import APIRouter, HTTPException, status
from api.schemas import WriteRequest, WriteResponse, ReadResponse
from api.store import KeyValueStore
from api.replication import replication_service
from api.config import config

router = APIRouter()
store = KeyValueStore()


@router.post("/write", response_model=WriteResponse)
async def write(request: WriteRequest):
    """
    Write endpoint - only available on leader.
    Writes to local store and replicates to followers with quorum.
    """
    if not config.is_leader():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Write operations are only allowed on the leader"
        )
    
    await store.set(request.key, request.value)

    success, replicated_count = await replication_service.replicate(
        key=request.key,
        value=request.value,
        quorum=config.WRITE_QUORUM
    )
    
    if success:
        return WriteResponse(
            success=True,
            message=f"Write successful, replicated to {replicated_count} followers",
            replicated_count=replicated_count
        )
    else:
        return WriteResponse(
            success=False,
            message=f"Write failed to meet quorum. Required: {config.WRITE_QUORUM}, Achieved: {replicated_count}",
            replicated_count=replicated_count
        )


@router.get("/read/{key}", response_model=ReadResponse)
async def read(key: str):
    """
    Read endpoint - available on leader.
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
        "role": "leader",
        "node_id": config.NODE_ID,
        "write_quorum": config.WRITE_QUORUM,
        "followers": len(config.FOLLOWER_URLS)
    }
