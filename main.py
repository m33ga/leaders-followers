from fastapi import FastAPI
from api.config import config

# Create FastAPI app
app = FastAPI(
    title="Key-Value Store with Leader-Follower Replication",
    description=f"Node: {config.NODE_ID} | Role: {config.ROLE}",
    version="1.0.0"
)


if config.is_leader():
    from api.endpoints.leader import router as leader_router
    app.include_router(leader_router, tags=["leader"])
    print(f"Starting LEADER node: {config.NODE_ID}")
    print(f"Write quorum: {config.WRITE_QUORUM}")
    print(f"Followers: {config.FOLLOWER_URLS}")
    print(f"Replication delay: [{config.MIN_DELAY_MS}ms, {config.MAX_DELAY_MS}ms]")
else:
    from api.endpoints.follower import router as follower_router
    app.include_router(follower_router, tags=["follower"])
    print(f"Starting FOLLOWER node: {config.NODE_ID}")


@app.get("/")
async def root():
    return {
        "service": "Key-Value Store",
        "node_id": config.NODE_ID,
        "role": config.ROLE,
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORT)
