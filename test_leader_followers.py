import pytest
import pytest_asyncio
import httpx
import asyncio
import time


LEADER_URL = "http://localhost:8000"
FOLLOWER_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]
REQUEST_TIMEOUT = 30.0
REPLICATION_WAIT = 1.0


@pytest_asyncio.fixture
async def http_client():
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        yield client


@pytest.mark.asyncio
async def test_all_nodes_are_healthy(http_client):
    response = await http_client.get(f"{LEADER_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["role"] == "leader"
    
    for i, follower_url in enumerate(FOLLOWER_URLS, 1):
        response = await http_client.get(f"{follower_url}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["role"] == "follower"


@pytest.mark.asyncio
async def test_replication_to_all_followers(http_client):
    """writes are replicated to all followers."""
    test_data = {
        "repl_key_1": "repl_value_1",
        "repl_key_2": "repl_value_2",
        "repl_key_3": "repl_value_3",
    }
    
    for key, value in test_data.items():
        response = await http_client.post(
            f"{LEADER_URL}/write",
            json={"key": key, "value": value}
        )
        assert response.status_code == 200
        result = response.json()
        assert result["success"] is True

    await asyncio.sleep(REPLICATION_WAIT)
    
    for i, follower_url in enumerate(FOLLOWER_URLS, 1):
        for key, expected_value in test_data.items():
            response = await http_client.get(f"{follower_url}/read/{key}")
            assert response.status_code == 200
            result = response.json()
            assert result["success"] is True, f"Follower {i} missing key {key}"
            assert result["value"] == expected_value, f"Follower {i} has wrong value for {key}"


@pytest.mark.asyncio
async def test_all_endpoint_consistency(http_client):
    """Test that /all endpoint shows the same data on leader and all followers."""
    test_data = {
        "k1": "v1",
        "k2": "v2",
        "k3": "v3",
        "k4": "v4",
        "k5": "v5",
    }

    for key, value in test_data.items():
        response = await http_client.post(
            f"{LEADER_URL}/write",
            json={"key": key, "value": value}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
    
    await asyncio.sleep(REPLICATION_WAIT)
    
    response = await http_client.get(f"{LEADER_URL}/all")
    assert response.status_code == 200
    leader_data = response.json()
    assert leader_data["success"] is True
    leader_kvs = leader_data["data"]

    for key, value in test_data.items():
        assert key in leader_kvs, f"Key {key} missing from leader"
        assert leader_kvs[key] == value, f"Value mismatch for {key} on leader"
    
    for i, follower_url in enumerate(FOLLOWER_URLS, 1):
        response = await http_client.get(f"{follower_url}/all")
        assert response.status_code == 200
        follower_data = response.json()
        assert follower_data["success"] is True
        follower_kvs = follower_data["data"]
        
        for key, value in test_data.items():
            assert key in follower_kvs, f"Key {key} missing from follower {i}"
            assert follower_kvs[key] == value, f"Value mismatch for {key} on follower {i}"
        
        for key in test_data.keys():
            assert follower_kvs[key] == leader_kvs[key]


@pytest.mark.asyncio
async def test_multiple_keys_overwrite(http_client):
    """Test overwriting multiple keys with different values."""
    keys_to_test = ["key_a", "key_b", "key_c"]

    for iteration in range(1, 5):
        for key in keys_to_test:
            response = await http_client.post(
                f"{LEADER_URL}/write",
                json={"key": key, "value": f"{key}_v{iteration}"}
            )
            assert response.status_code == 200
            assert response.json()["success"] is True


    response = await http_client.get(f"{LEADER_URL}/all")
    leader_data = response.json()["data"]
    
    for key in keys_to_test:
        assert leader_data[key] == f"{key}_v4", f"Leader has wrong value for {key}"
    
    for i, follower_url in enumerate(FOLLOWER_URLS, 1):
        response = await http_client.get(f"{follower_url}/all")
        follower_data = response.json()["data"]
        
        for key in keys_to_test:
            assert follower_data[key] == f"{key}_v4", \
                f"Follower {i} has wrong value for {key}: expected {key}_v4, got {follower_data[key]}"
            assert follower_data[key] == leader_data[key], \
                f"Follower {i} inconsistent with leader for {key}"
