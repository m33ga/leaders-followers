import asyncio
import httpx
import time
from typing import List, Dict, Tuple
import statistics
import matplotlib.pyplot as plt


LEADER_URL = "http://localhost:8000"
FOLLOWER_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]
REQUEST_TIMEOUT = 30.0
NUM_KEYS = 10
NUM_WRITES_PER_KEY = 10
BATCH_SIZE = 10


async def check_health(client: httpx.AsyncClient) -> bool:
    """Check if leader is healthy."""
    try:
        response = await client.get(f"{LEADER_URL}/health")
        return response.status_code == 200
    except:
        return False


async def perform_write(client: httpx.AsyncClient, key: str, value: str) -> Tuple[bool, float, int]:
    """
    Perform a single write operation.
    Returns: (success, latency_ms, replicated_count)
    """
    start = time.perf_counter()
    try:
        response = await client.post(
            f"{LEADER_URL}/write",
            json={"key": key, "value": value}
        )
        latency = (time.perf_counter() - start) * 1000
        
        if response.status_code == 200:
            data = response.json()
            return data["success"], latency, data.get("replicated_count", 0)
        return False, latency, 0
    except Exception as e:
        latency = (time.time() - start) * 1000
        return False, latency, 0


async def run_concurrent_writes(client: httpx.AsyncClient, writes: List[Tuple[str, str]]) -> List[Tuple[bool, float, int]]:
    """Run multiple writes concurrently."""
    tasks = [perform_write(client, key, value) for key, value in writes]
    return await asyncio.gather(*tasks)


async def test_quorum(client: httpx.AsyncClient, quorum: int) -> Dict:
    """Test performance for a specific quorum value."""
    results = {
        "quorum": quorum,
        "latencies": [],
        "successes": 0,
        "failures": 0,
        "replicated_counts": []
    }
    
    for batch in range(NUM_WRITES_PER_KEY):
        writes = [(f"key_{i}", f"value_{batch}") for i in range(NUM_KEYS)]
        batch_results = await run_concurrent_writes(client, writes)
        
        for success, latency, rep_count in batch_results:
            results["latencies"].append(latency)
            results["replicated_counts"].append(rep_count)
            if success:
                results["successes"] += 1
            else:
                results["failures"] += 1
        
        print(f"  Batch {batch + 1}/{NUM_WRITES_PER_KEY}: {len([r for r in batch_results if r[0]])} successful")
    
    return results


async def verify_consistency(client: httpx.AsyncClient) -> Dict:
    """Verify data consistency across all nodes."""
    response = await client.get(f"{LEADER_URL}/all")
    leader_data = response.json()["data"]
    
    consistency = {
        "leader_keys": len(leader_data),
        "followers_match": 0,
        "followers_mismatch": 0,
        "mismatches": []
    }
    
    for i, follower_url in enumerate(FOLLOWER_URLS, 1):
        try:
            response = await client.get(f"{follower_url}/all")
            follower_data = response.json()["data"]

            matches = all(
                key in follower_data and follower_data[key] == value
                for key, value in leader_data.items()
            )
            
            if matches and len(follower_data) == len(leader_data):
                consistency["followers_match"] += 1
            else:
                consistency["followers_mismatch"] += 1
                consistency["mismatches"].append(f"Follower {i}")
        except Exception as e:
            consistency["followers_mismatch"] += 1
            consistency["mismatches"].append(f"Follower {i} (error)")
    
    return consistency


def plot_results(all_results: List[Dict]):
    """Generate performance plots."""
    quorums = [r["quorum"] for r in all_results]
    avg_latencies = [statistics.mean(r["latencies"]) for r in all_results]
    median_latencies = [statistics.median(r["latencies"]) for r in all_results]
    plt.figure(figsize=(12, 7))
    plt.plot(quorums, avg_latencies, marker='o', linewidth=2, markersize=8, label='Average')
    plt.plot(quorums, median_latencies, marker='s', linewidth=2, markersize=8, label='Median')
    plt.xlabel("Write Quorum", fontsize=12)
    plt.ylabel("Latency (ms)", fontsize=12)
    plt.title("Write Quorum and Latency (Delay Range: 0-1000ms)", fontsize=14)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.xticks(quorums)
    plt.tight_layout()
    plt.show()


def print_statistics(results: Dict):
    latencies = results["latencies"]
    total = results["successes"] + results["failures"]
    
    print(f"\n  Results:")
    print(f"Writes:       {results['successes']}/{total} successful")
    print(f"Avg latency:  {statistics.mean(latencies):.1f}ms")
    print(f"Min/Max:      {min(latencies):.1f}ms / {max(latencies):.1f}ms")
    print(f"Median:       {statistics.median(latencies):.1f}ms")


def print_analysis(all_results: List[Dict], consistency: Dict):
    print("ANALYSIS")
    
    print("\nLatency by Quorum:")
    for r in all_results:
        avg = statistics.mean(r["latencies"])
        print(f"  Quorum {r['quorum']}: {avg:.1f}ms average")

    print("\nConsistency Check:")
    print(f"  {consistency['followers_match']}/5 followers match leader")


async def main():
    print("Performance Analysis: Leader-Follower Replication\n")
    
    all_results = []
    
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        for quorum in range(1, 6):
            print(f"\n\nQ = {quorum}")
            print(f"\n1. Set WRITE_QUORUM={quorum} in compose.yaml")
            input("2. Press Enter to continue...")
            
            if not await check_health(client):
                print("ERROR: Leader not responding")
                return
            
            print("\nRunning 100 writes...")
            results = await test_quorum(client, quorum)
            all_results.append(results)
            
            print_statistics(results)

            print("CONSISTENCY CHECK")
            print("\nVerifying data across all nodes...")
            consistency = await verify_consistency(client)
            print(f"  Leader: {consistency['leader_keys']} keys")
            print(f"  Followers: {consistency['followers_match']}/5 consistent")
    

    plot_results(all_results)
    print_analysis(all_results, consistency)


if __name__ == "__main__":
    asyncio.run(main())
