# Distributed Cache System

This project implements a Redis-inspired distributed cache in Python.

## Design

The system is split into four layers:

- `distributed_cache/cache`: in-memory storage, TTL, and LRU eviction.
- `distributed_cache/cluster`: consistent hashing, replicas, membership, and heartbeats.
- `distributed_cache/api`: FastAPI request handlers for client and node-to-node traffic.
- `tests`: unit and integration tests for store, routing, failure handling, and API behavior.

### Folder Structure

```text
distributed_cache/
  cache/
  cluster/
  api/
tests/
scripts/
docker/
```

### Design Choices

- FastAPI is used for a lightweight HTTP API and easy local multi-node simulation.
- A shared hash-ring module handles sharding deterministically.
- Each node keeps an in-memory store and can replicate writes to a configured replica.
- A cluster manager tracks node health with heartbeat timestamps and routes around failed primaries.

## Run Locally

Install dependencies with `pip install -e .[dev]`, then start the demo gateway with `python -m distributed_cache.entrypoint`.

For a multi-container run, use `docker compose up --build`. The compose file starts the gateway and three node services.

## Distributed Mode

This system can run in two modes:

### Mode 1: In-Process (Local Development)

All nodes run inside a single Python process. This is the default and is useful for development, benchmarking, and testing. The gateway and all cache nodes are fully in-memory.

```bash
pip install -e .[dev]
python -m distributed_cache.entrypoint
```

This starts a gateway on `http://localhost:8000` with three in-memory nodes. You can then send requests to the gateway, and it will route them to the appropriate node using consistent hashing.

### Mode 2: Distributed (Docker)

Nodes run in separate containers and communicate over HTTP. This is closer to production and useful for testing failover, network behavior, and multi-machine deployments.

**Using Docker Compose (recommended):**

```bash
docker compose up --build
```

This starts:
- 1 gateway on port 8000
- 4 cache nodes on ports 8001–8004

All containers can reach each other by hostname. The gateway routes requests to nodes via HTTP.

**To scale the cluster:**

Edit `docker-compose.yml` and add more node services, or set `COMPOSE_SCALE` environment variables:

```bash
docker compose up --build --scale node-d=0
```

to remove node-d, or add new services manually:

```yaml
node-e:
  build: .
  command: ["python", "-m", "distributed_cache.node_app"]
  environment:
    NODE_ID: node-e
    NODE_HOST: 0.0.0.0
    NODE_PORT: 8005
  ports:
    - "8005:8005"
```

Then update the gateway environment to include the new node(s).

### Mode 3: Manual HTTP Mode (Multi-Machine)

You can also run nodes on different machines. Set environment variables on each process to point to the other nodes.

**Terminal 1: Start node-a**

```bash
NODE_ID=node-a \
NODE_HOST=0.0.0.0 \
NODE_PORT=8001 \
python -m distributed_cache.node_app
```

**Terminal 2: Start node-b**

```bash
NODE_ID=node-b \
NODE_HOST=0.0.0.0 \
NODE_PORT=8002 \
python -m distributed_cache.node_app
```

**Terminal 3: Start the gateway**

```bash
CLUSTER_TRANSPORT=http \
NODE_A_HOST=127.0.0.1 \
NODE_A_PORT=8001 \
NODE_B_HOST=127.0.0.1 \
NODE_B_PORT=8002 \
NODE_C_HOST=127.0.0.1 \
NODE_C_PORT=8003 \
python -m distributed_cache.entrypoint
```

The gateway will start on `http://localhost:8000`.

## Client Usage in Distributed Mode

Once the cluster is running (in any mode), you can send HTTP requests:

**Put a key (with TTL):**

```bash
curl -X PUT http://localhost:8000/cache/my-key \
  -H "Content-Type: application/json" \
  -d '{"value": {"user_id": 42, "name": "Alice"}, "ttl_seconds": 60}'
```

**Get a key:**

```bash
curl http://localhost:8000/cache/my-key
```

**Delete a key:**

```bash
curl -X DELETE http://localhost:8000/cache/my-key
```

**Check health:**

```bash
curl http://localhost:8000/health
```

### Programmatic Usage

In Python, you can also use the in-process cluster directly:

```python
import asyncio
from distributed_cache.entrypoint import build_local_cluster

async def main():
    cluster, _ = build_local_cluster()
    
    # Put a value
    result = await cluster.put("user:123", {"name": "Bob", "age": 30}, ttl_seconds=120)
    print(f"Put result: {result}")
    
    # Get a value
    result = await cluster.get("user:123")
    print(f"Get result: {result}")
    
    # Delete a value
    result = await cluster.delete("user:123")
    print(f"Delete result: {result}")

asyncio.run(main())
```

## Testing

Run `python -m pytest -q`.

The suite covers cache CRUD, TTL expiration, LRU eviction, consistent hashing, cluster routing, failover, and the HTTP API.

You can also test distributed mode manually by adding/removing nodes and observing key rebalancing:

```bash
python scripts/verify_rebalancing.py    # Add a node and see keys move
python scripts/verify_node_removal.py   # Remove a node and see failover
```

## Demo Flow

1. Start the service.
2. `PUT /cache/{key}` with a JSON body containing `value` and optional `ttl_seconds`.
3. `GET /cache/{key}` to confirm sharding and retrieval.
4. Stop or stale a node heartbeat to exercise replica failover.

## Performance Check

Run `python scripts/benchmark.py` to measure average PUT/GET latency and approximate throughput in the in-process cluster.

### Ring Visualization

Generate an animated GIF that shows how the virtual-node ring changes as servers are added one by one:

```bash
python scripts/benchmark.py --ring-gif artifacts/ring-growth.gif --max-nodes 6 --replicas 32
```

The animation includes the ring layout and a bar chart of sampled key ownership at each step, which makes it easier to judge whether distribution stays balanced as the cluster grows.

### Key Rebalancing GIF

Generate an animated GIF that compares key movement before and after each topology change for both adding and removing servers:

```bash
python scripts/benchmark.py --rebalance-gif artifacts/key-rebalancing.gif --max-nodes 6 --replicas 32
```

Each frame shows the sampled key ownership before and after the change, plus a prominent count of how many keys moved. The animation runs through server additions first and then server removals.

