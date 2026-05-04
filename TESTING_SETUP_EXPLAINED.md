# Testing Setup: Simulated vs. True Distributed System

## 🔍 Your Current Architecture (in comprehensive_test.py)

### Layer 1: HTTP Gateway (Realistic Client Interaction)
```python
# Line 14 of comprehensive_test.py:
BASE_URL = "http://localhost:8000"

# Line ~100+: Making HTTP requests
response = httpx.put(
    f"{BASE_URL}/cache/{key}",
    json={"value": value, "ttl_seconds": None}
)
```

**Location in code:**
- Gateway API: [`distributed_cache/api/app.py`](distributed_cache/api/app.py)
- Entrypoint: [`distributed_cache/entrypoint.py`](distributed_cache/entrypoint.py)

**What happens:**
```
Client (your test script)
    ↓ (HTTP over localhost:8000)
Gateway (FastAPI at port 8000)
    ↓ (routes to cluster)
Cluster Manager
```

✔ **This simulates a real user request**

---

### Layer 2: In-Process Cluster (Internal Simulation)
```python
# Line 444 of comprehensive_test.py:
cluster, runtimes = build_local_cluster()

# Lines 465, 468, 475: Inspecting internal state
print(json.dumps(runtimes[primary_node].store.items(), indent=2))
print(json.dumps(runtimes[replica_node].store.items(), indent=2))
```

**Location in code:**
- Cluster builder: [`distributed_cache/entrypoint.py#build_local_cluster()`](distributed_cache/entrypoint.py#L8-L15)
- In-process transport: [`distributed_cache/cluster/transport.py#InProcessNodeTransport`](distributed_cache/cluster/transport.py#L19-L34)
- Node runtime: [`distributed_cache/cluster/runtime.py`](distributed_cache/cluster/runtime.py)

**What happens:**
```
Cluster Manager
    ↓ (direct function calls, NO network)
InProcessNodeTransport
    ↓
NodeRuntime instances (node-a, node-b, node-c)
    ↓
CacheStore (in-memory, per node)
```

✔ **This lets you inspect replication, failover, data distribution directly**

---

## 📊 Comparison Table

| Aspect | Your Current Setup | True Distributed |
|--------|-------------------|------------------|
| **Gateway** | ✅ HTTP (realistic) | ✅ HTTP (same) |
| **Inter-node communication** | ❌ Direct function calls | ✅ HTTP network |
| **Node isolation** | ❌ All in one process | ✅ Separate processes |
| **Testing internals** | ✅ Can inspect stores directly | ❌ Must use API endpoints |
| **Realistic for demo** | ⚠️ Semi-realistic | ✅ Fully realistic |
| **Easy to debug** | ✅ Yes (direct access) | ❌ Harder (need logs/tracing) |

---

## 🎯 What You're Actually Testing

### ✅ YOU ARE Testing (logically distributed):
- ✔ Consistent hashing algorithm
- ✔ Key-to-node mapping
- ✔ Primary + Replica selection
- ✔ Replication logic (data goes to both primary and replica)
- ✔ Failover logic (requests route to replica when primary dies)
- ✔ TTL expiration
- ✔ LRU eviction
- ✔ HTTP API layer

### ❌ YOU ARE NOT Testing (physically distributed):
- ✗ Real network latency
- ✗ Network packet loss
- ✗ HTTP timeout handling between nodes
- ✗ Process crashes and recovery
- ✗ Multi-process synchronization issues
- ✗ Real distributed debugging scenarios

---

## 🔧 How to Run Each Mode

### Mode 1: Current Setup (Simulated + HTTP)
```bash
# Terminal 1: Start gateway
./.venv/bin/python -m distributed_cache.entrypoint

# Terminal 2: Run tests
./.venv/bin/python scripts/comprehensive_test.py
```

**What's happening:**
- Gateway runs at port 8000
- All 3 nodes (node-a, b, c) run in-process inside gateway
- Tests make HTTP calls to gateway
- Internal nodes communicate via direct function calls

---

### Mode 2: Truly Distributed (Docker Compose)
```bash
# Builds and runs 3 separate node containers + 1 gateway container
docker-compose up
```

**What's happening:**
- Gateway at port 8000 (separate container)
- node-a at port 8001 (separate container)
- node-b at port 8002 (separate container)  
- node-c at port 8003 (separate container)
- Inter-node communication via real HTTP

**Test with:**
```bash
# Must set environment variable to use HTTP transport
export CLUSTER_TRANSPORT=http
./.venv/bin/python scripts/comprehensive_test.py
```

---

### Mode 3: Manual Multi-Node (for learning)
```bash
# Terminal 1: Start node-a
NODE_ID=node-a NODE_PORT=8001 ./.venv/bin/python -m distributed_cache.node_app

# Terminal 2: Start node-b
NODE_ID=node-b NODE_PORT=8002 ./.venv/bin/python -m distributed_cache.node_app

# Terminal 3: Start node-c
NODE_ID=node-c NODE_PORT=8003 ./.venv/bin/python -m distributed_cache.node_app

# Terminal 4: Start gateway with HTTP transport
CLUSTER_TRANSPORT=http ./.venv/bin/python -m distributed_cache.entrypoint

# Terminal 5: Run tests
./.venv/bin/python scripts/comprehensive_test.py
```

---

## 🎓 Code References for Each Layer

### Gateway & HTTP Layer
- **Entry point:** [`distributed_cache/entrypoint.py`](distributed_cache/entrypoint.py)
  - `build_local_cluster()` - creates simulated cluster
  - `build_cluster_from_env()` - creates HTTP-based cluster
- **API endpoints:** [`distributed_cache/api/app.py`](distributed_cache/api/app.py)
  - `PUT /cache/{key}` 
  - `GET /cache/{key}`
  - `DELETE /cache/{key}`
- **Node HTTP service:** [`distributed_cache/node_app.py`](distributed_cache/node_app.py)

### Cluster & Hashing Layer
- **Routing logic:** [`distributed_cache/cluster/manager.py`](distributed_cache/cluster/manager.py)
  - `route_for_key()` - determines primary + replica
  - `is_alive()` - checks heartbeat (failover detection)
- **Hash ring:** [`distributed_cache/cluster/consistent_hash.py`](distributed_cache/cluster/consistent_hash.py)
  - `get_node(key)` - finds primary
  - `get_successor_for_key()` - finds replica (skips same-server virtual nodes)
- **Cluster operations:** [`distributed_cache/cluster/service.py`](distributed_cache/cluster/service.py)
  - `put()` - write to primary + replicate
  - `get()` - read from served_by
  - `delete()` - delete from served_by + replica

### Transport Layer (the bridge)
- **In-process:** [`distributed_cache/cluster/transport.py#InProcessNodeTransport`](distributed_cache/cluster/transport.py)
  - Direct function calls (what you're using now)
- **HTTP:** [`distributed_cache/cluster/transport.py#HttpNodeTransport`](distributed_cache/cluster/transport.py)
  - Real HTTP calls (for distributed mode)

### Per-Node Storage
- **Node runtime:** [`distributed_cache/cluster/runtime.py`](distributed_cache/cluster/runtime.py)
  - Local operations on single node store
- **Cache store:** [`distributed_cache/cache/store.py`](distributed_cache/cache/store.py)
  - In-memory storage with TTL + LRU

---

## 📋 For Your Report/Viva

### Answer to "Is this a distributed system?"

> "Yes, this is a **logically distributed system simulator**. The architecture implements all distributed system concepts: consistent hashing, replication, failover, and independent node stores. However, it currently executes within a single process using direct function calls between nodes rather than network communication.
>
> The testing setup has two layers:
> 1. **External layer:** An HTTP gateway that simulates realistic client requests
> 2. **Internal layer:** In-process nodes that allow direct inspection of distributed behavior
>
> For a truly distributed deployment, the system supports Docker Compose mode where each node runs in a separate container with real HTTP communication between them."

---

## 🚀 Next Steps

**Which would you like to explore?**

1. **See a real distributed demo** - Run with Docker Compose and test multi-process failover
2. **Create a visual diagram** - Show the flow for your report
3. **Test HTTP transport** - Run manually in 5 terminals to watch real network behavior
4. **Verify replication** - Add extra logging to see data flow during writes
5. **Simulate network failure** - Add latency injection to test timeout behavior

Just let me know! 👍
