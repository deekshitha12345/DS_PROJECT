# Running Distributed Tests - Two Modes Explained

Your comprehensive_test.py now supports **two distinct testing modes**:

---

## 🎯 Mode 1: SIMULATED (Default)

### What It Is
All nodes run **in-process** within a single Python interpreter, communicating via direct function calls.

```
┌─────────────────────────────────────────────┐
│ Gateway (FastAPI)                           │
│                                             │
│  Cluster Manager                            │
│    ↓                                        │
│  InProcessNodeTransport                     │
│    ├─→ NodeRuntime (node-a) [in-process]   │
│    ├─→ NodeRuntime (node-b) [in-process]   │
│    └─→ NodeRuntime (node-c) [in-process]   │
│                                             │
└─────────────────────────────────────────────┘
         ↑ HTTP (port 8000)
    Your test script
```

### How to Run

**Terminal 1: Start the gateway**
```bash
cd /home/acer/Downloads/DS_PROJECT
./.venv/bin/python -m distributed_cache.entrypoint
```

Output:
```
INFO:     Uvicorn running on http://0.0.0.0:8000
```

**Terminal 2: Run the tests**
```bash
./.venv/bin/python scripts/comprehensive_test.py
```

Output shows:
```
════════════════════════════════════════════════════════════════════════════════
Test Mode: SIMULATED (In-Process)
════════════════════════════════════════════════════════════════════════════════
```

### ✅ Advantages
- **Fast**: No network latency
- **Debuggable**: Can inspect internal node state directly
- **Simple**: Only 2 commands to run
- **Deterministic**: No timing issues

### ❌ Limitations
- **Not real**: Nodes don't crash independently
- **No network testing**: Can't test network failures
- **Direct visibility**: Defeats some distributed isolation

### 📊 Best For
- **Learning** distributed concepts
- **Unit/integration testing**
- **Debugging** logic issues
- **CI/CD pipelines** (fast feedback)

---

## 🎯 Mode 2: DISTRIBUTED (Real)

### What It Is
Each node runs in a **separate Docker container** with **real network communication** between them.

```
┌────────────────────────────────────────────────────────────────┐
│ Docker Network (docker-compose)                               │
│                                                                │
│  ┌─────────────────┐  ┌──────────────┐  ┌────────────────┐   │
│  │ Gateway         │  │ node-a       │  │ node-b         │   │
│  │ (port 8000)     │  │ (port 8001)  │  │ (port 8002)    │   │
│  │                 │──┼──HTTP────────┼──┤                │   │
│  │ FastAPI         │  │ FastAPI      │  │ FastAPI        │   │
│  └────────┬────────┘  └──────────────┘  └────────────────┘   │
│           │                                                    │
│           └──────────────────────┬─────────────────────────┐  │
│                                  │                         │  │
│                            ┌──────────────┐  ┌─────────────┴──┐
│                            │ node-c       │  │ (each node     │
│                            │ (port 8003)  │  │  independent)  │
│                            │              │  │                │
│                            │ FastAPI      │  │                │
│                            └──────────────┘  └────────────────┘
│                                                                │
└────────────────────────────────────────────────────────────────┘
              ↑ HTTP (client requests)
         Your test script
```

### How to Run

**Terminal 1: Start docker-compose**
```bash
cd /home/acer/Downloads/DS_PROJECT
docker-compose up
```

Wait for output:
```
gateway_1 | INFO:     Uvicorn running on http://0.0.0.0:8000
node-a_1  | INFO:     Uvicorn running on http://0.0.0.0:8001
node-b_1  | INFO:     Uvicorn running on http://0.0.0.0:8002
node-c_1  | INFO:     Uvicorn running on http://0.0.0.0:8003
```

**Terminal 2: Run tests with distributed mode enabled**
```bash
cd /home/acer/Downloads/DS_PROJECT
CLUSTER_TRANSPORT=http ./.venv/bin/python scripts/comprehensive_test.py
```

Output shows:
```
════════════════════════════════════════════════════════════════════════════════
Test Mode: DISTRIBUTED (Real Network)
════════════════════════════════════════════════════════════════════════════════
```

### ✅ Advantages
- **Realistic**: True multi-process system
- **Network testing**: Tests latency, timeouts, failures
- **Isolation**: Nodes are truly independent
- **Production-like**: Closer to real deployment
- **Real failure scenarios**: Can kill containers to test recovery

### ❌ Limitations
- **Slower**: Network overhead
- **Harder to debug**: Can't inspect internal state directly
- **Complex setup**: Requires Docker
- **Timing issues**: Network delays affect test timing

### 📊 Best For
- **Integration testing**
- **Performance benchmarking**
- **Failure scenario testing**
- **Deployment validation**
- **Viva/presentation** (shows real distributed system)

---

## 🔄 Testing Failover in Distributed Mode

### Simulated Mode (Easy)
```python
# Directly manipulate heartbeat state
cluster.manager._last_heartbeat[node_id] = 0.0
# Gateway immediately routes to replica
```

### Distributed Mode (Real)
```bash
# Kill a node container
docker-compose kill node-a

# Gateway detects heartbeat timeout (after 3 seconds)
# All subsequent requests route to replica
# See logs: docker-compose logs gateway
```

---

## 📋 Quick Reference

### Start Simulated Testing
```bash
# Terminal 1
./.venv/bin/python -m distributed_cache.entrypoint

# Terminal 2
./.venv/bin/python scripts/comprehensive_test.py
```

### Start Distributed Testing
```bash
# Terminal 1
docker-compose up

# Terminal 2
CLUSTER_TRANSPORT=http ./.venv/bin/python scripts/comprehensive_test.py
```

### Stop Everything
```bash
# Kill gateway (Terminal 1)
Ctrl+C

# Kill docker-compose (Terminal 1)
docker-compose down
```

---

## 🧪 What Each Mode Tests

| Feature | Simulated | Distributed |
|---------|-----------|-------------|
| **CRUD Operations** | ✅ | ✅ |
| **Sharding** | ✅ | ✅ |
| **TTL Expiration** | ✅ | ✅ |
| **LRU Eviction** | ✅ | ✅ |
| **Replication** | ✅ Internal state | ⚠️ Via API |
| **Failover** | ✅ Simulated | ✅ Via timeout |
| **Network latency** | ❌ | ✅ |
| **Process isolation** | ❌ | ✅ |
| **Container recovery** | ❌ | ✅ |
| **Internal inspection** | ✅ Direct access | ❌ Logs only |

---

## 💡 Recommendation for Your Project

### For Learning & Development
→ Use **SIMULATED mode** (faster, easier to debug)

### For Integration Testing
→ Use **DISTRIBUTED mode** (realistic testing)

### For Your Viva/Presentation
→ Show **BOTH modes**:
1. Quick demo of simulated mode (fast)
2. Show distributed docker-compose (impressive)
3. Explain the difference

---

## 🚨 Common Issues

### "Connection refused" in simulated mode?
Make sure gateway is running in another terminal:
```bash
./.venv/bin/python -m distributed_cache.entrypoint
```

### Tests hang in distributed mode?
Containers might not be fully started. Wait 5 seconds after `docker-compose up`.

### Want to see container logs?
```bash
docker-compose logs -f gateway
docker-compose logs -f node-a
```

### Want to restart everything?
```bash
docker-compose down
docker-compose up
```

---

## 📊 Mode Indicators in Output

The test output will clearly show which mode is running:

**Simulated:**
```
════════════════════════════════════════════════════════════════════════════════
Test Mode: SIMULATED (In-Process)
════════════════════════════════════════════════════════════════════════════════
```

**Distributed:**
```
════════════════════════════════════════════════════════════════════════════════
Test Mode: DISTRIBUTED (Real Network)
════════════════════════════════════════════════════════════════════════════════
```

---

## 🎓 Architecture Comparison

**SIMULATED:**
```
HTTP Layer (realistic for clients)
         ↓
Cluster Manager (routes requests)
         ↓
InProcessNodeTransport (function calls)
         ↓
Per-node CacheStore (all in RAM)
```

**DISTRIBUTED:**
```
HTTP Layer (realistic for clients)
         ↓
Gateway Cluster Manager (routes requests)
         ↓
HttpNodeTransport (HTTP calls)
         ↓
Network (Docker bridge)
         ↓
Separate Node Processes (port 8001/8002/8003)
         ↓
Per-node CacheStore (isolated)
```

---

## 👍 Next Steps

1. **Try simulated mode first**: `python scripts/comprehensive_test.py`
2. **Then try distributed mode**: `docker-compose up` → `CLUSTER_TRANSPORT=http python scripts/comprehensive_test.py`
3. **Compare the outputs**: Notice which tests show different behavior
4. **Use for your viva**: Demonstrate understanding of both modes
