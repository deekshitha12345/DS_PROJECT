# Comprehensive Test Guide

## What is `comprehensive_test.py`?

It's an **educational test script** that explains every step of testing the distributed cache system. Unlike traditional tests that just say "PASS" or "FAIL", this script shows you:

1. **What** is being tested
2. **Why** we're testing it
3. **How** the test works (step by step)
4. **What** the response looks like
5. **What** the response means
6. **Whether** the test passed or failed

## How to Use It

### Prerequisites
Make sure the cache gateway is running:
```bash
python -m distributed_cache.entrypoint
```

Then in another terminal:
```bash
cd /home/acer/Downloads/DS_PROJECT
python scripts/comprehensive_test.py
```

## What Gets Tested?

### TEST 1: BASIC CRUD OPERATIONS
- **Create (PUT)**: Write new data
- **Read (GET)**: Retrieve data
- **Update (PUT with existing key)**: Modify data
- **Delete**: Remove data

**What you learn**: How the basic cache operations work

---

### TEST 2: SHARDING (CONSISTENT HASHING)
- Writes 10 different keys
- Shows which node each key goes to
- Explains how the hash ring distributes data

**What you learn**: How data is distributed across nodes for load balancing

---

### TEST 3: TTL (TIME-TO-LIVE) EXPIRATION
- Writes a key with TTL=2 seconds
- Reads it immediately (exists)
- Waits 2.5 seconds
- Tries to read again (gone!)

**What you learn**: How keys automatically expire after a timeout

---

### TEST 4: LRU EVICTION
- Explains LRU (Least Recently Used)
- Runs the actual LRU test with a 2-item cache
- Shows how old items are automatically removed

**What you learn**: How cache memory is managed when full

---

### TEST 5: REPLICATION
- Writes a critical key
- Explains primary + replica setup
- Shows that data is accessible

**What you learn**: How data is backed up for fault tolerance

---

### TEST 6: FAILOVER MECHANISM
- Explains what happens when primary dies
- Shows how replica takes over
- Runs the failover test

**What you learn**: How high availability is achieved

---

### TEST 7: EDGE CASES & ERROR HANDLING
- Reads non-existent keys (returns 404)
- Deletes non-existent keys (returns 404)
- Tests different data types (strings, numbers, objects, lists)
- Tests large values (~1KB)

**What you learn**: How the system handles unusual situations gracefully

---

### TEST 8: PERFORMANCE EVALUATION
- Measures single PUT/GET latency
- Runs benchmark with 1000 operations
- Shows throughput (operations/second)

**What you learn**: How fast the system is

---

## Understanding the Output Format

Every test follows this structure:

```
═══════════════════════════════════════════════════════════════════
                    TEST NAME EXPLANATION
═══════════════════════════════════════════════════════════════════

STEP 1: What are we about to do?
────────────────────────────────────────────────────────────────────
→ ACTION: The specific action we're taking
→ REQUEST: What we're sending to the server
  {"payload": "details"}
← RESPONSE: What the server sends back
  {"response": "details"}

📝 EXPLANATION:
   Why this response? What does it mean? Why is it important?

✓ PASS: Test passed with message
```

---

## What Each Symbol Means

| Symbol | Meaning |
|--------|---------|
| `→` | Request we're sending to the server |
| `←` | Response from the server |
| `✓` | Test passed |
| `✗` | Test failed |
| `📝` | Explanation of what just happened |

---

## Example: Understanding a TTL Test

Here's what the script outputs:

```
→ REQUEST: PUT http://localhost:8000/cache/temp:session_token
  {"value": {"token": "abc123xyz"}, "ttl_seconds": 2.0}

← RESPONSE:
  {"ok": true, "status_code": 200, "served_by": "node-a"}

📝 EXPLANATION:
   We wrote a key with 'ttl_seconds': 2.0
   The server will automatically delete this key after 2 seconds.
   This is useful for: session tokens, OTP codes, temporary locks, etc.
```

**What's happening:**
1. We send a PUT request with TTL=2 seconds
2. Server responds with "ok: true" (success)
3. Explanation tells us what TTL does and why it's useful

---

## Performance Metrics Explained

The final performance test shows:

```
PUT Average Latency:      0.0751 ms
GET Average Latency:      0.0134 ms
Operations per Second:    11292 ops/sec
```

**What this means:**
- **PUT Latency**: Each write takes 0.0751 milliseconds on average
- **GET Latency**: Each read takes 0.0134 milliseconds on average
- **Throughput**: The system can handle 11,292 operations per second

**Is this good?**
- ✓ YES! Response times are sub-millisecond
- ✓ YES! Can serve thousands of requests per second
- ✓ YES! Suitable for high-traffic applications

---

## Test Results at a Glance

After all tests complete, you'll see:

```
Total Tests Run:  8
Passed:           8
Failed:           0

✓ ALL TESTS PASSED!
Your distributed cache system is working perfectly!
```

This means every single feature works as designed!

---

## Common Questions

### Q: Why is it 32ms for single operation but 0.07ms in benchmark?
**A:** Single operations include HTTP overhead (network communication). The benchmark runs 1000 operations in quick succession, so the average is much faster. In-memory operations themselves are sub-microsecond!

### Q: What if some tests fail?
**A:** The script will show `✗ FAIL: message`. Look at the explanation section to understand what went wrong.

### Q: Can I modify the script?
**A:** Yes! The script is in `scripts/comprehensive_test.py`. You can:
- Change test values
- Add new tests
- Adjust explanations
- Test your own scenarios

### Q: What if the gateway is not running?
**A:** You'll see connection errors. Make sure to run:
```bash
python -m distributed_cache.entrypoint
```
in another terminal first.

---

## Next Steps

Now that you understand how the system works:

1. **Run the full test**: `python scripts/comprehensive_test.py`
2. **Modify values**: Edit the script to test different scenarios
3. **Check the code**: Look at `distributed_cache/` folder to see implementation
4. **Run in Docker**: `docker compose up --build` for a real multi-node setup
5. **Build on it**: Use the cache in your own applications!

---

## Files Referenced

- Test script: [scripts/comprehensive_test.py](../scripts/comprehensive_test.py)
- Cache store: [distributed_cache/cache/store.py](../distributed_cache/cache/store.py)
- Cluster manager: [distributed_cache/cluster/manager.py](../distributed_cache/cluster/manager.py)
- HTTP API: [distributed_cache/api/app.py](../distributed_cache/api/app.py)
- Unit tests: [tests/](../tests/)
