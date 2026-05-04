# Failover Mapping Mechanism - Quick Reference

## ❓ The Question: "How is the key mapped to the next node when primary fails?"

**Short Answer:** It's NOT. The key doesn't get re-mapped. Instead, **routing switches to the replica**.

---

## 🎯 The Key Insight

Think of it like a restaurant with a head chef and a sous chef:

- **Head Chef (Primary)**: Main responsibility for the kitchen
- **Sous Chef (Replica)**: Knows all the recipes, stands ready as backup
- **When head chef gets sick**: Sous chef takes over temporarily
- **The responsibilities don't change**: It's still the same restaurant
- **Just the person serving changes**: From head chef to sous chef

---

## 📊 Visual Comparison

### ❌ WRONG Understanding: Re-mapping keys

```
BEFORE FAILURE:
┌──────────┐  ┌──────────┐  ┌──────────┐
│ node-a   │  │ node-b   │  │ node-c   │
│ order:1  │  │ order:2  │  │ order:3  │
└──────────┘  └──────────┘  └──────────┘

node-a CRASHES! 💥

❌ WRONG: Keys get moved to other nodes
┌──────────┐  ┌──────────┐  ┌──────────┐
│ DEAD     │  │ order:2  │  │ order:3  │
│          │  │ order:1? │  │ order:1? │
└──────────┘  └──────────┘  └──────────┘
              (Which one gets order:1?)
```

**Problem with this approach:**
- Need to decide which node gets the key
- Involves complex re-hashing
- Time-consuming reorganization
- Possible data loss/conflicts
- Replica data would be ignored

---

### ✅ CORRECT Understanding: Routing changes

```
BEFORE FAILURE:
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ node-a       │  │ node-b       │  │ node-c       │
│ P: order:1   │  │ P: order:2   │  │ P: order:3   │
│ R: from node-c   R: from node-a   R: from node-b  │
└──────────────┘  └──────────────┘  └──────────────┘
P = Primary    R = Replica (backup)

node-a CRASHES! 💥

✅ CORRECT: Just change routing, keep mapping same
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ ✗ DEAD       │  │ node-b       │  │ node-c       │
│ order:1 (P)  │  │ order:2 (P)  │  │ order:3 (P)  │
│ order:1 (R)  │  │ order:2 (R)  │  │ order:3 (R)  │
│ [SERVED FROM ]   [FROM node-a] │  [FROM node-b] │
│ [REPLICA]    │  │ [NOW HERE]   │  │              │
└──────────────┘  └──────────────┘  └──────────────┘

Requests for order:1 now served from node-c (replica)
```

**Advantages of this approach:**
- NO re-hashing needed
- Instant failover (just change routing)
- Replica already has the data
- Simple and elegant

---

## 📋 The Routing Decision Logic

```python
# When a request comes in for a key:

routing = manager.route_for_key(key)
# Returns: {
#   "primary": node-c,        (who owns the key)
#   "replica": node-b,        (who has backup)
#   "served_by": ?            (who serves the request)
# }

# Determine who serves the request:
if primary.is_alive():
    served_by = primary       # Serve from primary
else:
    served_by = replica       # Serve from replica (failover)

# Send request to served_by node
response = await transport.get(served_by, key)
```

---

## 🔄 Request Flow Comparison

### Scenario 1: Primary is ALIVE ✓

```
Client Request: GET order:2024
    ↓
Gateway checks: Is primary node-c alive?
    ↓
YES ✓
    ↓
Route to: node-c (PRIMARY)
    ↓
Response: Fresh data from primary
```

**Node Assignment:** NEVER CHANGES  
**Key Ownership:** node-c (primary), node-b (replica)  
**Request Served By:** node-c

---

### Scenario 2: Primary FAILS ✗

```
Client Request: GET order:2024
    ↓
Gateway checks: Is primary node-c alive?
    ↓
NO ✗ (Heartbeat timeout)
    ↓
Route to: node-b (REPLICA) [FAILOVER!]
    ↓
Response: Data from replica (already there from replication)
```

**Node Assignment:** SAME AS BEFORE (didn't change)  
**Key Ownership:** node-c (primary), node-b (replica)  
**Request Served By:** node-b (changed)

---

## 🔑 Why This Works

### Data Replication

Every time you write a key, it goes to:
1. **Primary node** - Main copy, written immediately
2. **Replica node** - Backup copy, written in background

```
PUT order:2024 = {amount: 500}

Step 1: Write to primary node-c
  node-c: {order:2024 = {amount: 500}}

Step 2: Replicate to replica node-b
  node-b: {order:2024 = {amount: 500}}

Both nodes now have identical data!
```

### Failover Mechanism

When primary fails:
```
Primary node-c: ✗ DEAD (no heartbeat)
Replica node-b: ✓ ALIVE (and has all data)

Route all requests to node-b until node-c recovers
```

---

## ⏱️ Timeline Example

```
Time 0:   Key written to Primary=node-c, Replica=node-b
          Data is: {amount: 500}

Time 1:   Request: GET order:2024
          Router: primary alive? YES
          Served from: node-c

Time 5:   Primary node-c CRASHES! 💥
          (Heartbeat messages stop)

Time 6:   Request: GET order:2024
          Router: primary alive? NO → failover
          Served from: node-b (replica)
          Data returned: {amount: 500} ✓ (same!)

Time 8:   Request: PUT order:2024 = {amount: 750}
          Router: primary alive? NO → failover
          Served from: node-b (replica)
          Data written: node-b has {amount: 750}

Time 12:  Primary node-c RECOVERS! 🟢
          (Heartbeat messages resume)

Time 13:  Request: GET order:2024
          Router: primary alive? YES
          Served from: node-c (back to normal)
          Data returned: {amount: 500} (old data!)
          
NOTE: In real systems, you'd reconcile this using:
      - Write-Ahead Logs (WAL)
      - Change Data Capture (CDC)
      - Manual synchronization
      - Consensus protocols
```

---

## 🎓 Key Differences Explained

| Aspect | What Changes | What Stays Same |
|--------|--------------|-----------------|
| **Key Ownership** | ❌ Never | ✅ Always: Primary + Replica |
| **Hash Ring** | ❌ Never | ✅ Always: Same positions |
| **Request Routing** | ✅ YES | ❌ If primary fails, goes to replica |
| **Data Location** | ❌ Never moved | ✅ Already on replica from replication |
| **Re-hashing** | ❌ Not needed | ✅ Consistent hashing handles it |

---

## 💡 Summary

**The Answer to Your Question:**

When primary node fails:
- ✅ **Routing changes** - requests go to replica instead
- ❌ **Key doesn't get re-mapped** - primary/replica assignments stay same
- ❌ **Hash ring doesn't reorganize** - positions stay same
- ✅ **Data is already on replica** - no movement needed
- ✅ **System continues working** - seamless failover

**In One Sentence:**
> The key always "belongs" to its primary node, but if primary dies, requests are automatically served from the replica that already has a copy of the data.

---

## 🚀 Run the Visual Explanation

To see this in action, run:

```bash
cd /home/acer/Downloads/DS_PROJECT
./.venv/bin/python scripts/failover_mapping_explained.py
```

This shows all 7 steps of the failover process with real code execution!
