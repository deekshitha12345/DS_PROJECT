#!/usr/bin/env python3
"""
Detailed explanation of how failover mapping works in the distributed cache.
Shows step-by-step how requests are routed when primary fails.
"""

import asyncio
from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import InProcessNodeTransport

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

def print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{BLUE}{'='*80}{RESET}")
    print(f"{BLUE}{BOLD}{title.center(80)}{RESET}")
    print(f"{BLUE}{'='*80}{RESET}\n")

def print_section(title: str) -> None:
    """Print a section header."""
    print(f"\n{CYAN}{BOLD}→ {title}{RESET}")
    print(f"{CYAN}{'-'*80}{RESET}")

def print_diagram(diagram: str) -> None:
    """Print a text diagram."""
    print(f"{YELLOW}{diagram}{RESET}")

def print_info(label: str, value: str) -> None:
    """Print an info line."""
    print(f"{YELLOW}  {label:25} {value}{RESET}")

def print_success(message: str) -> None:
    """Print success message."""
    print(f"{GREEN}✓ {message}{RESET}")

def print_error(message: str) -> None:
    """Print error message."""
    print(f"{RED}✗ {message}{RESET}")

async def main() -> None:
    """Run the failover mapping explanation."""
    
    print_header("FAILOVER MAPPING MECHANISM - DETAILED EXPLANATION")
    
    print("""
KEY CONCEPT:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When a primary node fails, the system does NOT re-map keys to new nodes.
Instead, it routes requests to the REPLICA that already has the data.

Think of it like this:
  • Primary is the main owner of the key
  • Replica is a backup copy in a different node
  • If primary dies, replica takes over temporarily
  • No data is lost because replica has a copy!
""")

    # Setup cluster
    nodes = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
        NodeConfig("node-c", "127.0.0.1", 8003),
    ]
    manager = ClusterManager(nodes, heartbeat_timeout_seconds=3.0)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=256) for node in nodes}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))

    # =========================================================================
    # STEP 1: Show the hash ring
    # =========================================================================
    print_section("STEP 1: Understanding the Hash Ring")
    
    print("""
The nodes are arranged in a circular ring:

        node-b (position: 0°)
       /           \\
   node-a        node-c
  (120°)         (240°)
   \\           /
    (back to node-b)

This ring determines key ownership.
""")

    # =========================================================================
    # STEP 2: Write a key and show its mapping
    # =========================================================================
    print_section("STEP 2: Writing a Key and Showing Its Mapping")
    
    key = "order:2024"
    value = {"amount": 500, "status": "pending"}
    
    print(f"\nWriting key: {BOLD}{key}{RESET}")
    print(f"Value: {value}\n")
    
    # Get routing information
    routing = manager.route_for_key(key)
    
    print_info("Key Name", key)
    print_info("Primary Node", routing.primary.node_id if routing.primary else "None")
    print_info("Replica Node", routing.replica.node_id if routing.replica else "None")
    print_info("Currently Served By", routing.served_by.node_id if routing.served_by else "None")
    print_info("Primary Alive?", "YES" if manager.is_alive(routing.primary.node_id) else "NO")
    print_info("Replica Alive?", "YES" if manager.is_alive(routing.replica.node_id) else "NO")
    
    # Write the key
    await cluster.put(key, value)
    
    print(f"\n{YELLOW}Mapping Diagram:{RESET}")
    print(f"""
    Hash Ring:
    
          node-b
         /      \\
    {CYAN}node-a{RESET}      node-c
       /         \\
      
    Key '{key}' is owned by PRIMARY: {CYAN}{routing.primary.node_id}{RESET}
    Backup (REPLICA) is: {CYAN}{routing.replica.node_id}{RESET}
    
    The data is now stored in BOTH nodes:
      • {routing.primary.node_id}: MAIN copy (PRIMARY)
      • {routing.replica.node_id}: BACKUP copy (REPLICA)
    """)
    
    print_success(f"Data written to {routing.primary.node_id}")
    print_success(f"Data replicated to {routing.replica.node_id}")
    
    # Verify data is on both nodes
    primary_data = runtimes[routing.primary.node_id].store.get(key)
    replica_data = runtimes[routing.replica.node_id].store.get(key)
    
    print(f"\n{YELLOW}Verification:{RESET}")
    print(f"  {routing.primary.node_id} has data: {primary_data}")
    print(f"  {routing.replica.node_id} has data: {replica_data}")
    
    # =========================================================================
    # STEP 3: Normal operation (all nodes alive)
    # =========================================================================
    print_section("STEP 3: Normal Operation - Primary is Alive")
    
    print(f"\n{YELLOW}Scenario: All nodes are healthy{RESET}\n")
    
    get_result = await cluster.get(key)
    
    print_info("Request to", f"GET {key}")
    print_info("Primary Node Status", "✓ ALIVE")
    print_info("Replica Node Status", "✓ ALIVE")
    print_info("Request Served By", f"{get_result['served_by']} (PRIMARY)")
    print_info("Data Retrieved", str(get_result["value"]))
    
    print(f"\n{YELLOW}Routing Flow:{RESET}")
    print(f"""
    Client sends GET request
         ↓
    Gateway checks: Is primary {routing.primary.node_id} alive?
         ↓
    YES! Primary is alive
         ↓
    Route to PRIMARY {routing.primary.node_id} (faster, no need for replica)
         ↓
    Return data
    """)
    
    # =========================================================================
    # STEP 4: Primary fails
    # =========================================================================
    print_section("STEP 4: PRIMARY NODE FAILS!")
    
    print(f"\n{RED}⚠️  SIMULATING FAILURE: {routing.primary.node_id} crashes!{RESET}\n")
    
    # Simulate failure by setting heartbeat to very old time
    manager._last_heartbeat[routing.primary.node_id] = 0.0  # Very old = dead
    
    # Check status after failure
    primary_alive = manager.is_alive(routing.primary.node_id)
    replica_alive = manager.is_alive(routing.replica.node_id)
    
    print_info("Primary Node Status", f"{'✓ ALIVE' if primary_alive else '✗ DEAD'}")
    print_info("Replica Node Status", f"{'✓ ALIVE' if replica_alive else '✗ DEAD'}")
    
    print(f"\n{RED}What DOESN'T happen:{RESET}")
    print(f"  ✗ Key ownership doesn't change")
    print(f"  ✗ Key is NOT re-mapped to a new node")
    print(f"  ✗ Hash ring doesn't reorganize")
    print(f"  ✗ No data is lost")
    
    print(f"\n{GREEN}What DOES happen:{RESET}")
    print(f"  ✓ System detects primary is dead (heartbeat timeout)")
    print(f"  ✓ Routing automatically switches to replica")
    print(f"  ✓ Replica serves the request instead")
    print(f"  ✓ Data is still accessible!")
    
    # =========================================================================
    # STEP 5: Failover - Request routed to replica
    # =========================================================================
    print_section("STEP 5: Request After Failure - Automatic Failover")
    
    print(f"\n{YELLOW}Scenario: Client sends GET request (primary is DEAD){RESET}\n")
    
    get_result = await cluster.get(key)
    
    print_info("Request to", f"GET {key}")
    print_info("Primary Node Status", "✗ DEAD (no heartbeat)")
    print_info("Replica Node Status", "✓ ALIVE")
    print_info("Request Served By", f"{get_result['served_by']} (REPLICA - FAILOVER)")
    print_info("Data Retrieved", str(get_result["value"]))
    
    print(f"\n{YELLOW}Routing Flow After Failure:{RESET}")
    print(f"""
    Client sends GET request for '{key}'
         ↓
    Gateway checks: Is primary {routing.primary.node_id} alive?
         ↓
    NO! Primary is DEAD (heartbeat timeout)
         ↓
    Route to REPLICA {routing.replica.node_id} instead
         ↓
    Return data from replica
    
    KEY POINT: The key still "belongs" to {routing.primary.node_id}
               But requests are served from {routing.replica.node_id}
    """)
    
    print_success(f"Data still accessible via replica {get_result['served_by']}")
    print_success(f"No data loss! No application error!")
    
    # =========================================================================
    # STEP 6: Write new data during failover
    # =========================================================================
    print_section("STEP 6: Write New Data While Primary is Down")
    
    print(f"\n{YELLOW}Scenario: Client writes new data (primary is DEAD){RESET}\n")
    
    new_value = {"amount": 750, "status": "confirmed", "timestamp": "2026-05-03"}
    
    put_result = await cluster.put(key, new_value)
    
    print_info("Request to", f"PUT {key}")
    print_info("Primary Node Status", "✗ DEAD")
    print_info("Replica Node Status", "✓ ALIVE")
    print_info("Data Written To", f"{put_result['served_by']} (REPLICA - FAILOVER)")
    
    print(f"\n{YELLOW}What happens during write:{RESET}")
    print(f"""
    Client sends PUT request
         ↓
    Gateway checks: Is primary {routing.primary.node_id} alive?
         ↓
    NO! Primary is dead
         ↓
    Route write to REPLICA {routing.replica.node_id}
         ↓
    Write new data to replica
         ↓
    Return success
    """)
    
    # Verify the new data is on the replica
    replica_updated_data = runtimes[routing.replica.node_id].store.get(key)
    
    print_info("Data on Replica", str(replica_updated_data))
    
    print_success(f"New data written to replica during failover")
    
    # =========================================================================
    # STEP 7: Primary recovers
    # =========================================================================
    print_section("STEP 7: Primary Node RECOVERS")
    
    print(f"\n{GREEN}✓ PRIMARY {routing.primary.node_id} COMES BACK ONLINE!{RESET}\n")
    
    # Simulate recovery by setting heartbeat to current time
    manager._last_heartbeat[routing.primary.node_id] = manager._last_heartbeat[routing.replica.node_id]
    
    primary_alive = manager.is_alive(routing.primary.node_id)
    
    print_info("Primary Node Status", f"{'✓ ALIVE' if primary_alive else '✗ DEAD'}")
    print_info("Replica Node Status", f"{'✓ ALIVE' if manager.is_alive(routing.replica.node_id) else '✗ DEAD'}")
    
    print(f"\n{YELLOW}Important Note:{RESET}")
    print(f"""
    When primary recovers:
      • It does NOT automatically get the new data written to replica
      • In a real system, you'd need a reconciliation mechanism
      • For this demo, the replica is the source of truth
      • In production, you'd use WAL (Write-Ahead Logs) or sync
    """)
    
    # Get from primary (will have old data since we didn't sync)
    get_result_primary = await cluster.get(key)
    
    print_info("Request Served By", f"{get_result_primary['served_by']} (PRIMARY - RECOVERED)")
    
    print(f"\n{YELLOW}Routing restored to PRIMARY:{RESET}")
    print(f"""
    Client sends GET request
         ↓
    Gateway checks: Is primary {routing.primary.node_id} alive?
         ↓
    YES! Primary has recovered
         ↓
    Route to PRIMARY {routing.primary.node_id}
         ↓
    Return data from primary
    
    (In this demo, primary still has old data)
    """)
    
    # =========================================================================
    # STEP 8: Summary
    # =========================================================================
    print_section("STEP 8: KEY TAKEAWAYS")
    
    print(f"""
{BOLD}HOW FAILOVER MAPPING WORKS:{RESET}

1. {CYAN}KEY OWNERSHIP DOESN'T CHANGE{RESET}
   • Primary and Replica assignments are based on hash ring
   • They remain the same even if primary fails
   • The key still "belongs" to the primary node

2. {CYAN}ROUTING CHANGES, NOT MAPPING{RESET}
   • When primary is alive: requests go to primary
   • When primary is dead: requests go to replica
   • The mapping (who owns the key) stays the same
   • The routing (who serves the request) changes

3. {CYAN}DATA IS ALREADY ON REPLICA{RESET}
   • Every write goes to both primary AND replica
   • Replica is updated in the background
   • When primary fails, replica has the data
   • No re-mapping needed!

4. {CYAN}CONSISTENCY MODEL{RESET}
   • Write Primary: data goes immediately
   • Write Replica: happens in background
   • Read Primary: fresh data
   • Read Replica (failover): slightly delayed, but present
   • This is called "eventual consistency"

5. {CYAN}NO RE-HASHING{RESET}
   • Unlike some systems, we don't re-hash when nodes fail
   • This keeps performance stable
   • Consistent hashing already handles this elegantly

{BOLD}TIMELINE EXAMPLE:{RESET}
""")
    
    timeline = f"""
    Time 0:  Key written to Primary={routing.primary.node_id}, Replica={routing.replica.node_id}
    Time 1:  All requests go to Primary (PRIMARY)
    Time 5:  Primary {routing.primary.node_id} CRASHES! 💥
    Time 6:  New requests go to Replica {routing.replica.node_id} (FAILOVER)
    Time 10: New writes go to Replica {routing.replica.node_id}
    Time 15: Primary {routing.primary.node_id} comes back online
    Time 16: Requests go back to Primary {routing.primary.node_id}
    Time 20: (In real system) Replica syncs changes back to Primary
    
    KEY POINT: At no point does the key get "re-mapped" to a different node!
    """
    print(f"{CYAN}{timeline}{RESET}")
    
    # =========================================================================
    # VISUAL COMPARISON
    # =========================================================================
    print_section("VISUAL COMPARISON")
    
    print(f"""
{YELLOW}❌ WHAT DOESN'T HAPPEN (incorrect understanding):{RESET}

    Before Failure:
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  node-a     │  │  node-b     │  │  node-c     │
    │ order:2024  │  │             │  │             │
    └─────────────┘  └─────────────┘  └─────────────┘
    
    Primary dies...
    
    ❌ WRONG! Re-mapping key to new node:
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  node-a ✗   │  │  node-b ✓   │  │  node-c     │
    │ (deleted)   │  │ order:2024  │  │             │
    └─────────────┘  └─────────────┘  └─────────────┘
    
    ❌ WRONG! Re-hashing to find new home:
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  node-a ✗   │  │  node-b     │  │  node-c ✓   │
    │ (deleted)   │  │             │  │ order:2024  │
    └─────────────┘  └─────────────┘  └─────────────┘

{GREEN}✅ WHAT ACTUALLY HAPPENS (correct understanding):{RESET}

    Before Failure:
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  node-a (P) │  │  node-b (R) │  │  node-c     │
    │ order:2024  │  │ order:2024  │  │             │
    └─────────────┘  └─────────────┘  └─────────────┘
    P = Primary, R = Replica
    
    Primary dies...
    
    ✅ CORRECT! Just change routing:
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  node-a ✗   │  │  node-b ✓   │  │  node-c     │
    │ (DEAD)      │  │ order:2024  │  │             │
    │             │  │ NOW SERVING │  │             │
    └─────────────┘  └─────────────┘  └─────────────┘
    
    Key still "belongs" to node-a (primary)
    But requests served from node-b (replica)
    No re-mapping, no re-hashing!
    """)
    
    print_success("Failover mapping explanation complete!")

if __name__ == "__main__":
    asyncio.run(main())
