#!/usr/bin/env python3
"""
Script to verify key ownership (primary & replica nodes) before and after removing a node.
Useful for manual validation of node failure rebalancing behavior.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import InProcessNodeTransport


async def print_key_ownership(cluster: DistributedCacheCluster, keys: list[str], stage: str) -> dict:
    """Print ownership information for all keys at a given stage."""
    print(f"\n{'='*80}")
    print(f"KEY OWNERSHIP - {stage}")
    print(f"{'='*80}")
    print(f"Nodes in cluster: {[n.node_id for n in cluster.manager.get_all_nodes()]}")
    print(f"\n{'Key':<20} {'Primary':<15} {'Replica':<15} {'Status'}")
    print(f"{'-'*70}")

    ownership_map = {}
    for key in keys:
        decision = cluster.manager.route_for_key(key)
        primary_name = decision.primary.node_id if decision.primary else "NONE"
        replica_name = decision.replica.node_id if decision.replica else "NONE"
        status = "✓" if decision.primary else "✗ NO PRIMARY"

        print(f"{key:<20} {primary_name:<15} {replica_name:<15} {status}")

        ownership_map[key] = {
            "primary": primary_name,
            "replica": replica_name,
        }

    return ownership_map


async def main():
    print("\n" + "="*80)
    print("DISTRIBUTED CACHE NODE REMOVAL VERIFICATION")
    print("="*80)

    # Step 1: Create initial cluster with 3 nodes
    print("\n[1/5] Creating initial cluster with 3 nodes...")
    node_specs = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
        NodeConfig("node-c", "127.0.0.1", 8003),
    ]
    manager = ClusterManager(node_specs, heartbeat_timeout_seconds=2.0, replicas=1)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=100) for node in node_specs}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))

    # Step 2: Add test data
    print("[2/5] Adding test data...")
    test_keys = [
        "user:1",
        "user:2",
        "user:3",
        "user:4",
        "user:5",
        "product:101",
        "product:102",
        "product:103",
        "session:abc",
        "session:def",
    ]

    for key in test_keys:
        result = await cluster.put(key, {"key": key, "value": f"data_for_{key}"})
        if not result.get("ok"):
            print(f"  ✗ Failed to put {key}")
        else:
            print(f"  ✓ Put {key}")

    # Step 3: Print ownership BEFORE removing node
    ownership_before = await print_key_ownership(cluster, test_keys, "BEFORE REMOVING NODE")

    # Step 4: Remove a node (simulate node failure)
    print(f"\n[3/5] Removing node (node-b) - simulating node failure...")
    result = await cluster.remove_node("node-b")
    if result.get("ok"):
        print(f"  ✓ Removed node-b and rebalanced data")
    else:
        print(f"  ✗ Failed to remove node-b: {result.get('message')}")

    # Step 5: Print ownership AFTER removing node
    ownership_after = await print_key_ownership(cluster, test_keys, "AFTER REMOVING NODE")

    # Step 6: Show changes
    print(f"\n{'='*80}")
    print("OWNERSHIP CHANGES")
    print(f"{'='*80}")
    print(f"{'Key':<20} {'Before Primary':<15} {'After Primary':<15} {'Changed?'}")
    print(f"{'-'*70}")

    changed_keys = []
    for key in test_keys:
        before_primary = ownership_before[key]["primary"]
        after_primary = ownership_after[key]["primary"]
        changed = before_primary != after_primary

        if changed:
            changed_keys.append(key)
            symbol = "★ YES"
        else:
            symbol = "  no"

        print(f"{key:<20} {before_primary:<15} {after_primary:<15} {symbol}")

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total keys: {len(test_keys)}")
    print(f"Keys with changed primary: {len(changed_keys)}")
    print(f"Keys with unchanged primary: {len(test_keys) - len(changed_keys)}")

    if changed_keys:
        print(f"\nKeys that moved to new primary (due to node-b removal):")
        for key in changed_keys:
            before = ownership_before[key]["primary"]
            after = ownership_after[key]["primary"]
            print(f"  {key:<20} {before} → {after}")

    print(f"\n{'='*80}")
    print("Verification complete!")
    print("="*80)
    print("\nNote: Keys whose primary was node-b have been repaired:")
    print("  - Promoted from replica to new primary, OR")
    print("  - Assigned to new primary from remaining nodes")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    asyncio.run(main())
