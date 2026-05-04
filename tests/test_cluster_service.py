import pytest

from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster import consistent_hash
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import InProcessNodeTransport


def build_cluster() -> tuple[DistributedCacheCluster, dict[str, NodeRuntime]]:
    nodes = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
        NodeConfig("node-c", "127.0.0.1", 8003),
    ]
    manager = ClusterManager(nodes, heartbeat_timeout_seconds=2.0)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=8) for node in nodes}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))
    return cluster, runtimes


def build_rebalancing_cluster(monkeypatch) -> tuple[DistributedCacheCluster, dict[str, NodeRuntime]]:
    vnode_positions = {
        "node-a:0": 10,
        "node-b:0": 30,
        "node-c:0": 20,
        "case-primary-shift": 15,
        "case-replica-shift": 5,
    }

    monkeypatch.setattr(consistent_hash, "_hash_to_int", lambda value: vnode_positions.get(value, 999))

    node_specs = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
    ]
    manager = ClusterManager(node_specs, heartbeat_timeout_seconds=2.0, replicas=1)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=8) for node in node_specs}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))

    return cluster, runtimes


def build_failure_cluster(monkeypatch) -> tuple[DistributedCacheCluster, dict[str, NodeRuntime]]:
    vnode_positions = {
        "node-a:0": 10,
        "node-b:0": 20,
        "node-c:0": 30,
        "case-primary-failed": 15,
        "case-replica-failed": 5,
        "case-stable-after-remove": 25,
    }

    monkeypatch.setattr(consistent_hash, "_hash_to_int", lambda value: vnode_positions.get(value, 999))

    node_specs = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
        NodeConfig("node-c", "127.0.0.1", 8003),
    ]
    manager = ClusterManager(node_specs, heartbeat_timeout_seconds=2.0, replicas=1)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=8) for node in node_specs}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))

    return cluster, runtimes


@pytest.mark.asyncio
async def test_put_get_delete_across_cluster() -> None:
    cluster, runtimes = build_cluster()

    put_result = await cluster.put("user:1", {"name": "Ada"})
    assert put_result["ok"] is True

    get_result = await cluster.get("user:1")
    assert get_result["ok"] is True
    assert get_result["value"] == {"name": "Ada"}

    delete_result = await cluster.delete("user:1")
    assert delete_result["ok"] is True
    assert (await cluster.get("user:1"))["ok"] is False


@pytest.mark.asyncio
async def test_replica_gets_write_when_primary_is_alive() -> None:
    cluster, runtimes = build_cluster()

    result = await cluster.put("session:9", "token")
    assert result["ok"] is True

    stored = [runtime.store.items() for runtime in runtimes.values() if runtime.store.size() > 0]
    assert stored


@pytest.mark.asyncio
async def test_failover_serves_from_replica_when_primary_times_out() -> None:
    cluster, runtimes = build_cluster()
    routing = cluster.manager.route_for_key("order:7")
    assert routing.primary is not None

    cluster.manager._last_heartbeat[routing.primary.node_id] = 0.0
    await cluster.put("order:7", "paid")

    get_result = await cluster.get("order:7")
    assert get_result["ok"] is True
    assert get_result["value"] == "paid"


@pytest.mark.asyncio
async def test_add_node_rebalances_primary_and_preserves_ttl(monkeypatch) -> None:
    cluster, runtimes = build_rebalancing_cluster(monkeypatch)

    await cluster.put("case-primary-shift", "value", ttl_seconds=60.0)
    source_expiry = runtimes["node-b"].store.snapshot()["case-primary-shift"]["expires_at"]

    add_result = await cluster.add_node(NodeConfig("node-c", "127.0.0.1", 8003), NodeRuntime.create(NodeConfig("node-c", "127.0.0.1", 8003), max_items=8))
    assert add_result["ok"] is True
    assert add_result.get("status_code") == 202
    await cluster.wait_for_rebalance()

    assert runtimes["node-a"].store.get("case-primary-shift") is None
    assert runtimes["node-b"].store.get("case-primary-shift") == "value"
    assert runtimes["node-c"].store.get("case-primary-shift") == "value"

    destination_expiry = runtimes["node-c"].store.snapshot()["case-primary-shift"]["expires_at"]
    assert source_expiry is not None
    assert destination_expiry is not None
    assert abs(destination_expiry - source_expiry) < 1.0


@pytest.mark.asyncio
async def test_add_node_rebalances_replica_only(monkeypatch) -> None:
    cluster, runtimes = build_rebalancing_cluster(monkeypatch)

    await cluster.put("case-replica-shift", "replica-value")

    add_result = await cluster.add_node(NodeConfig("node-c", "127.0.0.1", 8003), NodeRuntime.create(NodeConfig("node-c", "127.0.0.1", 8003), max_items=8))
    assert add_result["ok"] is True
    await cluster.wait_for_rebalance()

    assert runtimes["node-a"].store.get("case-replica-shift") == "replica-value"
    assert runtimes["node-b"].store.get("case-replica-shift") is None
    assert runtimes["node-c"].store.get("case-replica-shift") == "replica-value"


@pytest.mark.asyncio
async def test_remove_node_rebalances_primary_and_replica(monkeypatch) -> None:
    cluster, runtimes = build_failure_cluster(monkeypatch)

    await cluster.put("case-primary-failed", "primary")
    await cluster.put("case-replica-failed", "replica")
    await cluster.put("case-stable-after-remove", "stable")

    remove_result = await cluster.remove_node("node-b")
    assert remove_result["ok"] is True
    await cluster.wait_for_rebalance()

    primary_routing = cluster.manager.route_for_key("case-primary-failed")
    replica_routing = cluster.manager.route_for_key("case-replica-failed")
    stable_routing = cluster.manager.route_for_key("case-stable-after-remove")

    assert primary_routing.primary is not None
    assert primary_routing.replica is not None
    assert primary_routing.primary.node_id == "node-c"
    assert primary_routing.replica.node_id == "node-a"

    assert replica_routing.primary is not None
    assert replica_routing.replica is not None
    assert replica_routing.primary.node_id == "node-a"
    assert replica_routing.replica.node_id == "node-c"

    assert stable_routing.primary is not None
    assert stable_routing.replica is not None
    assert stable_routing.primary.node_id == "node-c"
    assert stable_routing.replica.node_id == "node-a"

    assert runtimes["node-a"].store.get("case-primary-failed") == "primary"
    assert runtimes["node-c"].store.get("case-primary-failed") == "primary"
    assert runtimes["node-a"].store.get("case-replica-failed") == "replica"
    assert runtimes["node-c"].store.get("case-replica-failed") == "replica"
    assert runtimes["node-a"].store.get("case-stable-after-remove") == "stable"
    assert runtimes["node-c"].store.get("case-stable-after-remove") == "stable"
    assert cluster.manager.get_node("node-b") is None
