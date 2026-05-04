from distributed_cache.cluster import consistent_hash
from distributed_cache.cluster.consistent_hash import ConsistentHashRing, NodeAddress


def test_ring_routes_keys_to_nodes() -> None:
    ring = ConsistentHashRing(replicas=4)
    ring.add_node(NodeAddress("node-a", "127.0.0.1", 8001))
    ring.add_node(NodeAddress("node-b", "127.0.0.1", 8002))

    node = ring.get_node("customer:42")

    assert node is not None
    assert node.node_id in {"node-a", "node-b"}


def test_ring_remove_node_and_reassign() -> None:
    ring = ConsistentHashRing(replicas=4)
    node_a = NodeAddress("node-a", "127.0.0.1", 8001)
    node_b = NodeAddress("node-b", "127.0.0.1", 8002)
    ring.add_node(node_a)
    ring.add_node(node_b)

    ring.remove_node("node-a")

    assert ring.get_node("customer:42") == node_b


def test_successor_for_key_skips_same_node_virtual_entries(monkeypatch) -> None:
    ring = ConsistentHashRing(replicas=1)
    node_a = NodeAddress("node-a", "127.0.0.1", 8001)
    node_b = NodeAddress("node-b", "127.0.0.1", 8002)
    node_c = NodeAddress("node-c", "127.0.0.1", 8003)

    ring._ring = {
        10: node_a,
        20: node_a,
        30: node_b,
        40: node_c,
    }
    ring._sorted_keys = [10, 20, 30, 40]

    monkeypatch.setattr(consistent_hash, "_hash_to_int", lambda value: 15)

    assert ring.get_successor_for_key("customer:42", exclude_node_id="node-a") == node_b
