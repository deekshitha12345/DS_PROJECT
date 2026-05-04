from fastapi.testclient import TestClient

from distributed_cache.api.app import create_app
from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import InProcessNodeTransport


def test_http_api_round_trip() -> None:
    nodes = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
    ]
    manager = ClusterManager(nodes, heartbeat_timeout_seconds=2.0)
    runtimes = {node.node_id: NodeRuntime.create(node, max_items=8) for node in nodes}
    cluster = DistributedCacheCluster(manager, InProcessNodeTransport(runtimes))
    client = TestClient(create_app(cluster))

    put_response = client.put("/cache/hello", json={"value": "world", "ttl_seconds": None})
    assert put_response.status_code == 200

    get_response = client.get("/cache/hello")
    assert get_response.status_code == 200
    assert get_response.json()["value"] == "world"

    delete_response = client.delete("/cache/hello")
    assert delete_response.status_code == 200
