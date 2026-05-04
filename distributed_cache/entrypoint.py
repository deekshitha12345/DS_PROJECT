from __future__ import annotations

import os

from distributed_cache.api.app import create_app
from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import HttpNodeTransport, InProcessNodeTransport


def build_local_cluster() -> tuple[DistributedCacheCluster, dict[str, NodeRuntime]]:
    node_specs = [
        NodeConfig("node-a", "127.0.0.1", 8001),
        NodeConfig("node-b", "127.0.0.1", 8002),
        NodeConfig("node-c", "127.0.0.1", 8003),
    ]
    manager = ClusterManager(node_specs, heartbeat_timeout_seconds=3.0)
    runtimes = {spec.node_id: NodeRuntime.create(spec, max_items=256) for spec in node_specs}
    cluster = DistributedCacheCluster(manager=manager, transport=InProcessNodeTransport(runtimes))
    return cluster, runtimes


def build_cluster_from_env() -> tuple[DistributedCacheCluster, dict[str, NodeRuntime]]:
    transport_mode = os.getenv("CLUSTER_TRANSPORT", "local").lower()
    if transport_mode == "http":
        node_specs = [
            NodeConfig("node-a", os.getenv("NODE_A_HOST", "node-a"), int(os.getenv("NODE_A_PORT", "8001"))),
            NodeConfig("node-b", os.getenv("NODE_B_HOST", "node-b"), int(os.getenv("NODE_B_PORT", "8002"))),
            NodeConfig("node-c", os.getenv("NODE_C_HOST", "node-c"), int(os.getenv("NODE_C_PORT", "8003"))),
        ]
        manager = ClusterManager(node_specs, heartbeat_timeout_seconds=float(os.getenv("HEARTBEAT_TIMEOUT", "3.0")))
        cluster = DistributedCacheCluster(manager=manager, transport=HttpNodeTransport())
        return cluster, {}
    return build_local_cluster()


cluster, runtimes = build_cluster_from_env()
app = create_app(cluster)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
