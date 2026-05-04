from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Iterable

from distributed_cache.cluster.consistent_hash import ConsistentHashRing
from distributed_cache.cluster.models import NodeConfig


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    primary: NodeConfig | None
    replica: NodeConfig | None
    served_by: NodeConfig | None


class ClusterManager:
    """Tracks membership, consistent hashing, and heartbeat-based liveness."""

    def __init__(self, nodes: Iterable[NodeConfig], heartbeat_timeout_seconds: float = 5.0, replicas: int = 32) -> None:
        if heartbeat_timeout_seconds <= 0:
            raise ValueError("heartbeat_timeout_seconds must be positive")
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._ring = ConsistentHashRing(replicas=replicas)
        self._nodes: dict[str, NodeConfig] = {}
        self._last_heartbeat: dict[str, float] = {}
        for node in nodes:
            self.add_node(node)

    def copy_ring(self) -> ConsistentHashRing:
        return self._ring.clone()

    def add_node(self, node: NodeConfig) -> None:
        self._nodes[node.node_id] = node
        self._ring.add_node(node)
        self._last_heartbeat.setdefault(node.node_id, monotonic())

    def remove_node(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)
        self._last_heartbeat.pop(node_id, None)
        self._ring.remove_node(node_id)

    def record_heartbeat(self, node_id: str) -> None:
        if node_id in self._nodes:
            self._last_heartbeat[node_id] = monotonic()

    def is_alive(self, node_id: str) -> bool:
        last_seen = self._last_heartbeat.get(node_id)
        if last_seen is None:
            return False
        return monotonic() - last_seen <= self._heartbeat_timeout_seconds

    def get_node(self, node_id: str) -> NodeConfig | None:
        return self._nodes.get(node_id)

    def get_all_nodes(self) -> list[NodeConfig]:
        return list(self._nodes.values())

    def route_for_key(self, key: str) -> RoutingDecision:
        primary = self._ring.get_node(key)
        if primary is None:
            return RoutingDecision(primary=None, replica=None, served_by=None)

        replica = self._ring.get_successor_for_key(key, exclude_node_id=primary.node_id)
        served_by = primary if self.is_alive(primary.node_id) else replica
        if served_by is not None and not self.is_alive(served_by.node_id):
            served_by = primary if self.is_alive(primary.node_id) else None

        return RoutingDecision(primary=primary, replica=replica, served_by=served_by)

    def alive_nodes(self) -> list[NodeConfig]:
        return [node for node in self._nodes.values() if self.is_alive(node.node_id)]

    def health_report(self) -> dict[str, bool]:
        return {node_id: self.is_alive(node_id) for node_id in self._nodes}
