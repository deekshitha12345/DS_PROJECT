from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256


def _hash_to_int(value: str) -> int:
    return int.from_bytes(sha256(value.encode("utf-8")).digest(), byteorder="big")


@dataclass(frozen=True)
class NodeAddress:
    node_id: str
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class ConsistentHashRing:
    """Consistent hash ring with virtual nodes for even distribution."""

    def __init__(self, replicas: int = 32) -> None:
        if replicas <= 0:
            raise ValueError("replicas must be positive")
        self._replicas = replicas
        self._ring: dict[int, NodeAddress] = {}
        self._sorted_keys: list[int] = []
        self._nodes: dict[str, NodeAddress] = {}

    def add_node(self, node: NodeAddress) -> None:
        self._nodes[node.node_id] = node
        for index in range(self._replicas):
            ring_key = _hash_to_int(f"{node.node_id}:{index}")
            self._ring[ring_key] = node
        self._sorted_keys = sorted(self._ring)

    def remove_node(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)
        keys_to_remove = [key for key, node in self._ring.items() if node.node_id == node_id]
        for key in keys_to_remove:
            self._ring.pop(key, None)
        self._sorted_keys = sorted(self._ring)

    def clone(self) -> "ConsistentHashRing":
        ring = ConsistentHashRing(replicas=self._replicas)
        ring._ring = dict(self._ring)
        ring._sorted_keys = list(self._sorted_keys)
        ring._nodes = dict(self._nodes)
        return ring

    def virtual_nodes(self) -> list[tuple[int, NodeAddress]]:
        return [(key, self._ring[key]) for key in self._sorted_keys]

    def get_node(self, key: str) -> NodeAddress | None:
        if not self._ring:
            return None
        position = _hash_to_int(key)
        index = bisect_right(self._sorted_keys, position)
        if index == len(self._sorted_keys):
            index = 0
        return self._ring[self._sorted_keys[index]]

    def get_nodes(self) -> list[NodeAddress]:
        return list(self._nodes.values())

    def get_successor(self, node_id: str) -> NodeAddress | None:
        if not self._ring:
            return None
        candidates = [key for key in self._sorted_keys if self._ring[key].node_id != node_id]
        if not candidates:
            return None
        return self._ring[candidates[0]]

    def get_successor_for_key(self, key: str, exclude_node_id: str | None = None) -> NodeAddress | None:
        if not self._ring:
            return None

        position = _hash_to_int(key)
        index = bisect_right(self._sorted_keys, position)
        if index == len(self._sorted_keys):
            index = 0

        for offset in range(len(self._sorted_keys)):
            candidate = self._ring[self._sorted_keys[(index + offset) % len(self._sorted_keys)]]
            if exclude_node_id is None or candidate.node_id != exclude_node_id:
                return candidate

        return None

    def get_node_ids(self) -> list[str]:
        return list(self._nodes)

    def groups(self) -> dict[str, list[NodeAddress]]:
        grouped: dict[str, list[NodeAddress]] = defaultdict(list)
        for node in self._nodes.values():
            grouped[node.node_id].append(node)
        return dict(grouped)
