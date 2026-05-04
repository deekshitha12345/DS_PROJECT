from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Any

from distributed_cache.cluster.manager import ClusterManager
from distributed_cache.cluster.models import NodeConfig, OperationResult
from distributed_cache.cluster.runtime import NodeRuntime
from distributed_cache.cluster.transport import NodeTransport


@dataclass(slots=True)
class CacheValue:
    key: str
    value: Any
    ttl_seconds: float | None = None


class DistributedCacheCluster:
    def __init__(self, manager: ClusterManager, transport: NodeTransport) -> None:
        self.manager = manager
        self.transport = transport

    async def add_node(self, node: NodeConfig, runtime: NodeRuntime | None = None) -> dict[str, Any]:
        if self.manager.get_node(node.node_id) is not None:
            return {"ok": False, "status_code": 409, "message": "node already exists"}

        previous_ring = self.manager.copy_ring()

        register_node = getattr(self.transport, "register_node", None)
        if callable(register_node):
            register_node(node, runtime)

        self.manager.add_node(node)
        await self._rebalance_after_add(previous_ring)

        return {"ok": True, "status_code": 200, "served_by": node.node_id}

    async def put(self, key: str, value: Any, ttl_seconds: float | None = None) -> dict[str, Any]:
        routing = self.manager.route_for_key(key)
        if routing.served_by is None:
            return {"ok": False, "status_code": 503, "message": "cluster has no live nodes"}

        target = routing.served_by
        result = await self.transport.put(target, key, value, ttl_seconds)
        if not result.ok:
            return {"ok": False, "status_code": result.status_code, "message": result.message}

        self.manager.record_heartbeat(target.node_id)
        if routing.replica is not None and routing.replica.node_id != target.node_id and self.manager.is_alive(routing.replica.node_id):
            await self.transport.put(routing.replica, key, value, ttl_seconds)

        return {"ok": True, "status_code": 200, "served_by": target.node_id}

    async def get(self, key: str) -> dict[str, Any]:
        routing = self.manager.route_for_key(key)
        if routing.served_by is None:
            return {"ok": False, "status_code": 503, "message": "cluster has no live nodes"}

        target = routing.served_by
        found, value = await self.transport.get(target, key)
        if found:
            self.manager.record_heartbeat(target.node_id)
            return {"ok": True, "status_code": 200, "value": value, "served_by": target.node_id}

        if routing.replica is not None and routing.replica.node_id != target.node_id and self.manager.is_alive(routing.replica.node_id):
            found, value = await self.transport.get(routing.replica, key)
            if found:
                return {"ok": True, "status_code": 200, "value": value, "served_by": routing.replica.node_id}

        return {"ok": False, "status_code": 404, "message": "not found"}

    async def delete(self, key: str) -> dict[str, Any]:
        routing = self.manager.route_for_key(key)
        if routing.served_by is None:
            return {"ok": False, "status_code": 503, "message": "cluster has no live nodes"}

        target = routing.served_by
        result = await self.transport.delete(target, key)
        if not result.ok:
            return {"ok": False, "status_code": result.status_code, "message": result.message}

        if routing.replica is not None and routing.replica.node_id != target.node_id and self.manager.is_alive(routing.replica.node_id):
            await self.transport.delete(routing.replica, key)

        return {"ok": True, "status_code": 200, "served_by": target.node_id}

    async def heartbeat(self, node_id: str) -> dict[str, Any]:
        node = self.manager.get_node(node_id)
        if node is None:
            return {"ok": False, "status_code": 404, "message": "unknown node"}
        await self.transport.heartbeat(node)
        self.manager.record_heartbeat(node_id)
        return {"ok": True, "status_code": 200}

    async def remove_node(self, node_id: str) -> dict[str, Any]:
        if self.manager.get_node(node_id) is None:
            return {"ok": False, "status_code": 404, "message": "unknown node"}

        previous_ring = self.manager.copy_ring()
        self.manager.remove_node(node_id)
        await self._rebalance_after_remove(previous_ring)
        return {"ok": True, "status_code": 200}

    async def _rebalance_after_add(self, previous_ring) -> None:
        current_ring = self.manager.copy_ring()
        snapshots: dict[str, dict[str, dict[str, Any | None]]] = {}

        for node in self.manager.get_all_nodes():
            snapshots[node.node_id] = await self.transport.snapshot(node)

        key_locations: dict[str, set[str]] = {}
        for node_id, entries in snapshots.items():
            for key in entries:
                key_locations.setdefault(key, set()).add(node_id)

        now = monotonic()
        nodes_by_id = {node.node_id: node for node in self.manager.get_all_nodes()}

        for key in sorted(key_locations):
            old_primary = previous_ring.get_node(key)
            if old_primary is None:
                continue

            old_replica = previous_ring.get_successor_for_key(key, exclude_node_id=old_primary.node_id)
            new_primary = current_ring.get_node(key)
            if new_primary is None:
                continue
            new_replica = current_ring.get_successor_for_key(key, exclude_node_id=new_primary.node_id)

            old_owner_ids = {node.node_id for node in (old_primary, old_replica) if node is not None}
            new_owner_ids = {node.node_id for node in (new_primary, new_replica) if node is not None}

            if old_owner_ids == new_owner_ids:
                continue

            source_node_id = self._source_node_id_for_key(key, previous_ring)
            source_entry = self._get_entry_snapshot(snapshots, key, source_node_id)
            if source_entry is None:
                source_entry = self._get_first_entry_snapshot(snapshots, key)
            if source_entry is None:
                continue

            expires_at = source_entry.get("expires_at")
            ttl_seconds: float | None = None
            if expires_at is not None:
                ttl_seconds = max(float(expires_at) - now, 0.0)
                if ttl_seconds <= 0:
                    continue

            for node_id in new_owner_ids:
                if node_id not in nodes_by_id:
                    continue
                if key in snapshots.get(node_id, {}):
                    continue
                await self.transport.put(nodes_by_id[node_id], key, source_entry["value"], ttl_seconds)

            for node_id, entries in snapshots.items():
                if node_id in new_owner_ids or key not in entries:
                    continue
                node = nodes_by_id.get(node_id)
                if node is not None:
                    await self.transport.delete(node, key)

    def _source_node_id_for_key(self, key: str, ring) -> str | None:
        primary = ring.get_node(key)
        if primary is None:
            return None

        replica = ring.get_successor_for_key(key, exclude_node_id=primary.node_id)
        if self.manager.is_alive(primary.node_id):
            return primary.node_id
        if replica is not None:
            if self.manager.is_alive(replica.node_id):
                return replica.node_id
            return replica.node_id
        return primary.node_id

    def _get_entry_snapshot(
        self,
        snapshots: dict[str, dict[str, dict[str, Any | None]]],
        key: str,
        node_id: str | None,
    ) -> dict[str, Any | None] | None:
        if node_id is None:
            return None
        return snapshots.get(node_id, {}).get(key)

    def _get_first_entry_snapshot(
        self,
        snapshots: dict[str, dict[str, dict[str, Any | None]]],
        key: str,
    ) -> dict[str, Any | None] | None:
        for entries in snapshots.values():
            entry = entries.get(key)
            if entry is not None:
                return entry
        return None

    async def _rebalance_after_remove(self, previous_ring) -> None:
        current_ring = self.manager.copy_ring()
        snapshots: dict[str, dict[str, dict[str, Any | None]]] = {}

        for node in self.manager.get_all_nodes():
            snapshots[node.node_id] = await self.transport.snapshot(node)

        key_locations: dict[str, set[str]] = {}
        for node_id, entries in snapshots.items():
            for key in entries:
                key_locations.setdefault(key, set()).add(node_id)

        now = monotonic()
        nodes_by_id = {node.node_id: node for node in self.manager.get_all_nodes()}

        for key in sorted(key_locations):
            old_primary = previous_ring.get_node(key)
            if old_primary is None:
                continue

            old_replica = previous_ring.get_successor_for_key(key, exclude_node_id=old_primary.node_id)
            new_primary = current_ring.get_node(key)
            if new_primary is None:
                continue
            new_replica = current_ring.get_successor_for_key(key, exclude_node_id=new_primary.node_id)

            old_owner_ids = {node.node_id for node in (old_primary, old_replica) if node is not None}
            new_owner_ids = {node.node_id for node in (new_primary, new_replica) if node is not None}

            if old_owner_ids == new_owner_ids:
                continue

            source_entry = self._get_entry_snapshot(snapshots, key, new_primary.node_id)
            if source_entry is None and old_replica is not None:
                source_entry = self._get_entry_snapshot(snapshots, key, old_replica.node_id)
            if source_entry is None:
                source_entry = self._get_first_entry_snapshot(snapshots, key)
            if source_entry is None:
                continue

            expires_at = source_entry.get("expires_at")
            ttl_seconds: float | None = None
            if expires_at is not None:
                ttl_seconds = max(float(expires_at) - now, 0.0)
                if ttl_seconds <= 0:
                    continue

            for node_id in new_owner_ids:
                if node_id not in nodes_by_id:
                    continue
                if key in snapshots.get(node_id, {}):
                    continue
                await self.transport.put(nodes_by_id[node_id], key, source_entry["value"], ttl_seconds)

            for node_id, entries in snapshots.items():
                if node_id in new_owner_ids or key not in entries:
                    continue
                node = nodes_by_id.get(node_id)
                if node is not None:
                    await self.transport.delete(node, key)
