from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Any

from distributed_cache.cache.store import CacheStore
from distributed_cache.cluster.models import NodeConfig, OperationResult


@dataclass(slots=True)
class NodeRuntime:
    config: NodeConfig
    store: CacheStore
    last_heartbeat_at: float

    @classmethod
    def create(cls, config: NodeConfig, max_items: int = 1024) -> "NodeRuntime":
        return cls(config=config, store=CacheStore(max_items=max_items), last_heartbeat_at=monotonic())

    def put_local(self, key: str, value: Any, ttl_seconds: float | None = None) -> OperationResult:
        self.store.put(key, value, ttl_seconds=ttl_seconds)
        self.last_heartbeat_at = monotonic()
        return OperationResult(ok=True, status_code=200, message="stored")

    def get_local(self, key: str) -> tuple[bool, Any | None]:
        value = self.store.get(key)
        self.last_heartbeat_at = monotonic()
        return value is not None, value

    def delete_local(self, key: str) -> OperationResult:
        deleted = self.store.delete(key)
        self.last_heartbeat_at = monotonic()
        if deleted:
            return OperationResult(ok=True, status_code=200, message="deleted")
        return OperationResult(ok=False, status_code=404, message="not found")

    def heartbeat(self) -> None:
        self.last_heartbeat_at = monotonic()

    def snapshot(self) -> dict[str, Any]:
        return {
            "node_id": self.config.node_id,
            "keys": self.store.items(),
            "entries": self.store.snapshot(),
            "size": self.store.size(),
            "last_heartbeat_at": self.last_heartbeat_at,
        }
