from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from distributed_cache.cluster.models import NodeConfig, OperationResult
from distributed_cache.cluster.runtime import NodeRuntime


class NodeTransport(Protocol):
    async def put(self, node: NodeConfig, key: str, value: Any, ttl_seconds: float | None = None) -> OperationResult: ...

    async def get(self, node: NodeConfig, key: str) -> tuple[bool, Any | None]: ...

    async def delete(self, node: NodeConfig, key: str) -> OperationResult: ...

    async def heartbeat(self, node: NodeConfig) -> None: ...

    async def snapshot(self, node: NodeConfig) -> dict[str, dict[str, Any | None]]: ...


class InProcessNodeTransport:
    def __init__(self, runtimes: dict[str, NodeRuntime]) -> None:
        self._runtimes = runtimes

    def register_node(self, node: NodeConfig, runtime: NodeRuntime | None = None) -> NodeRuntime:
        if runtime is None:
            runtime = NodeRuntime.create(node)
        self._runtimes[node.node_id] = runtime
        return runtime

    async def put(self, node: NodeConfig, key: str, value: Any, ttl_seconds: float | None = None) -> OperationResult:
        return self._runtimes[node.node_id].put_local(key, value, ttl_seconds)

    async def get(self, node: NodeConfig, key: str) -> tuple[bool, Any | None]:
        return self._runtimes[node.node_id].get_local(key)

    async def delete(self, node: NodeConfig, key: str) -> OperationResult:
        return self._runtimes[node.node_id].delete_local(key)

    async def heartbeat(self, node: NodeConfig) -> None:
        self._runtimes[node.node_id].heartbeat()

    async def snapshot(self, node: NodeConfig) -> dict[str, dict[str, Any | None]]:
        return self._runtimes[node.node_id].snapshot()["entries"]


class HttpNodeTransport:
    def __init__(self, timeout_seconds: float = 2.0) -> None:
        self._timeout_seconds = timeout_seconds

    async def put(self, node: NodeConfig, key: str, value: Any, ttl_seconds: float | None = None) -> OperationResult:
        async with httpx.AsyncClient(base_url=node.base_url, timeout=self._timeout_seconds) as client:
            response = await client.put(f"/internal/cache/{key}", json={"value": value, "ttl_seconds": ttl_seconds})
        return OperationResult(ok=response.is_success, status_code=response.status_code, message=response.text)

    async def get(self, node: NodeConfig, key: str) -> tuple[bool, Any | None]:
        async with httpx.AsyncClient(base_url=node.base_url, timeout=self._timeout_seconds) as client:
            response = await client.get(f"/internal/cache/{key}")
        if response.status_code == 200:
            return True, response.json()["value"]
        return False, None

    async def delete(self, node: NodeConfig, key: str) -> OperationResult:
        async with httpx.AsyncClient(base_url=node.base_url, timeout=self._timeout_seconds) as client:
            response = await client.delete(f"/internal/cache/{key}")
        return OperationResult(ok=response.is_success, status_code=response.status_code, message=response.text)

    async def heartbeat(self, node: NodeConfig) -> None:
        async with httpx.AsyncClient(base_url=node.base_url, timeout=self._timeout_seconds) as client:
            await client.post("/internal/heartbeat")

    async def snapshot(self, node: NodeConfig) -> dict[str, dict[str, Any | None]]:
        async with httpx.AsyncClient(base_url=node.base_url, timeout=self._timeout_seconds) as client:
            response = await client.get("/internal/snapshot")
        response.raise_for_status()
        payload = response.json()
        return payload.get("entries", {})
