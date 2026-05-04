from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from distributed_cache.api.schemas import CacheResponse, HeartbeatResponse, PutRequest
from distributed_cache.cluster.service import DistributedCacheCluster
from distributed_cache.cluster.transport import HttpNodeTransport


def create_app(cluster: DistributedCacheCluster) -> FastAPI:
    heartbeat_task: asyncio.Task[None] | None = None

    async def heartbeat_loop() -> None:
        while True:
            for node in cluster.manager.get_all_nodes():
                try:
                    await cluster.heartbeat(node.node_id)
                except Exception:
                    continue
            await asyncio.sleep(1.0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal heartbeat_task
        if cluster.manager.get_all_nodes():
            heartbeat_task = asyncio.create_task(heartbeat_loop())
        try:
            yield
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(title="Distributed Cache", version="0.1.0", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "nodes": cluster.manager.health_report()}

    @app.put("/cache/{key}", response_model=CacheResponse)
    async def put_cache(key: str, request: PutRequest) -> CacheResponse:
        result = await cluster.put(key, request.value, ttl_seconds=request.ttl_seconds)
        if not result["ok"]:
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        return CacheResponse(**result)

    @app.get("/cache/{key}", response_model=CacheResponse)
    async def get_cache(key: str) -> CacheResponse:
        result = await cluster.get(key)
        if not result["ok"]:
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        return CacheResponse(**result)

    @app.delete("/cache/{key}", response_model=CacheResponse)
    async def delete_cache(key: str) -> CacheResponse:
        result = await cluster.delete(key)
        if not result["ok"]:
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        return CacheResponse(**result)

    @app.post("/internal/heartbeat/{node_id}", response_model=HeartbeatResponse)
    async def heartbeat(node_id: str) -> HeartbeatResponse:
        result = await cluster.heartbeat(node_id)
        if not result["ok"]:
            raise HTTPException(status_code=result["status_code"], detail=result["message"])
        return HeartbeatResponse(**result)

    return app


__all__ = ["create_app"]
