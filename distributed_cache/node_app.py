from __future__ import annotations

from fastapi import FastAPI, HTTPException

from distributed_cache.cluster.models import NodeConfig
from distributed_cache.cluster.runtime import NodeRuntime


def create_node_app(runtime: NodeRuntime) -> FastAPI:
    app = FastAPI(title=f"Cache Node {runtime.config.node_id}", version="0.1.0")

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "node_id": runtime.config.node_id, "size": runtime.store.size()}

    @app.put("/internal/cache/{key}")
    async def put_cache(key: str, body: dict[str, object]) -> dict[str, object]:
        result = runtime.put_local(key, body.get("value"), ttl_seconds=body.get("ttl_seconds"))
        if not result.ok:
            raise HTTPException(status_code=result.status_code, detail=result.message)
        return {"ok": True, "served_by": runtime.config.node_id}

    @app.get("/internal/cache/{key}")
    async def get_cache(key: str) -> dict[str, object]:
        found, value = runtime.get_local(key)
        if not found:
            raise HTTPException(status_code=404, detail="not found")
        return {"ok": True, "value": value, "served_by": runtime.config.node_id}

    @app.delete("/internal/cache/{key}")
    async def delete_cache(key: str) -> dict[str, object]:
        result = runtime.delete_local(key)
        if not result.ok:
            raise HTTPException(status_code=result.status_code, detail=result.message)
        return {"ok": True, "served_by": runtime.config.node_id}

    @app.post("/internal/heartbeat")
    async def heartbeat() -> dict[str, object]:
        runtime.heartbeat()
        return {"ok": True, "node_id": runtime.config.node_id}

    @app.get("/internal/snapshot")
    async def snapshot() -> dict[str, object]:
        return runtime.snapshot()

    return app


def build_runtime_from_env() -> NodeRuntime:
    import os

    node_id = os.getenv("NODE_ID", "node-a")
    host = os.getenv("NODE_HOST", "0.0.0.0")
    port = int(os.getenv("NODE_PORT", "8001"))
    max_items = int(os.getenv("NODE_MAX_ITEMS", "1024"))
    return NodeRuntime.create(NodeConfig(node_id=node_id, host=host, port=port), max_items=max_items)


if __name__ == "__main__":
    import os
    import uvicorn

    runtime = build_runtime_from_env()
    uvicorn.run(create_node_app(runtime), host=os.getenv("NODE_HOST", "0.0.0.0"), port=int(os.getenv("NODE_PORT", "8001")))
