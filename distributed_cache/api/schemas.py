from __future__ import annotations

from pydantic import BaseModel, Field


class PutRequest(BaseModel):
    value: object
    ttl_seconds: float | None = Field(default=None, ge=0)


class CacheResponse(BaseModel):
    ok: bool
    status_code: int
    message: str | None = None
    value: object | None = None
    served_by: str | None = None


class HeartbeatResponse(BaseModel):
    ok: bool
    status_code: int
    message: str | None = None
